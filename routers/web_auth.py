from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from database.session import get_db
from database.models import User
from services.user_service import get_or_create_user, save_bank, apply_referral
from services.security_service import set_pin, verify_pin, freeze_account, is_frozen
from core.web_jwt import decode_token, issue_session_tokens, refresh_session_tokens, revoke_all_sessions, revoke_session
from core.banks import BANKS, resolve_bank
from core.session import set_state, State
import redis.asyncio as aioredis
import os, re

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
_redis = None


async def _r():
    global _redis
    if not _redis:
        _redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


def normalise_phone(phone: str) -> str:
    phone = re.sub(r"\s+", "", phone)
    if phone.startswith("0") and len(phone) == 11:
        phone = "+234" + phone[1:]
    elif phone.startswith("234") and not phone.startswith("+"):
        phone = "+" + phone
    elif not phone.startswith("+"):
        phone = "+" + phone
    return phone


def user_to_dict(user: User) -> dict:
    return {
        "phone":          user.phone,
        "name":           user.name,
        "kyc_verified":   user.kyc_verified,
        "is_merchant":    user.is_merchant,
        "onboarding_done":user.onboarding_done,
        "referral_code":  user.referral_code,
        "created_at":     user.created_at.isoformat() if user.created_at else None,
        "balance_ngn":    user.balance_ngn,
        "balance_usdt":   user.balance_usdt,
        "balance_usdc":   user.balance_usdc,
        "balance_btc":    user.balance_btc,
        "balance_eth":    user.balance_eth,
        "balance_bnb":    user.balance_bnb,
        "balance_sol":    user.balance_sol,
        "bank_account":   user.bank_account,
        "bank_code":      user.bank_code,
        "bank_name":      user.bank_name,
    }


class RegisterBody(BaseModel):
    phone:         str
    firstName:     str
    lastName:      str
    pin:           str
    referral_code: str | None = None


class LoginBody(BaseModel):
    phone: str
    pin:   str


class ChangePinBody(BaseModel):
    current_pin: str
    new_pin:     str


class SaveBankBody(BaseModel):
    account_number: str
    bank_code:      str


class RefreshBody(BaseModel):
    refresh_token: str


@router.post("/register")
async def register(body: RegisterBody, request: Request, db: AsyncSession = Depends(get_db)):
    phone = normalise_phone(body.phone)

    result   = await db.execute(select(User).where(User.phone == phone))
    existing = result.scalar_one_or_none()
    if existing and existing.onboarding_done:
        raise HTTPException(status_code=400, detail="Phone already registered")

    if not re.match(r"^\d{4,6}$", body.pin):
        raise HTTPException(status_code=400, detail="PIN must be 4–6 digits")

    user      = await get_or_create_user(db, phone)
    user.name = f"{body.firstName.strip()} {body.lastName.strip()}"
    user.kyc_verified    = True
    user.onboarding_done = True
    await db.commit()
    await db.refresh(user)
    await set_pin(db, phone, body.pin)

    if body.referral_code:
        await apply_referral(db, phone, body.referral_code)

    await set_state(phone, State.VERIFIED)

    tokens = await issue_session_tokens(db, phone, request)
    await db.commit()
    return {**tokens, "user": user_to_dict(user)}


@router.post("/login")
async def login(body: LoginBody, request: Request, db: AsyncSession = Depends(get_db)):
    phone  = normalise_phone(body.phone)
    result = await db.execute(select(User).where(User.phone == phone))
    user   = result.scalar_one_or_none()

    if not user or not user.onboarding_done:
        raise HTTPException(status_code=401, detail="Account not found. Please register first.")

    if await is_frozen(db, phone):
        raise HTTPException(status_code=403, detail="Account frozen after too many failed PIN attempts. Contact support.")

    r        = await _r()
    fail_key = f"web_pin_fail:{phone}"
    ok       = await verify_pin(db, phone, body.pin)

    if not ok:
        fails = await r.incr(fail_key)
        await r.expire(fail_key, 3600)
        if int(fails) >= 5:
            await freeze_account(db, phone)
            raise HTTPException(status_code=403, detail="Account frozen after 5 failed attempts.")
        raise HTTPException(status_code=401, detail=f"Incorrect PIN. {5 - int(fails)} attempts remaining.")

    await r.delete(fail_key)
    tokens = await issue_session_tokens(db, phone, request)
    await db.commit()
    return {**tokens, "user": user_to_dict(user)}


@router.post("/refresh")
async def refresh(body: RefreshBody, request: Request, db: AsyncSession = Depends(get_db)):
    return await refresh_session_tokens(db, body.refresh_token, request)


@router.post("/logout")
async def logout(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    await revoke_session(db, claims["session_id"], claims["phone"])
    return {"message": "Logged out successfully"}


@router.post("/logout-all")
async def logout_all(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    await revoke_all_sessions(db, claims["phone"])
    return {"message": "All sessions revoked successfully"}


@router.get("/me")
async def me(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone  = claims["phone"]
    result = await db.execute(select(User).where(User.phone == phone))
    user   = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user_to_dict(user)


@router.post("/change-pin")
async def change_pin(
    body: ChangePinBody,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    if not await verify_pin(db, phone, body.current_pin):
        raise HTTPException(status_code=401, detail="Current PIN is incorrect")
    if not re.match(r"^\d{4,6}$", body.new_pin):
        raise HTTPException(status_code=400, detail="New PIN must be 4–6 digits")
    await set_pin(db, phone, body.new_pin)
    return {"message": "PIN changed successfully"}


@router.post("/save-bank")
async def save_bank_route(
    body: SaveBankBody,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone     = claims["phone"]
    bank      = resolve_bank(body.bank_code)
    bank_name = bank["name"] if bank else body.bank_code
    await save_bank(db, phone, body.account_number, body.bank_code, bank_name)
    return {"message": "Bank account saved", "bank_name": bank_name}


@router.get("/banks")
async def list_banks():
    return {"banks": [{"code": b["code"], "name": b["name"]} for b in BANKS]}
