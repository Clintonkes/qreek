"""
@file web_saved_cards.py
@description Account-level management of a Qreek user's saved cards — list,
remove, set default. Cards are only ever created via a successful, verified
Flutterwave charge (see finalize_flutterwave_link_payment's save_card_for_phone
path and the charge-saved-card flow in web_payment_links.py); this router never
accepts raw card data, only manages the resulting tokens.
"""
import json as _json
import uuid
import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from database.session import get_db
from database.models import SavedCard, User
from core.web_jwt import decode_token
from services.flutterwave_service import FlutterwaveAPIError, direct_card_charge, encrypt_flutterwave_payload, validate_charge

router = APIRouter(prefix="/api/v1/cards", tags=["cards"])


def _card_dict(c: SavedCard) -> dict:
    return {
        "id": c.id,
        "brand": c.card_brand,
        "last4": c.last4,
        "exp_month": c.exp_month,
        "exp_year": c.exp_year,
        "bank": c.bank,
        "is_default": c.is_default,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


@router.get("")
async def list_saved_cards(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(SavedCard).where(SavedCard.owner_phone == claims["phone"]).order_by(SavedCard.created_at.desc())
    )
    return {"cards": [_card_dict(c) for c in result.scalars().all()]}


@router.delete("/{card_id}")
async def delete_saved_card(card_id: str, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SavedCard).where(SavedCard.id == card_id, SavedCard.owner_phone == claims["phone"]))
    card = result.scalar_one_or_none()
    if not card:
        raise HTTPException(status_code=404, detail="Saved card not found.")
    was_default = card.is_default
    await db.delete(card)
    await db.flush()

    if was_default:
        remaining_result = await db.execute(
            select(SavedCard).where(SavedCard.owner_phone == claims["phone"]).order_by(SavedCard.created_at.desc())
        )
        remaining = remaining_result.scalar_one_or_none()
        if remaining:
            remaining.is_default = True

    await db.commit()
    return {"message": "Card removed."}


CARD_SAVE_FEE = 50  # NGN — charged to verify and tokenise the card


class AddCardIn(BaseModel):
    card_number:  str
    cvv:          str
    expiry_month: str
    expiry_year:  str
    pin:          Optional[str] = None


class ValidateAddCardIn(BaseModel):
    flw_ref: str
    otp:     str


async def _persist_card(db: AsyncSession, *, owner_phone: str, card_data: dict) -> Optional[SavedCard]:
    """Save Flutterwave card token + masked display fields. No-ops when no token."""
    token = card_data.get("token")
    if not token:
        return None
    existing = await db.execute(
        select(SavedCard).where(SavedCard.owner_phone == owner_phone, SavedCard.token == token)
    )
    if existing.scalar_one_or_none():
        return None
    has_cards = await db.execute(select(SavedCard).where(SavedCard.owner_phone == owner_phone))
    is_first = has_cards.scalar_one_or_none() is None
    expiry = card_data.get("expiry") or ""
    parts = expiry.split("/")
    card = SavedCard(
        owner_phone=owner_phone,
        token=token,
        card_brand=card_data.get("type"),
        last4=card_data.get("last_4digits"),
        exp_month=parts[0] if parts else None,
        exp_year=parts[-1] if len(parts) > 1 else None,
        bank=card_data.get("issuer"),
        is_default=is_first,
    )
    db.add(card)
    return card


@router.post("/add")
async def add_card(
    body: AddCardIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Saves a new card to the user's Qreek account.
    A ₦50 card-verification charge is made via Flutterwave Direct Charge.
    Qreek never stores raw card data — only the reusable token Flutterwave returns
    after a successful charge. The card number, CVV, and expiry travel encrypted
    (3DES ECB) directly to Flutterwave and are never written to Qreek's database.

    Returns one of:
      { status: 'success', card }        — card saved immediately
      { status: 'pin', tx_ref }          — re-submit with pin field set
      { status: 'otp', flw_ref, tx_ref } — enter OTP via /cards/add/validate
      { status: 'redirect', url }        — 3DS auth required (open in browser)
    """
    user_r = await db.execute(select(User).where(User.phone == claims["phone"]))
    user = user_r.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    tx_ref = "QRK_CVSV_" + uuid.uuid4().hex[:10].upper()
    frontend_url = os.getenv("FRONTEND_URL", "https://qreekfinance.org")

    payload = {
        "card_number":  body.card_number.replace(" ", ""),
        "cvv":          body.cvv,
        "expiry_month": body.expiry_month,
        "expiry_year":  body.expiry_year.replace("20", "", 1) if len(body.expiry_year) == 4 else body.expiry_year,
        "currency":     "NGN",
        "amount":       CARD_SAVE_FEE,
        "fullname":     user.name or claims["phone"],
        "email":        f"{claims['phone']}@qreekfinance.org",
        "phone_number": claims["phone"],
        "tx_ref":       tx_ref,
        "redirect_url": f"{frontend_url}/settings",
    }
    if body.pin:
        payload["authorization"] = {"mode": "pin", "pin": body.pin}

    try:
        encrypted = encrypt_flutterwave_payload(_json.dumps(payload))
        result = await direct_card_charge(encrypted_payload=encrypted)
    except FlutterwaveAPIError as e:
        raise HTTPException(status_code=502, detail=f"Card verification failed: {e}")

    data      = result.get("data", {})
    auth_mode = (result.get("meta", {}).get("authorization", {}).get("mode") or "").lower()
    flw_ref   = data.get("flw_ref", "")
    status_val = str(result.get("status", "")).lower()

    if auth_mode == "pin":
        return {"status": "pin", "tx_ref": tx_ref, "message": "Enter your card PIN to continue."}

    if auth_mode == "otp":
        return {"status": "otp", "flw_ref": flw_ref, "tx_ref": tx_ref, "message": result.get("message", "Enter the OTP sent to your phone.")}

    redirect_url = result.get("meta", {}).get("authorization", {}).get("redirect") or data.get("redirect_url", "")
    if auth_mode in ("redirect", "3dsecure", "vbvsecurecode") or redirect_url:
        return {"status": "redirect", "url": redirect_url, "tx_ref": tx_ref}

    if status_val == "success" or str(data.get("status", "")).lower() == "successful":
        card = await _persist_card(db, owner_phone=claims["phone"], card_data=data.get("card", {}))
        await db.commit()
        if card:
            return {"status": "success", "card": _card_dict(card)}
        return {"status": "success", "card": None, "message": "Charge succeeded but card was not tokenised by your bank. Try a different card."}

    raise HTTPException(status_code=502, detail=result.get("message") or "Unexpected response from card verification.")


@router.post("/add/validate")
async def validate_add_card_otp(
    body: ValidateAddCardIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """Submits the OTP from an add-card step-up and saves the card on success."""
    try:
        result = await validate_charge(otp=body.otp, flw_ref=body.flw_ref)
    except FlutterwaveAPIError as e:
        raise HTTPException(status_code=502, detail=f"OTP validation failed: {e}")

    data = result.get("data", {})
    if str(data.get("status", "")).lower() not in ("successful", "completed"):
        raise HTTPException(status_code=400, detail="OTP was not accepted. Please check the code and try again.")

    card = await _persist_card(db, owner_phone=claims["phone"], card_data=data.get("card", {}))
    await db.commit()

    if card:
        return {"status": "success", "card": _card_dict(card)}
    return {"status": "success", "card": None, "message": "Card verified but not tokenised by your bank."}


@router.put("/{card_id}/default")
async def set_default_card(card_id: str, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SavedCard).where(SavedCard.owner_phone == claims["phone"]))
    cards = result.scalars().all()
    found = False
    for c in cards:
        if c.id == card_id:
            c.is_default = True
            found = True
        else:
            c.is_default = False
    if not found:
        raise HTTPException(status_code=404, detail="Saved card not found.")
    await db.commit()
    return {"message": "Default card updated."}
