"""
Payment Links API — shareable URLs for receiving NGN payments.
Anyone (Qreek user or not) can pay via a link. Funds go straight to creator's bank.
"""
import uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from pydantic import BaseModel
from typing import Optional

from database.session import get_db
from database.models import PaymentLink, User, PoolTransaction
from core.web_jwt import decode_token
from core.banks import resolve_bank
from core.payout import best_payout, settle_fee
from services.payment_service import debit_ngn_or_reject, refund_ngn
from services.security_service import is_frozen, pin_attempts_remaining, verify_transaction_pin
import asyncio

router = APIRouter(prefix="/api/v1/payment-links", tags=["payment-links"])

FEE_PCT = 0.004


class CreateLinkIn(BaseModel):
    title:        str
    description:  Optional[str] = None
    amount:       Optional[float] = None   # None = flexible
    bank_account: str
    bank_code:    str
    max_uses:     Optional[int] = None
    expires_days: Optional[int] = None


class PayLinkIn(BaseModel):
    amount:         float
    payer_name:     str
    payer_phone:    Optional[str] = None
    pin:            str            # payer's Qreek PIN (must be registered)


def _link_dict(l: PaymentLink, show_bank: bool = False) -> dict:
    d = {
        "id": l.id, "code": l.code, "title": l.title, "description": l.description,
        "amount": l.amount, "is_flexible": l.is_flexible,
        "bank_name": l.bank_name,
        "max_uses": l.max_uses, "use_count": l.use_count,
        "total_collected": l.total_collected,
        "expires_at": l.expires_at.isoformat() if l.expires_at else None,
        "is_active": l.is_active,
        "created_at": l.created_at.isoformat() if l.created_at else None,
        "url": f"https://qreekfinance.org/pay/{l.code}",
    }
    if show_bank:
        d["bank_account"] = "****" + l.bank_account[-4:] if l.bank_account else None
        d["bank_code"]    = l.bank_code
    return d


@router.post("")
async def create_link(
    body: CreateLinkIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]

    bank = resolve_bank(body.bank_code)
    if not bank:
        raise HTTPException(status_code=400, detail=f"Invalid bank code: {body.bank_code}")

    expires_at = None
    if body.expires_days:
        from datetime import timedelta
        expires_at = datetime.utcnow() + timedelta(days=body.expires_days)

    link = PaymentLink(
        created_by=phone,
        title=body.title,
        description=body.description,
        amount=body.amount,
        is_flexible=body.amount is None,
        bank_account=body.bank_account,
        bank_code=body.bank_code,
        bank_name=bank["name"],
        max_uses=body.max_uses,
        expires_at=expires_at,
    )
    db.add(link)
    await db.commit()
    await db.refresh(link)
    return {"link": _link_dict(link, show_bank=True)}


@router.get("")
async def list_links(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone  = claims["phone"]
    result = await db.execute(
        select(PaymentLink).where(PaymentLink.created_by == phone).order_by(desc(PaymentLink.created_at)).limit(50)
    )
    links = result.scalars().all()
    return {"links": [_link_dict(l, show_bank=True) for l in links]}


@router.get("/resolve/{code}")
async def resolve_link(code: str, db: AsyncSession = Depends(get_db)):
    """Public endpoint — no auth needed. Anyone can view a payment link."""
    result = await db.execute(select(PaymentLink).where(PaymentLink.code == code.upper()))
    link   = result.scalar_one_or_none()
    if not link or not link.is_active:
        raise HTTPException(status_code=404, detail="Payment link not found or no longer active.")
    if link.expires_at and link.expires_at < datetime.utcnow():
        raise HTTPException(status_code=410, detail="This payment link has expired.")
    if link.max_uses and link.use_count >= link.max_uses:
        raise HTTPException(status_code=410, detail="This payment link has reached its maximum uses.")
    return {"link": _link_dict(link)}


@router.post("/pay/{code}")
async def pay_link(
    code: str,
    body: PayLinkIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """Pay a payment link. Payer must be a Qreek user (needs PIN)."""
    payer_phone = claims["phone"]

    result = await db.execute(select(PaymentLink).where(PaymentLink.code == code.upper()))
    link   = result.scalar_one_or_none()
    if not link or not link.is_active:
        raise HTTPException(status_code=404, detail="Payment link not found.")
    if link.expires_at and link.expires_at < datetime.utcnow():
        raise HTTPException(status_code=410, detail="This payment link has expired.")
    if link.max_uses and link.use_count >= link.max_uses:
        raise HTTPException(status_code=410, detail="Maximum uses reached.")
    if link.created_by == payer_phone:
        raise HTTPException(status_code=400, detail="You cannot pay your own payment link.")

    amount = link.amount if not link.is_flexible else body.amount
    if not amount or amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid amount.")

    if await is_frozen(db, payer_phone):
        raise HTTPException(status_code=403, detail="Account frozen after too many failed PIN attempts. Contact support.")

    ok = await verify_transaction_pin(db, payer_phone, body.pin)
    if not ok:
        remaining = await pin_attempts_remaining(db, payer_phone)
        if remaining <= 0:
            raise HTTPException(status_code=403, detail="Account frozen after 5 failed PIN attempts.")
        raise HTTPException(status_code=401, detail=f"Incorrect PIN. {remaining} attempts remaining.")

    fee = round(amount * FEE_PCT, 2)
    net = round(amount - fee, 2)
    ref = "QRK_LNK_" + uuid.uuid4().hex[:10].upper()

    await debit_ngn_or_reject(db, payer_phone, amount)
    bank = {"account_number": link.bank_account, "bank_code": link.bank_code}
    asyncio.create_task(_fire_link_payout(payer_phone, amount, net, fee, bank, ref))

    link.use_count      = (link.use_count or 0) + 1
    link.total_collected = (link.total_collected or 0) + amount
    await db.commit()

    return {
        "message": f"Payment of ₦{amount:,.2f} to {link.title} is processing.",
        "reference": ref,
        "fee": fee,
        "net": net,
    }


async def _fire_link_payout(payer_phone: str, gross: float, net: float, fee: float, bank: dict, ref: str):
    from database.session import AsyncSessionLocal
    try:
        await best_payout(payer_phone, net, bank, ref)
        await settle_fee(payer_phone, fee, ref)
    except Exception:
        async with AsyncSessionLocal() as db:
            await refund_ngn(db, payer_phone, gross)
            await db.commit()


@router.delete("/{link_id}")
async def deactivate_link(
    link_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone  = claims["phone"]
    result = await db.execute(
        select(PaymentLink).where(PaymentLink.id == link_id, PaymentLink.created_by == phone)
    )
    link = result.scalar_one_or_none()
    if not link:
        raise HTTPException(status_code=404, detail="Link not found.")
    link.is_active = False
    await db.commit()
    return {"message": "Payment link deactivated."}
