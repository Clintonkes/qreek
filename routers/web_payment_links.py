"""
@file web_payment_links.py
@description Payment Links API — facilitates shareable URLs for receiving NGN payments.
Anyone (Qreek user or not) can pay via a link. Funds go straight to the creator's bank account.

Flow:
1. Creation: Authenticated users define link parameters (title, amount, bank destination).
2. Resolution: Public users (payers) fetch link details via a unique code.
3. Execution: Payers submit payment info, then complete Flutterwave hosted checkout
   with card, bank transfer, or any method enabled on the Qreek merchant account.
4. Security: Enforces link activity status, expiration dates, and usage limits (max_uses).
"""
import uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from pydantic import BaseModel
from typing import Optional

from database.session import get_db
from database.models import PaymentLink, Transaction
from core.web_jwt import decode_token
from core.banks import resolve_bank
from services.flutterwave_service import create_transfer, initialize_checkout, query_transaction_fee, verify_transaction

router = APIRouter(prefix="/api/v1/payment-links", tags=["payment-links"])

FEE_PCT = 0.0025  # 0.25% for direct payment links


async def _checkout_total_for_recipient(recipient_amount: float) -> tuple[float, float, float]:
    """
    Builds a one-time payer total where the link owner receives the requested
    amount and Qreek's fee is added on top of that amount.
    """
    qreek_fee = round(recipient_amount * FEE_PCT, 2)
    checkout_amount = round(recipient_amount + qreek_fee, 2)
    provider_fee = 0.0
    for _ in range(2):
        provider_fee = await query_transaction_fee(checkout_amount)
        checkout_amount = round(recipient_amount + qreek_fee + provider_fee, 2)
    return checkout_amount, qreek_fee, provider_fee


class CreateLinkIn(BaseModel):
    title:        str
    description:  str
    amount:       Optional[float] = None   # None = flexible
    bank_account: str
    bank_code:    str
    max_uses:     Optional[int] = None
    expires_days: Optional[int] = None
    provider:     Optional[str] = "flutterwave"


class PayLinkIn(BaseModel):
    amount:       float
    name:         Optional[str] = None
    payer_name:   Optional[str] = None
    phone:        Optional[str] = None
    payer_phone:  Optional[str] = None
    payment_description: Optional[str] = None
    note:         Optional[str] = None
    provider:     Optional[str] = "flutterwave"
    redirect_url: Optional[str] = None
    idempotency_key: Optional[str] = None


class ConfirmFlutterwaveIn(BaseModel):
    transaction_id: Optional[str] = None
    tx_ref:         Optional[str] = None
    status:         Optional[str] = None


def _payment_dict(tx: Transaction) -> dict:
    """
    Returns the public payment status shape used by checkout redirects and
    polling. A transaction is only complete after recipient settlement succeeds.
    """
    return {
        "reference": tx.reference,
        "amount": tx.gross_amount or tx.amount,
        "fee": tx.qreek_fee or tx.fee,
        "provider_fee": tx.provider_fee,
        "provider_settled_amount": tx.provider_settled_amount,
        "net": tx.net_amount or tx.ngn_amount,
        "recipient_amount": tx.net_amount or tx.ngn_amount,
        "checkout_amount": tx.gross_amount or tx.amount,
        "status": tx.status,
        "provider": tx.provider,
        "provider_transaction_id": tx.provider_transaction_id,
        "payout_status": tx.payout_status,
        "payout_reference": tx.payout_reference,
    }


def _link_dict(l: PaymentLink, show_bank: bool = False) -> dict:
    """
    Helper function to convert a PaymentLink model instance to a dictionary.
    Optionally includes bank details (masked) for the link creator.
    """
    d = {
        "id": l.id, "code": l.code, "title": l.title, "description": l.description,
        "amount": l.amount, "is_flexible": l.is_flexible,
        "bank_name": l.bank_name,
        "max_uses": l.max_uses, "use_count": l.use_count,
        "total_collected": l.total_collected,
        "expires_at": l.expires_at.isoformat() if l.expires_at else None,
        "is_active": l.is_active,
        "created_at": l.created_at.isoformat() if l.created_at else None,
        "url": f"https://qreekfinance.org/p/{l.code}",
    }
    if show_bank:
        d["bank_account"] = "****" + l.bank_account[-4:] if l.bank_account else None
        d["bank_code"]    = l.bank_code
    return d


def _provider_fee(data: dict) -> float:
    """
    Extracts Flutterwave's processing fee from a verification payload.
    Flutterwave payloads can differ by payment method, so we accept the common
    fee keys and fall back to charged_amount - amount when present.
    """
    for key in ("app_fee", "merchant_fee", "processor_fee"):
        value = data.get(key)
        if value is not None:
            return round(float(value or 0), 2)
    charged = data.get("charged_amount")
    amount = data.get("amount")
    if charged is not None and amount is not None:
        return round(max(float(charged or 0) - float(amount or 0), 0), 2)
    return 0.0


def _provider_settled_amount(data: dict, amount: float, provider_fee: float) -> float:
    """
    Calculates what Flutterwave actually settled into Qreek's merchant balance.
    That settled amount is the maximum pot available for recipient payout plus
    Qreek's own fee.
    """
    for key in ("amount_settled", "settled_amount", "merchant_amount"):
        value = data.get(key)
        if value is not None:
            return round(float(value or 0), 2)
    return round(max(float(amount or 0) - float(provider_fee or 0), 0), 2)


async def _get_live_link(db: AsyncSession, code: str) -> PaymentLink:
    result = await db.execute(select(PaymentLink).where(PaymentLink.code == code.upper()))
    link = result.scalar_one_or_none()
    if not link or not link.is_active:
        raise HTTPException(status_code=404, detail="Payment link not found.")
    if link.expires_at and link.expires_at < datetime.utcnow():
        raise HTTPException(status_code=410, detail="This payment link has expired.")
    if link.max_uses and link.use_count >= link.max_uses:
        raise HTTPException(status_code=410, detail="Maximum uses reached.")
    return link


async def finalize_flutterwave_link_payment(db: AsyncSession, tx_ref: str, transaction_id: str | int = None) -> dict:
    """
    Verifies a Flutterwave payment, settles the creator's net amount, and records the fee.
    Qreek's fee remains in the Qreek Flutterwave merchant balance; only net is transferred out.
    """
    tx_result = await db.execute(select(Transaction).where(Transaction.reference == tx_ref).with_for_update())
    tx = tx_result.scalar_one_or_none()
    if not tx:
        raise HTTPException(status_code=404, detail="Payment reference not found.")

    link_result = await db.execute(select(PaymentLink).where(PaymentLink.id == tx.pool_id).with_for_update())
    link = link_result.scalar_one_or_none()
    if not link:
        raise HTTPException(status_code=404, detail="Payment link not found for reference.")

    if tx.status == "completed" and tx.payout_status == "completed":
        return {"payment": _payment_dict(tx)}

    if not transaction_id:
        raise HTTPException(status_code=400, detail="Flutterwave transaction_id is required for verification.")

    verified = await verify_transaction(transaction_id)
    data = verified.get("data", {})
    flw_status = str(data.get("status", "")).lower()
    flw_ref = data.get("tx_ref")
    flw_currency = data.get("currency")
    flw_amount = float(data.get("amount") or 0)

    if flw_ref != tx.reference:
        raise HTTPException(status_code=400, detail="Flutterwave reference does not match this payment.")
    if flw_currency != "NGN":
        raise HTTPException(status_code=400, detail="Unsupported payment currency.")
    if round(flw_amount, 2) != round(tx.gross_amount or tx.amount, 2):
        raise HTTPException(status_code=400, detail="Flutterwave amount does not match this payment.")
    if flw_status != "successful":
        raise HTTPException(status_code=400, detail="Flutterwave payment is not successful.")

    tx.provider_transaction_id = str(data.get("id") or transaction_id)
    tx.provider_fee = _provider_fee(data)
    tx.provider_settled_amount = _provider_settled_amount(data, tx.gross_amount or tx.amount, tx.provider_fee)
    recipient_amount = tx.net_amount or tx.ngn_amount
    if (tx.provider_settled_amount or 0) < recipient_amount:
        tx.status = "payout_pending"
        tx.payout_status = "pending"
        tx.payout_reference = f"{tx.reference}_NET"
        tx.payout_error = (
            f"Flutterwave settled {tx.provider_settled_amount}, below recipient amount {recipient_amount}. "
            "Increase provider-fee allowance or use Flutterwave split settlement."
        )
        await db.commit()
        return {"payment": _payment_dict(tx)}

    was_unsettled = tx.payout_status != "completed"
    tx.status = "processing"
    tx.provider = "flutterwave"
    tx.payout_status = "pending"

    # Transfer the customer's net amount after verifying the payment.
    # The transaction only becomes completed after this recipient payout works.
    try:
        transfer = await create_transfer(
            amount=recipient_amount,
            bank_code=link.bank_code,
            account_number=link.bank_account,
            reference=f"{tx.reference}_NET",
            narration=f"QreekPay: {link.title}"[:100],
        )
        tx.payout_status = "completed"
        tx.payout_reference = transfer.get("data", {}).get("reference") or f"{tx.reference}_NET"
        tx.payout_error = None
        tx.status = "completed"
    except Exception as exc:
        tx.status = "payout_pending"
        tx.payout_status = "pending"
        tx.payout_reference = f"{tx.reference}_NET"
        tx.payout_error = str(exc)[:1000]

    if was_unsettled and tx.payout_status == "completed":
        link.use_count = (link.use_count or 0) + 1
        link.total_collected = (link.total_collected or 0) + (tx.net_amount or tx.ngn_amount or 0)
    await db.commit()
    return {"payment": _payment_dict(tx)}


@router.post("")
async def create_link(
    body: CreateLinkIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Creates a new shareable payment link for receiving NGN.
    Validates the bank details and sets an optional expiration date.
    """
    phone = claims["phone"]

    if not body.description.strip():
        raise HTTPException(status_code=400, detail="Description is required.")

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
    """
    Lists all payment links created by the authenticated user.
    """
    phone  = claims["phone"]
    result = await db.execute(
        select(PaymentLink).where(PaymentLink.created_by == phone).order_by(desc(PaymentLink.created_at)).limit(50)
    )
    links = result.scalars().all()
    return {"links": [_link_dict(l, show_bank=True) for l in links]}


@router.get("/resolve/{code}")
async def resolve_link(code: str, db: AsyncSession = Depends(get_db)):
    """
    Public endpoint to view a payment link by its unique code.
    Validates that the link exists, is active, has not expired, and has not reached max uses.
    """
    link = await _get_live_link(db, code)
    return {"link": _link_dict(link)}


@router.post("/pay/{code}")
async def pay_link(
    code: str,
    body: PayLinkIn,
    db: AsyncSession = Depends(get_db),
):
    """
    Starts a public Flutterwave checkout for a specific Qreek payment link.
    The payer can complete with card, bank transfer, or any method enabled on Flutterwave.
    """
    link = await _get_live_link(db, code)

    recipient_amount = link.amount if not link.is_flexible else body.amount
    if not recipient_amount or recipient_amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid amount.")
    payment_description = (body.payment_description or body.note or "").strip()
    if not payment_description:
        raise HTTPException(status_code=400, detail="Payment description is required.")

    checkout_amount, fee, provider_fee_estimate = await _checkout_total_for_recipient(recipient_amount)
    idempotency_key = body.idempotency_key or f"{code.upper()}:{body.phone or body.payer_phone or 'anon'}:{recipient_amount}:{payment_description}"

    existing_result = await db.execute(select(Transaction).where(Transaction.idempotency_key == idempotency_key).with_for_update())
    existing = existing_result.scalar_one_or_none()
    if existing:
        if existing.status in ("completed", "processing", "payout_pending"):
            return {
                "message": "Payment already recorded.",
                "tx_ref": existing.tx_ref or existing.reference,
                "reference": existing.reference,
                "fee": existing.qreek_fee or existing.fee,
                "net": existing.net_amount or existing.ngn_amount,
                "recipient_amount": existing.net_amount or existing.ngn_amount,
                "checkout_amount": existing.gross_amount or existing.amount,
                "status": existing.status,
                "payout_status": existing.payout_status,
            }
        if existing.provider_checkout_url:
            return {
                "message": f"Checkout already created for ₦{existing.gross_amount or existing.amount:,.2f}.",
                "checkout_url": existing.provider_checkout_url,
                "payment_url": existing.provider_checkout_url,
                "tx_ref": existing.tx_ref or existing.reference,
                "reference": existing.reference,
                "fee": existing.qreek_fee or existing.fee,
                "net": existing.net_amount or existing.ngn_amount,
                "recipient_amount": existing.net_amount or existing.ngn_amount,
                "checkout_amount": existing.gross_amount or existing.amount,
            }
        ref = existing.reference
    else:
        ref = "QRK_LNK_" + uuid.uuid4().hex[:10].upper()

    payer_name = (body.name or body.payer_name or "Qreek payer").strip()
    payer_phone = body.phone or body.payer_phone
    if not existing:
        tx = Transaction(
            user_phone=link.created_by,
            tx_type="payment_link",
            currency="NGN",
            amount=checkout_amount,
            ngn_amount=recipient_amount,
            gross_amount=checkout_amount,
            qreek_fee=fee,
            provider_fee=provider_fee_estimate,
            provider_settled_amount=round(checkout_amount - provider_fee_estimate, 2),
            net_amount=recipient_amount,
            fee=fee,
            fee_pct=FEE_PCT,
            status="pending",
            provider="flutterwave",
            reference=ref,
            tx_ref=ref,
            idempotency_key=idempotency_key,
            payment_description=payment_description,
            pool_id=link.id,
            bank_account=link.bank_account,
            bank_code=link.bank_code,
            bank_name=link.bank_name,
        )
        db.add(tx)
        await db.commit()
    else:
        tx = existing

    checkout = await initialize_checkout(
        tx_ref=ref,
        amount=checkout_amount,
        customer_name=payer_name,
        customer_phone=payer_phone,
        redirect_url=body.redirect_url,
        title=link.title,
        description=payment_description,
        metadata={
            "code": link.code,
            "link_id": link.id,
            "creator_phone": link.created_by,
            "payment_description": payment_description,
            "qreek_fee": fee,
            "recipient_amount": recipient_amount,
            "provider_fee_estimate": provider_fee_estimate,
            "checkout_amount": checkout_amount,
        },
    )
    checkout_url = checkout.get("data", {}).get("link")
    tx.provider_checkout_url = checkout_url
    await db.commit()

    return {
        "message": f"Checkout created for ₦{checkout_amount:,.2f}. Recipient receives ₦{recipient_amount:,.2f}.",
        "checkout_url": checkout_url,
        "payment_url": checkout_url,
        "tx_ref": ref,
        "reference": ref,
        "fee": fee,
        "provider_fee_estimate": provider_fee_estimate,
        "net": recipient_amount,
        "recipient_amount": recipient_amount,
        "checkout_amount": checkout_amount,
    }


@router.post("/pay/{code}/flutterwave/confirm")
async def confirm_flutterwave_link_payment(
    code: str,
    body: ConfirmFlutterwaveIn,
    db: AsyncSession = Depends(get_db),
):
    """
    Confirms the Flutterwave redirect after hosted checkout.
    The backend verifies status, tx_ref, amount, and currency before recording success.
    """
    if body.status and body.status.lower() not in ("successful", "completed"):
        raise HTTPException(status_code=400, detail="Flutterwave did not mark this payment successful.")
    if not body.tx_ref:
        raise HTTPException(status_code=400, detail="Missing Flutterwave tx_ref.")
    result = await finalize_flutterwave_link_payment(db, body.tx_ref, body.transaction_id)
    return result


@router.get("/pay/{code}/status/{tx_ref}")
async def get_link_payment_status(
    code: str,
    tx_ref: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Public checkout status endpoint. It lets the redirect page keep showing
    progress after Flutterwave has collected funds while Qreek awaits recipient
    bank settlement.
    """
    link_result = await db.execute(select(PaymentLink).where(PaymentLink.code == code.upper()))
    link = link_result.scalar_one_or_none()
    if not link:
        raise HTTPException(status_code=404, detail="Payment link not found.")
    result = await db.execute(
        select(Transaction).where(
            Transaction.reference == tx_ref,
            Transaction.pool_id == link.id,
        )
    )
    tx = result.scalar_one_or_none()
    if not tx:
        raise HTTPException(status_code=404, detail="Payment reference not found.")
    return {"payment": _payment_dict(tx)}


@router.delete("/{link_id}")
async def deactivate_link(
    link_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Deactivates a payment link, making it unavailable for future payments.
    """
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
