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
import logging
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, or_
from pydantic import BaseModel
from typing import Optional

from database.session import get_db
from database.models import PaymentEvent, PaymentLink, Transaction, UserSecurity
from core.web_jwt import decode_token
from core.banks import resolve_bank
from services.payment_event_logger import log_payment_event
from services.flutterwave_service import FlutterwaveAPIError, create_collection_subaccount, create_transfer, initialize_checkout, query_transaction_fee, verify_transaction

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
        d["flutterwave_subaccount_id"] = l.flutterwave_subaccount_id
        d["flutterwave_subaccount_status"] = l.flutterwave_subaccount_status
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


async def _ensure_link_subaccount(db: AsyncSession, link: PaymentLink) -> None:
    """
    Ensures a payment link has a Flutterwave collection subaccount so checkout
    can settle the recipient directly through split payments.

    Uses SELECT FOR UPDATE to serialise concurrent worker requests so only one
    worker attempts subaccount creation; the rest pick up the created ID.
    """
    if link.flutterwave_subaccount_id:
        return
    # Re-fetch under a row-level lock so concurrent workers wait here and
    # the winner's result is visible to all subsequent readers.
    locked_result = await db.execute(
        select(PaymentLink).where(PaymentLink.id == link.id).with_for_update()
    )
    locked_link = locked_result.scalar_one_or_none()
    if locked_link and locked_link.flutterwave_subaccount_id:
        # Another worker already created it while we were waiting for the lock
        link.flutterwave_subaccount_id = locked_link.flutterwave_subaccount_id
        link.flutterwave_subaccount_status = locked_link.flutterwave_subaccount_status
        return
    if locked_link and locked_link.flutterwave_subaccount_status == "failed":
        # A previous attempt already failed (bad bank details or persistent API error).
        # Do not retry on every pay request — let checkout proceed without split
        # settlement; the manual transfer payout path will handle settlement instead.
        logger.warning(
            "Skipping subaccount re-creation for link %s (status=failed, last_error=%s)",
            link.code,
            locked_link.flutterwave_subaccount_error,
        )
        return
    try:
        await log_payment_event(
            db,
            event_type="flutterwave.subaccount.ensure.started",
            reference=link.code,
            status="started",
            payload={"link_id": link.id, "bank_code": link.bank_code, "account_number_last4": (link.bank_account or "")[-4:]},
        )
        security_result = await db.execute(
            select(UserSecurity).where(UserSecurity.phone == link.created_by)
        )
        security = security_result.scalar_one_or_none()
        subaccount = await create_collection_subaccount(
            account_bank=link.bank_code,
            account_number=link.bank_account,
            business_name=link.title,
            business_mobile=link.created_by,
            business_email=security.recovery_email if security else None,
            split_type="percentage",
            split_value=0.9975,
        )
        data = subaccount.get("data", {})
        sub_id = data.get("subaccount_id") or data.get("id")
        link.flutterwave_subaccount_id = str(sub_id) if sub_id else None
        link.flutterwave_subaccount_status = "active" if link.flutterwave_subaccount_id else "missing_id"
        link.flutterwave_subaccount_error = None if link.flutterwave_subaccount_id else str(subaccount)[:1000]
        await log_payment_event(
            db,
            event_type="flutterwave.subaccount.ensure.completed",
            reference=link.code,
            status=link.flutterwave_subaccount_status,
            payload={"link_id": link.id, "subaccount_id": link.flutterwave_subaccount_id, "flutterwave": data},
        )
        await db.commit()
    except Exception as exc:
        link.flutterwave_subaccount_status = "failed"
        link.flutterwave_subaccount_error = str(exc)[:1000]
        error_payload = {"link_id": link.id}
        if isinstance(exc, FlutterwaveAPIError):
            error_payload.update(exc.as_payload())
        await log_payment_event(
            db,
            event_type="flutterwave.subaccount.ensure.failed",
            reference=link.code,
            status="failed",
            message=link.flutterwave_subaccount_error,
            payload=error_payload,
        )
        await db.commit()
        raise HTTPException(status_code=502, detail="Could not prepare recipient settlement account. Check Railway payment_event logs for Flutterwave's subaccount response.")


async def finalize_flutterwave_link_payment(db: AsyncSession, tx_ref: str, transaction_id: str | int = None) -> dict:
    """
    Verifies a Flutterwave payment, settles the creator's net amount, and records the fee.
    Qreek's fee remains in the Qreek Flutterwave merchant balance; only net is transferred out.
    """
    tx_result = await db.execute(select(Transaction).where(Transaction.reference == tx_ref).with_for_update())
    tx = tx_result.scalar_one_or_none()
    if not tx:
        await log_payment_event(db, event_type="payment.finalize.missing_reference", reference=tx_ref, transaction_id=transaction_id, status="failed")
        raise HTTPException(status_code=404, detail="Payment reference not found.")

    link_result = await db.execute(select(PaymentLink).where(PaymentLink.id == tx.pool_id).with_for_update())
    link = link_result.scalar_one_or_none()
    if not link:
        await log_payment_event(db, event_type="payment.finalize.missing_link", reference=tx_ref, transaction_id=transaction_id, status="failed")
        raise HTTPException(status_code=404, detail="Payment link not found for reference.")

    if tx.status == "completed" and tx.payout_status == "completed":
        await log_payment_event(db, event_type="payment.finalize.idempotent_completed", reference=tx_ref, transaction_id=transaction_id, status="completed")
        return {"payment": _payment_dict(tx)}

    if not transaction_id:
        await log_payment_event(db, event_type="payment.finalize.missing_transaction_id", reference=tx_ref, status="failed")
        raise HTTPException(status_code=400, detail="Flutterwave transaction_id is required for verification.")

    await log_payment_event(db, event_type="flutterwave.verify.started", reference=tx_ref, transaction_id=transaction_id, status="started")
    verified = await verify_transaction(transaction_id)
    data = verified.get("data", {})
    flw_status = str(data.get("status", "")).lower()
    flw_ref = data.get("tx_ref")
    flw_currency = data.get("currency")
    flw_amount = float(data.get("amount") or 0)

    if flw_ref != tx.reference:
        await log_payment_event(db, event_type="flutterwave.verify.reference_mismatch", reference=tx_ref, transaction_id=transaction_id, status="failed", payload={"flutterwave_ref": flw_ref})
        raise HTTPException(status_code=400, detail="Flutterwave reference does not match this payment.")
    if flw_currency != "NGN":
        await log_payment_event(db, event_type="flutterwave.verify.currency_mismatch", reference=tx_ref, transaction_id=transaction_id, status="failed", payload={"currency": flw_currency})
        raise HTTPException(status_code=400, detail="Unsupported payment currency.")
    if round(flw_amount, 2) != round(tx.gross_amount or tx.amount, 2):
        await log_payment_event(db, event_type="flutterwave.verify.amount_mismatch", reference=tx_ref, transaction_id=transaction_id, status="failed", payload={"flutterwave_amount": flw_amount, "expected_amount": tx.gross_amount or tx.amount})
        raise HTTPException(status_code=400, detail="Flutterwave amount does not match this payment.")
    if flw_status != "successful":
        await log_payment_event(db, event_type="flutterwave.verify.not_successful", reference=tx_ref, transaction_id=transaction_id, status=flw_status, payload={"flutterwave_status": flw_status})
        raise HTTPException(status_code=400, detail="Flutterwave payment is not successful.")

    tx.provider_transaction_id = str(data.get("id") or transaction_id)
    tx.provider_fee = _provider_fee(data)
    tx.provider_settled_amount = _provider_settled_amount(data, tx.gross_amount or tx.amount, tx.provider_fee)
    recipient_amount = tx.net_amount or tx.ngn_amount
    was_unsettled = tx.payout_status not in ("completed", "split_settlement")
    await log_payment_event(
        db,
        event_type="flutterwave.verify.successful",
        reference=tx_ref,
        transaction_id=transaction_id,
        status="successful",
        payload={
            "checkout_amount": tx.gross_amount or tx.amount,
            "recipient_amount": recipient_amount,
            "qreek_fee": tx.qreek_fee or tx.fee,
            "provider_fee": tx.provider_fee,
            "provider_settled_amount": tx.provider_settled_amount,
        },
    )
    if link.flutterwave_subaccount_id:
        tx.status = "completed"
        tx.provider = "flutterwave"
        tx.payout_status = "split_settlement"
        tx.payout_reference = link.flutterwave_subaccount_id
        tx.payout_error = None
        if was_unsettled:
            link.use_count = (link.use_count or 0) + 1
            link.total_collected = (link.total_collected or 0) + (tx.net_amount or tx.ngn_amount or 0)
        await log_payment_event(
            db,
            event_type="flutterwave.split.completed",
            reference=tx_ref,
            transaction_id=transaction_id,
            status="completed",
            payload={
                "subaccount_id": link.flutterwave_subaccount_id,
                "recipient_amount": recipient_amount,
                "qreek_fee": tx.qreek_fee or tx.fee,
                "checkout_amount": tx.gross_amount or tx.amount,
            },
        )
        await db.commit()
        return {"payment": _payment_dict(tx)}

    if (tx.provider_settled_amount or 0) < recipient_amount:
        tx.status = "payout_pending"
        tx.payout_status = "pending"
        tx.payout_reference = f"{tx.reference}_NET"
        tx.payout_error = (
            f"Flutterwave settled {tx.provider_settled_amount}, below recipient amount {recipient_amount}. "
            "Increase provider-fee allowance or use Flutterwave split settlement."
        )
        await log_payment_event(db, event_type="payout.skipped.insufficient_settlement", reference=tx_ref, transaction_id=transaction_id, status="pending", message=tx.payout_error)
        await db.commit()
        return {"payment": _payment_dict(tx)}

    was_unsettled = tx.payout_status != "completed"
    tx.status = "processing"
    tx.provider = "flutterwave"
    tx.payout_status = "pending"

    # Transfer the customer's net amount after verifying the payment.
    # The transaction only becomes completed after this recipient payout works.
    try:
        await log_payment_event(
            db,
            event_type="flutterwave.transfer.started",
            reference=tx_ref,
            transaction_id=transaction_id,
            status="started",
            payload={"amount": recipient_amount, "bank_code": link.bank_code, "account_number_last4": (link.bank_account or "")[-4:]},
        )
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
        await log_payment_event(db, event_type="flutterwave.transfer.completed", reference=tx_ref, transaction_id=transaction_id, status="completed", payload=transfer.get("data", transfer))
    except Exception as exc:
        tx.status = "payout_pending"
        tx.payout_status = "pending"
        tx.payout_reference = f"{tx.reference}_NET"
        tx.payout_error = str(exc)[:1000]
        await log_payment_event(db, event_type="flutterwave.transfer.failed", reference=tx_ref, transaction_id=transaction_id, status="pending", message=tx.payout_error)

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

    # Enforce one active personal payment link per user
    existing_link_result = await db.execute(
        select(PaymentLink).where(
            PaymentLink.created_by == phone,
            PaymentLink.is_active == True,
            PaymentLink.pool_id.is_(None)
        )
    )
    if existing_link_result.scalars().first():
        raise HTTPException(
            status_code=400,
            detail="You already have an active personal payment link. Please edit your existing link's bank details instead of creating a new one."
        )

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

    creator_security_result = await db.execute(
        select(UserSecurity).where(UserSecurity.phone == phone)
    )
    creator_security = creator_security_result.scalar_one_or_none()

    try:
        await log_payment_event(
            db,
            event_type="flutterwave.subaccount.create.started",
            status="started",
            payload={"link_id": link.id, "bank_code": body.bank_code, "account_number_last4": body.bank_account[-4:]},
        )
        subaccount = await create_collection_subaccount(
            account_bank=body.bank_code,
            account_number=body.bank_account,
            business_name=body.title,
            business_mobile=phone,
            business_email=creator_security.recovery_email if creator_security else None,
            split_type="percentage",
            split_value=0.9975,
        )
        data = subaccount.get("data", {})
        sub_id = data.get("subaccount_id") or data.get("id")
        link.flutterwave_subaccount_id = str(sub_id) if sub_id else None
        link.flutterwave_subaccount_status = "active" if link.flutterwave_subaccount_id else "missing_id"
        link.flutterwave_subaccount_error = None if link.flutterwave_subaccount_id else str(subaccount)[:1000]
        await log_payment_event(
            db,
            event_type="flutterwave.subaccount.create.completed",
            reference=link.code,
            status=link.flutterwave_subaccount_status,
            payload={"link_id": link.id, "subaccount_id": link.flutterwave_subaccount_id, "flutterwave": data},
        )
    except Exception as exc:
        link.flutterwave_subaccount_status = "failed"
        link.flutterwave_subaccount_error = str(exc)[:1000]
        error_payload = {"link_id": link.id}
        if isinstance(exc, FlutterwaveAPIError):
            error_payload.update(exc.as_payload())
        await log_payment_event(
            db,
            event_type="flutterwave.subaccount.create.failed",
            reference=link.code,
            status="failed",
            message=link.flutterwave_subaccount_error,
            payload=error_payload,
        )
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
    await _ensure_link_subaccount(db, link)

    recipient_amount = link.amount if not link.is_flexible else body.amount
    if not recipient_amount or recipient_amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid amount.")
    payment_description = (body.payment_description or body.note or "").strip()
    if not payment_description:
        raise HTTPException(status_code=400, detail="Payment description is required.")

    checkout_amount, fee, provider_fee_estimate = await _checkout_total_for_recipient(recipient_amount)
    platform_charge = round(checkout_amount - recipient_amount, 2)
    idempotency_key = body.idempotency_key or f"{code.upper()}:{body.phone or body.payer_phone or 'anon'}:{recipient_amount}:{payment_description}"
    await log_payment_event(
        db,
        event_type="checkout.quote.created",
        reference=None,
        status="created",
        payload={"recipient_amount": recipient_amount, "checkout_amount": checkout_amount, "qreek_fee": fee, "provider_fee_estimate": provider_fee_estimate, "platform_charge": platform_charge, "subaccount_id": link.flutterwave_subaccount_id},
    )

    existing_result = await db.execute(select(Transaction).where(Transaction.idempotency_key == idempotency_key).with_for_update())
    existing = existing_result.scalar_one_or_none()
    if existing:
        if existing.status in ("completed", "processing", "payout_pending"):
            await log_payment_event(db, event_type="checkout.idempotent.recorded", reference=existing.reference, status=existing.status)
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
            await log_payment_event(db, event_type="checkout.idempotent.reused_url", reference=existing.reference, status=existing.status)
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
        await log_payment_event(db, event_type="checkout.transaction.created", reference=ref, status="pending", payload={"checkout_amount": checkout_amount, "recipient_amount": recipient_amount, "qreek_fee": fee, "provider_fee_estimate": provider_fee_estimate, "platform_charge": platform_charge, "subaccount_id": link.flutterwave_subaccount_id})
    else:
        tx = existing

    await log_payment_event(db, event_type="flutterwave.checkout.started", reference=ref, status="started", payload={"checkout_amount": checkout_amount, "subaccount_id": link.flutterwave_subaccount_id, "platform_charge": platform_charge})
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
        subaccounts=[{
            "id": link.flutterwave_subaccount_id,
            "transaction_charge_type": "flat_subaccount",
            "transaction_charge": recipient_amount,
        }],
    )
    checkout_url = checkout.get("data", {}).get("link")
    tx.provider_checkout_url = checkout_url
    await log_payment_event(db, event_type="flutterwave.checkout.created", reference=ref, status="created", payload={"checkout_url": checkout_url, "flutterwave": checkout.get("data", {})})
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
        await log_payment_event(db, event_type="flutterwave.redirect.not_successful", reference=body.tx_ref, transaction_id=body.transaction_id, status=body.status)
        raise HTTPException(status_code=400, detail="Flutterwave did not mark this payment successful.")
    if not body.tx_ref:
        await log_payment_event(db, event_type="flutterwave.redirect.missing_tx_ref", transaction_id=body.transaction_id, status="failed")
        raise HTTPException(status_code=400, detail="Missing Flutterwave tx_ref.")
    await log_payment_event(db, event_type="flutterwave.redirect.received", reference=body.tx_ref, transaction_id=body.transaction_id, status=body.status)
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
        await log_payment_event(db, event_type="checkout.status.missing_reference", reference=tx_ref, status="failed")
        raise HTTPException(status_code=404, detail="Payment reference not found.")
    await log_payment_event(db, event_type="checkout.status.polled", reference=tx_ref, status=tx.status, payload={"payout_status": tx.payout_status})
    return {"payment": _payment_dict(tx)}


@router.get("/debug/events/{reference}")
async def get_payment_events(
    reference: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns recent payment events for a reference or link code. This is for
    Railway/live debugging when provider errors need to be inspected quickly.
    """
    phone = claims["phone"]
    link_result = await db.execute(
        select(PaymentLink).where(
            PaymentLink.created_by == phone,
            or_(PaymentLink.code == reference.upper(), PaymentLink.id == reference),
        )
    )
    link = link_result.scalar_one_or_none()
    tx_result = await db.execute(
        select(Transaction).where(
            Transaction.user_phone == phone,
            or_(Transaction.reference == reference, Transaction.tx_ref == reference),
        )
    )
    tx = tx_result.scalar_one_or_none()
    if not link and not tx:
        raise HTTPException(status_code=404, detail="No payment events found for this account.")

    refs = {reference}
    if link:
        refs.update({link.code, link.id})
    if tx:
        refs.update(v for v in (tx.reference, tx.tx_ref, tx.pool_id) if v)

    result = await db.execute(
        select(PaymentEvent)
        .where(PaymentEvent.reference.in_(list(refs)))
        .order_by(desc(PaymentEvent.created_at))
        .limit(50)
    )
    events = result.scalars().all()
    return {
        "events": [
            {
                "created_at": e.created_at.isoformat() if e.created_at else None,
                "event_type": e.event_type,
                "status": e.status,
                "message": e.message,
                "payload": e.payload,
            }
            for e in events
        ]
    }


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
