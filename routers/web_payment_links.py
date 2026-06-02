"""
@file web_payment_links.py
@description Payment Links API — facilitates shareable URLs for receiving NGN payments.
Payment links (non-pool) are *created once* and unique per user for security (stable code).
Bank details (and limited other fields) are edited on the existing link; creating a
second active personal link is blocked.

Anyone (Qreek user or not) can pay via a link. The split happens at payment success:
Qreek collects its 0.25% fee into our FW main balance; the link creator's recipient
amount goes to *their* bank (stored on the link) via the Flutterwave subaccount
settlement. There is *no transfer fallback* and no create_transfer for recipient funds
on these link payments. Split is enforced by the subaccounts override passed to
initialize_checkout + the split_settlement path in finalize.

See create_link (one-active guard + edit promise), the new update_link, pay_link
(must have sub ready), and finalize (always split if sub configured on link).

Sub accounts use split_value=0.0025 at creation (plus tx override) so that the
split at success does the right thing.

Flow:
1. Creation (once per user): define title/amount/bank. Subaccount created for split.
2. Edit (bank etc on the same link): changes bank -> old sub cleared, new sub created
   for the (possibly test) bank account. Same link code preserved.
3. Resolution + pay: checkout created *with* subaccounts split instruction.
4. On success: verify + mark split_settlement (split already performed by FW).
5. Security + limits enforced.
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
from services.flutterwave_service import FlutterwaveAPIError, create_collection_subaccount, initialize_checkout, query_transaction_fee, verify_transaction

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


class UpdateLinkIn(BaseModel):
    """
    Partial update for a payment link. Per requirements, links (for non-pool payments)
    are created once and are unique per user (enforced in create_link). Only bank details
    *must* be editable (to support testing different accounts on the same link/code without
    creating new links, which the one-active check + "edit your existing" error message
    at create_link:444 expects). We also allow common safe fields for practicality.
    Changing bank_account/bank_code will clear the old subaccount and (re)create a fresh
    one using create_collection_subaccount (with split_value=0.0025 so main gets 0.25%
    commission by default, plus tx-time override ensures split at success).
    """
    title: Optional[str] = None
    description: Optional[str] = None
    amount: Optional[float] = None  # None = flexible
    bank_account: Optional[str] = None
    bank_code: Optional[str] = None
    max_uses: Optional[int] = None
    expires_days: Optional[int] = None


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
    can settle the recipient directly through split payments (NO transfer fallback).

    Link payments for non-pool users must always split at success: Qreek fee to
    main merchant balance, recipient_amount to the subaccount's linked bank.

    If sub creation failed previously, we do not allow checkout (caller checks).
    Use the update/edit endpoint to change bank details on the *single* user-unique
    link (see create_link:441 one-active enforcement + new update handler); editing
    bank clears old sub and creates fresh one with correct split_value=0.0025.

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
        # ERROR (pre-fix, this block at ~189-198):
        #   Allowed pay_link to continue and create checkout even with no/failed subaccount_id.
        #   Then finalize would hit the transfer fallback (or insufficient). This violated
        #   "split at the point of payment success" and "no transfer fallback".
        #   Combined with one-active-per-user (create_link:441) + no edit endpoint (despite
        #   error msg at create_link:444 promising "Please edit your existing link's bank details"),
        #   users had to deactivate/create-new per test bank (or live with bad split_value=0.9975
        #   sub on their canonical link).
        # FIX: Do not silently proceed. The caller (pay_link) will now check after _ensure and
        #   reject checkout if no subaccount_id. Bank edit endpoint (new) + re-create sub on
        #   bank change lets you update the single unique link's bank (and get fresh correct sub
        #   with split_value=0.0025) without new links.
        # System with error: payments could succeed without split configured -> either reversed
        #   funds or transfer attempts (IP whitelist failures).
        # System with fix: pay is blocked until a good subaccount split is attached to the link.
        logger.warning(
            "Subaccount creation previously failed for link %s (status=failed, last_error=%s) -- pay will be rejected until bank is edited",
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
            split_value=0.0025,  # main/Qreek commission 0.25%; sub (user) gets remainder. See also tx override in pay_link.
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
    Verifies a Flutterwave payment for a link, records the fee (which stays in main
    merchant balance via the split), and marks the tx completed with payout_status=split_settlement.
    The actual split (Qreek fee to main, recipient_amount to subaccount's bank) is
    performed by Flutterwave because we passed the subaccounts override when creating
    the hosted checkout (see pay_link + initialize_checkout). There is no create_transfer
    / transfer fallback for link recipient funds.
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

    if tx.status == "completed" and tx.payout_status in ("completed", "split_settlement"):
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

    # ERROR (before this fix, lines ~316-418 in finalize_flutterwave_link_payment + _ensure:189-198):
    # - If link had flutterwave_subaccount_id (we passed subaccounts override to /payments at checkout),
    #   but due to wrong split_value=0.9975 at sub create (lines 218,494 pre-fix) + wrong override
    #   "flat_subaccount" + recipient_amount (large) at pay_link:640 pre-fix, FW allocated the recipient
    #   share to main merchant balance instead of sub (reversed split). See user's logs: settled 100.09
    #   to main, test recipient bank got 0, "split.completed" logged anyway, then polling showed pending.
    # - Even when sub failed, _ensure allowed checkout to proceed (comment: "manual transfer payout path will handle").
    # - finalize then fell through (if not use_split or no sub id) to create_transfer (the fallback path).
    # - System behaviour with error: link payments (non-pool) did not reliably split at success; either
    #   wrong party got funds or transfer was attempted (which fails with IP whitelist on prod). User had
    #   to create new links per test bank (violating "create once, edit bank" + one-active-per-user at 441-445).
    # FIX: (a) set split_value=0.0025 at both sub creates (now lines 218,494); (b) send correct override
    #   at checkout time: "flat" + qreek_fee (main's exact commission) -- now lines 652-655; (c) in
    #   finalize, *always* take split_settlement path (no transfer) when the link had subaccount_id at
    #   checkout (meaning we instructed split); (d) remove all transfer/insufficient/ detection-fallback
    #   code from this function (no more fallback); (e) block in pay_link if no sub ready. This enforces
    #   "payments to split at the point of payment success" with no transfer fallback.
    # System behaviour *with* fix: For a link with sub id (ensured or created at edit/create), checkout
    #   is initialized with subaccounts override telling FW "main gets flat= qreek_fee, sub gets rest".
    #   On charge success + verify, we mark split_settlement + completed immediately (no create_transfer).
    #   Qreek fee stays in main FW balance via split; recipient amount settles directly to the sub's
    #   linked bank. Old bad subs (0.9975) are still forced correct by the tx override. Edit bank (new
    #   endpoint) lets you change test bank on the *same* unique link and gets fresh sub with good config.
    if link.flutterwave_subaccount_id:
        was_unsettled = tx.payout_status not in ("completed", "split_settlement")
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

    # No subaccount configured for this link payment (should be prevented at pay time for new flows).
    # Do not fallback to transfer. Mark pending for manual resolution; split is mandatory.
    tx.status = "payout_pending"
    tx.payout_status = "pending"
    tx.payout_reference = f"{tx.reference}_NET"
    tx.payout_error = "No Flutterwave subaccount split configured for this link at payment time. Split is required; no transfer fallback. Edit link bank details to recreate subaccount."
    await log_payment_event(db, event_type="payout.skipped.no_split_config", reference=tx_ref, transaction_id=transaction_id, status="pending", message=tx.payout_error)
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

    # ERROR (this + missing update pre-fix): This guard (one active personal/non-pool link per user)
    # + the error message promising edit, but no PUT handler existed until now. Result: to test
    # different recipient banks you had to deactivate + POST new (violating "created once", "unique
    # to a user for better security", and "only the bank details can be edited").
    # The subaccount (with its split config) is created at link creation time (or on first pay via
    # _ensure), so bad config (0.9975) stuck until new link.
    #
    # FIX + intended behaviour (post this change): Personal payment links (pool_id null) are
    # generated once per user and are unique/stable (same code/url for security). To change the
    # destination bank (e.g. for testing), call PUT /.../{link_id} with new bank details. The
    # handler clears the sub* fields and creates a *new* subaccount for the new bank (using
    # split_value=0.0025). The link id/code/created_by stay the same. Pay on it will use split
    # (override at checkout time + always-split path in finalize, no transfer fallback).
    # Enforce still applies: you can't create a second active personal link while one exists.
    #
    # System with error: multiple links, or stuck with bad sub on the "canonical" one; payments
    # didn't split correctly at success.
    # System with fix: one link per user, edit bank to test/switch, fresh correct sub, split
    # always happens at success for the payment.
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
            split_value=0.0025,  # main/Qreek 0.25% commission (the value on sub record is *main's* share per FW docs). Override at pay time + edit support ensures correct split behaviour even for old links.
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


@router.put("/{link_id}")
async def update_link(
    link_id: str,
    body: UpdateLinkIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Updates an existing payment link (the single active personal one per user).
    This implements the "edit your existing link's bank details instead of creating
    a new one" contract from the create_link error (line 444).

    ERROR (pre-fix): No edit existed. Combined with one-active-personal-link guard
    (create_link:433-445) + "links created once, unique to user", testing a different
    recipient bank required deactivate + create-new (or live with the subaccount created
    at original create time). Old subs created with split_value=0.9975 (reversed
    commission: main got ~99.75%) caused split at payment success to send recipient
    funds to Qreek main balance instead of the link owner's bank (see user's
    flutterwave dashboard + payment_event logs with provider_settled_amount ~100,
    recipient bank 0, and "I will keep creating a new link each time to test").

    FIX: This endpoint + logic below. On bank change we clear sub* fields and
    (re)create subaccount (using the now-correct split_value=0.0025 at create time
    + the tx override "flat" + qf at pay_link:652 which forces split allocation
    at the /payments hosted checkout creation time). Pay will only succeed if sub
    ready (see pay_link post-_ensure check). No transfer fallback anywhere in
    link finalize.

    Only owner can edit. Changing bank triggers fresh subaccount (old sub for
    previous bank is left on FW side; that's expected). Other fields are updatable
    for convenience while keeping the link/code stable for security ("generated once").

    System behaviour with error: users forced to new links per bank test; bad
    sub config stuck on the canonical link; payments used transfer fallback or
    had reversed shares.
    System behaviour with fix: edit bank on your one unique link -> sub recreated
    with correct config -> pay once -> split at success (Qreek fee in main balance,
    user money to their stored bank via sub settlement). Unique link preserved.
    """
    phone = claims["phone"]

    result = await db.execute(
        select(PaymentLink)
        .where(PaymentLink.id == link_id, PaymentLink.created_by == phone)
        .with_for_update()
    )
    link = result.scalar_one_or_none()
    if not link:
        raise HTTPException(status_code=404, detail="Link not found.")

    # Track if we need to (re)create subaccount
    bank_changed = False
    new_bank_account = body.bank_account if body.bank_account is not None else link.bank_account
    new_bank_code = body.bank_code if body.bank_code is not None else link.bank_code

    if body.bank_code is not None or body.bank_account is not None:
        if new_bank_code != link.bank_code or new_bank_account != link.bank_account:
            bank = resolve_bank(new_bank_code)
            if not bank:
                raise HTTPException(status_code=400, detail=f"Invalid bank code: {new_bank_code}")
            # Clear old sub so ensure/create will make a fresh one for the *new* bank account.
            # This is how you "change test bank" on the single link without new codes/links.
            link.flutterwave_subaccount_id = None
            link.flutterwave_subaccount_status = None
            link.flutterwave_subaccount_error = None
            link.bank_account = new_bank_account
            link.bank_code = new_bank_code
            link.bank_name = bank["name"]
            bank_changed = True

    # Apply other updatable fields (title/desc/amount etc can change on the stable link)
    if body.title is not None:
        link.title = body.title
    if body.description is not None:
        if not body.description.strip():
            raise HTTPException(status_code=400, detail="Description is required.")
        link.description = body.description
    if "amount" in body.model_fields_set:
        # Client explicitly provided amount (can be a number or null/None to make flexible).
        # This supports editing the amount/flexibility on the stable unique link.
        link.amount = body.amount
        link.is_flexible = body.amount is None
    if body.max_uses is not None:
        link.max_uses = body.max_uses
    if body.expires_days is not None:
        from datetime import timedelta
        link.expires_at = datetime.utcnow() + timedelta(days=body.expires_days) if body.expires_days > 0 else None

    await db.commit()

    # If bank changed, (re)create the subaccount now (like create_link does), using
    # correct split_value=0.0025. If this fails, link stays with status=failed and
    # subsequent pay will be rejected until you edit to a valid bank again.
    if bank_changed:
        creator_security_result = await db.execute(
            select(UserSecurity).where(UserSecurity.phone == phone)
        )
        creator_security = creator_security_result.scalar_one_or_none()
        try:
            await log_payment_event(
                db,
                event_type="flutterwave.subaccount.create.started",
                status="started",
                payload={"link_id": link.id, "bank_code": new_bank_code, "account_number_last4": (new_bank_account or "")[-4:]},
            )
            subaccount = await create_collection_subaccount(
                account_bank=new_bank_code,
                account_number=new_bank_account,
                business_name=link.title,
                business_mobile=phone,
                business_email=creator_security.recovery_email if creator_security else None,
                split_type="percentage",
                split_value=0.0025,
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

    # ERROR (pre-fix): after _ensure, pay proceeded even if no flutterwave_subaccount_id
    # (see _ensure:189 failed case + finalize transfer branch). This allowed non-split paths.
    # FIX (this check + the always-split in finalize): enforce that split must be ready before
    # any checkout is created for the link. Combined with bank edit support, you edit the
    # one-and-only personal link (per create_link:441) to a test bank -> sub recreated with
    # good config -> pay -> split at success. No new link per test, no transfer fallback.
    if not link.flutterwave_subaccount_id:
        raise HTTPException(
            status_code=502,
            detail="Recipient subaccount split not configured for this link (previous creation failed or bank not set). Edit the link's bank details to trigger subaccount creation with correct split, then retry. See payment_event logs for details.",
        )

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

    subaccounts = None
    if link.flutterwave_subaccount_id:
        # This override is sent at *checkout creation time* (before payer pays).
        # "flat" + fee (qreek's 0.25% of recipient) tells FW: main merchant gets exactly
        # this flat commission on the tx; the subaccount (recipient) gets the rest.
        # This + the correct split_value=0.0025 on the sub record ensures split at
        # success gives Qreek fee to our balance, user money to their bank.
        # (Previously: flat_subaccount + recipient_amount reversed it; main got the 100.)
        # The override takes precedence over whatever split_value is stored on the sub
        # (so even links created with old 0.9975 get correct split on payments).
        subaccounts = [{
            "id": link.flutterwave_subaccount_id,
            "transaction_charge_type": "flat",
            "transaction_charge": fee,
        }]

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
        subaccounts=subaccounts,
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
