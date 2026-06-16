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
from sqlalchemy import select, desc, or_, func
from sqlalchemy.exc import IntegrityError
from pydantic import BaseModel
from typing import Optional

from database.session import get_db
from database.models import FamilyGroup, FamilyMember, PaymentEvent, PaymentLink, Transaction, UserSecurity
from core.web_jwt import decode_token
from core.banks import resolve_bank
from services.payment_event_logger import log_payment_event
from services.flutterwave_service import FlutterwaveAPIError, create_collection_subaccount, find_collection_subaccount, initialize_checkout, query_transaction_fee, resolve_account, update_subaccount, update_subaccount_split, verify_transaction
from services.sms_service import send_link_payment_received_sms, send_payment_receipt_sms

router = APIRouter(prefix="/api/v1/payment-links", tags=["payment-links"])

FEE_PCT = 0.0025  # 0.25% default for personal (non-pool) payment links
GROUP_FEE_PCT = 0.0015  # 0.15% for group collection links (pools and family links)


async def _checkout_total_for_recipient(recipient_amount: float, fee_pct: float = FEE_PCT) -> tuple[float, float, float]:
    """
    Builds a one-time payer total where the link owner receives the requested
    amount and Qreek's fee is added on top of that amount.
    fee_pct: 0.0025 for personal links, 0.0015 for pool collection links.
    """
    qreek_fee = round(recipient_amount * fee_pct, 2)
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
    expires_days: Optional[int] = None
    provider:     Optional[str] = "flutterwave"
    pool_id:      Optional[str] = None  # if set, this is a pool collection link (0.15% fee, tied to pool for history)
    family_id:    Optional[str] = None  # if set, this is a family collection link (0.15% fee, tied to family history)


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
    expires_days: Optional[int] = None


def _payment_dict(tx: Transaction) -> dict:
    """
    Returns the public payment status shape used by checkout redirects and
    polling. A transaction is only complete after recipient settlement succeeds.
    Added created_at + payment_description so the Settlements table (PaymentLinks.jsx)
    can show Date and the details View has more tx info (not just events).
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
        "created_at": tx.created_at.isoformat() if tx.created_at else None,
        "payment_description": tx.payment_description,
        "payer_name": tx.payer_name,
        "payer_phone": tx.payer_phone,
    }


def _public_pool_payment_dict(tx: Transaction) -> dict:
    """
    Public payment summary for pool/family ledger views.
    The payer details are shown exactly as recorded in the database.
    """
    return {
        "reference": tx.reference,
        "created_at": tx.created_at.isoformat() if tx.created_at else None,
        "amount": tx.net_amount or tx.ngn_amount or tx.amount,
        "payer_name": tx.payer_name or "Anonymous",
        "payer_phone": masked_phone,
        "payment_description": tx.payment_description,
        "status": tx.status,
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
        "use_count": l.use_count,  # still tracked for info, but no max_uses enforcement
        "total_collected": l.total_collected,
        "expires_at": l.expires_at.isoformat() if l.expires_at else None,
        "is_active": l.is_active,
        "created_at": l.created_at.isoformat() if l.created_at else None,
        "url": f"https://qreekfinance.org/p/{l.code}",
        "pool_id": l.pool_id,  # present for pool collection links (0.15% fee, public ledger, data always visible after expire)
        "family_id": l.family_id,
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


async def _get_live_link(db: AsyncSession, code: str, *, for_payment: bool = True) -> PaymentLink:
    result = await db.execute(select(PaymentLink).where(PaymentLink.code == code.upper()))
    link = result.scalar_one_or_none()
    if not link or not link.is_active:
        await log_payment_event(db, event_type="link.resolve.not_found_or_inactive", reference=code, status="failed")
        raise HTTPException(status_code=404, detail="Payment link not found.")
    if link.expires_at and link.expires_at < datetime.utcnow():
        await log_payment_event(db, event_type="link.resolve.expired", reference=code, status="failed")
        if for_payment:
            # Block payments on expired links (pool or personal)
            raise HTTPException(status_code=410, detail="This payment link has expired.")
        else:
            # For pool collection links, allow resolve even if expired so that "every other data concerning it"
            # (history, totals, contributors, ledger) can ALWAYS be shown on the public checkout page / pool views.
            # Per user spec: unable to accept payments, but data always showable. Do NOT delete the link row.
            if not link.pool_id and not link.family_id:
                raise HTTPException(status_code=410, detail="This payment link has expired.")
            # else: return the expired pool link so frontend can render the full data/ledger without payment form
    # NOTE: max_uses removed entirely per spec (no max usage for any links; only expiration by date makes link inactive for new payments, but all received payment data remains visible forever).
    # Previously this check existed at this line and caused 410 for maxed links even for view.
    # Fix: removed the block and all references to max_uses in link creation/editing.
    # System behaviour: links only become inactive for payments on expire date (for_payment path); data always showable via resolve for pool links and owner views.
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
        # Ensure the subaccount's default split is correct (0.0025 for main) so the
        # Flutterwave dashboard shows the right config for this sub. Per-tx override
        # controls the actual split for payments.
        try:
            await update_subaccount(link.flutterwave_subaccount_id)
        except Exception:
            pass
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
        # A previous attempt failed, but the subaccount may already exist now or
        # Flutterwave may accept a retry for the same bank/account pair. Do not
        # hard-stop here; let the normal ensure/create flow attempt recovery.
        recovered = None
        if link.bank_code and link.bank_account:
            recovered = await find_collection_subaccount(link.bank_code, link.bank_account)
        if recovered:
            rs_id = recovered.get("subaccount_id") or recovered.get("id")
            link.flutterwave_subaccount_id = str(rs_id) if rs_id else None
            link.flutterwave_subaccount_status = "active" if link.flutterwave_subaccount_id else "missing_id"
            link.flutterwave_subaccount_error = None if link.flutterwave_subaccount_id else "Recovered subaccount missing id"
            await log_payment_event(
                db,
                event_type="flutterwave.subaccount.recovered",
                reference=link.code,
                status=link.flutterwave_subaccount_status,
                payload={"link_id": link.id, "subaccount_id": link.flutterwave_subaccount_id, "flutterwave": recovered},
            )
            try:
                await update_subaccount(link.flutterwave_subaccount_id)
            except Exception:
                pass
            await db.commit()
            return
        logger.warning(
            "Subaccount creation previously failed for link %s (status=failed, last_error=%s) -- retrying ensure/reuse",
            link.code,
            locked_link.flutterwave_subaccount_error,
        )
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
        sub_split_value = GROUP_FEE_PCT if (link.pool_id or link.family_id) else 0.0025
        subaccount = await create_collection_subaccount(
            account_bank=link.bank_code,
            account_number=link.bank_account,
            business_name=link.title,
            business_mobile=link.created_by,
            business_email=security.recovery_email if security else None,
            split_type="percentage",
            split_value=sub_split_value,  # 0.15% pool / 0.25% personal; tx override in pay_link wins for exact.
        )
        data = subaccount.get("data", {})
        # Prefer the RS_ code (data.subaccount_id) for flutterwave_subaccount_id because that is what
        # goes into checkout subaccounts[{"id": "..."}] and what user sees in FW. Numeric data.id is
        # only for management (update/delete/fetch single). See update_subaccount_split fix in
        # services/flutterwave_service.py which now resolves RS_ via list+match to avoid "Merchant not found".
        rs_id = data.get("subaccount_id") or data.get("id")
        numeric_id = data.get("id")
        link.flutterwave_subaccount_id = str(rs_id) if rs_id else None
        link.flutterwave_subaccount_status = "active" if link.flutterwave_subaccount_id else "missing_id"
        link.flutterwave_subaccount_error = None if link.flutterwave_subaccount_id else str(subaccount)[:1000]
        if numeric_id:
            # best-effort: stash numeric in error field only if no real error (for future; harmless, visible in debug)
            if not link.flutterwave_subaccount_error:
                link.flutterwave_subaccount_error = f"numeric_id={numeric_id}"  # overwritten on real error; used by update resolver if needed later
        await log_payment_event(
            db,
            event_type="flutterwave.subaccount.ensure.completed",
            reference=link.code,
            status=link.flutterwave_subaccount_status,
            payload={"link_id": link.id, "subaccount_id": link.flutterwave_subaccount_id, "flutterwave": data},
        )
        # Best-effort: update the subaccount record's default split so dashboard shows correct 0.25% for Qreek.
        # The per-tx override in checkout still controls the actual payment split.
        try:
            await update_subaccount(link.flutterwave_subaccount_id)
        except Exception:
            pass  # non-fatal
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
        await log_payment_event(db, event_type="link.subaccount.ensure.failed_hard", reference=link.code, status="failed", message=str(exc)[:300])
        raise HTTPException(status_code=502, detail="We couldn't set up your bank account for payments. Edit the link and try saving again.")


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
            if tx.family_id:
                family_result = await db.execute(select(FamilyGroup).where(FamilyGroup.id == tx.family_id).with_for_update())
                family = family_result.scalar_one_or_none()
                if family:
                    added = round(float(tx.net_amount or tx.ngn_amount or tx.amount or 0), 2)
                    family.balance_ngn = round((family.balance_ngn or 0) + added, 2)
                    family.total_contributed = round((family.total_contributed or 0) + added, 2)
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

        # Realtime SMS notifications to phones the moment payment succeeds (owner alert + payer receipt).
        # Called only on the verified split_settlement happy path (after commit).
        # Both functions are best-effort and log their own payment_event (sent/skipped/failed).
        # If no TERMII_API_KEY, they still create events and app logs (no external send).
        # This satisfies "realtime messaging to the phone numbers of users the moment they make payments".
        try:
            await send_link_payment_received_sms(
                owner_phone=link.created_by,
                link_title=link.title or "Qreek link",
                amount=recipient_amount,
                reference=tx_ref,
                payer_name=getattr(tx, "payer_name", None),
                db=db,
            )
            if getattr(tx, "payer_phone", None):
                await send_payment_receipt_sms(
                    payer_phone=tx.payer_phone,
                    link_title=link.title or "Qreek link",
                    amount=recipient_amount,
                    reference=tx_ref,
                    owner_bank_name=link.bank_name,
                    db=db,
                )
        except Exception:
            # SMS must never affect the payment confirmation path or raise to caller.
            pass

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
    await log_payment_event(db, event_type="link.create.started", status="started", payload={"has_bank": bool(body.bank_code and body.bank_account)})

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
    # one link per user, edit bank to test/switch, fresh correct sub, split
    # always happens at success for the payment.
    existing_link_result = await db.execute(
        select(PaymentLink).where(
            PaymentLink.created_by == phone,
            PaymentLink.is_active == True,
            PaymentLink.pool_id.is_(None)
        )
    )
    if existing_link_result.scalars().first() and not body.pool_id and not body.family_id:
        raise HTTPException(
            status_code=400,
            detail="You already have an active personal payment link. Please edit your existing link's bank details instead of creating a new one."
        )

    # For pool and family collection links: validate the destination group owner/admin.
    target_pool_id = body.pool_id
    target_family_id = body.family_id
    if target_pool_id and target_family_id:
        raise HTTPException(status_code=400, detail="Choose either a pool or family link, not both.")
    if target_pool_id:
        fpr = await db.execute(select(FiatPool).where(FiatPool.id == target_pool_id, FiatPool.creator_phone == phone))
        if not fpr.scalar_one_or_none():
            raise HTTPException(status_code=403, detail="Only the pool creator/admin can create collection links for a pool.")
    if target_family_id:
        family_result = await db.execute(select(FamilyGroup).where(FamilyGroup.id == target_family_id))
        family = family_result.scalar_one_or_none()
        if not family:
            raise HTTPException(status_code=404, detail="Family not found.")
        member_result = await db.execute(select(FamilyMember).where(FamilyMember.family_id == target_family_id, FamilyMember.user_phone == phone))
        member = member_result.scalar_one_or_none()
        if not member or member.role != "admin":
            raise HTTPException(status_code=403, detail="Only the family admin can create collection links for a family.")

    if not body.description.strip():
        raise HTTPException(status_code=400, detail="Description is required.")

    bank = resolve_bank(body.bank_code)
    if not bank:
        raise HTTPException(status_code=400, detail=f"Invalid bank code: {body.bank_code}")

    # For pool payment links: verify the bank details with Flutterwave before saving the link.
    # This ensures the account is valid (account name matches etc.) using FW's resolve.
    # File: routers/web_payment_links.py:568 (after local resolve_bank)
    # Error: previously bank could be saved without verification for pool collection links, leading to bad subaccounts later.
    # Fix: call resolve_account which hits /accounts/resolve ; if fails, 400 before creating link/sub.
    # System behaviour: pool link creation now requires successful FW account verification; personal links unchanged (sub create will still fail on bad bank).
    if target_pool_id or target_family_id:
        try:
            await resolve_account(body.bank_account, body.bank_code)
            await log_payment_event(db, event_type="pool.link.bank.verified", reference=None, status="success", payload={"pool_id": target_pool_id, "family_id": target_family_id, "bank_code": body.bank_code})
        except Exception as exc:
            await log_payment_event(db, event_type="pool.link.bank.verify_failed", reference=None, status="failed", message=str(exc)[:300])
            raise HTTPException(status_code=400, detail=f"Bank account verification failed using Flutterwave. Please check the account number and bank: {str(exc)[:200]}")

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
        expires_at=expires_at,
        pool_id=target_pool_id,  # pool collection link if set; enables 0.15% fee + public history on checkout + data always visible after expire
        family_id=target_family_id,
    )
    db.add(link)
    await db.commit()
    await db.refresh(link)

    creator_security_result = await db.execute(
        select(UserSecurity).where(UserSecurity.phone == phone)
    )
    creator_security = creator_security_result.scalar_one_or_none()

    sub_split_value = GROUP_FEE_PCT if (target_pool_id or target_family_id) else 0.0025
    try:
        await log_payment_event(
            db,
            event_type="flutterwave.subaccount.create.started",
            status="started",
            payload={"link_id": link.id, "bank_code": body.bank_code, "account_number_last4": body.bank_account[-4:], "is_pool": bool(target_pool_id), "is_family": bool(target_family_id)},
        )
        subaccount = await create_collection_subaccount(
            account_bank=body.bank_code,
            account_number=body.bank_account,
            business_name=body.title,
            business_mobile=phone,
            business_email=creator_security.recovery_email if creator_security else None,
            split_type="percentage",
            split_value=sub_split_value,  # 0.15% for pool links, 0.25% default; tx override at pay time always controls the exact flat fee for this collection.
        )
        data = subaccount.get("data", {})
        # Prefer the RS_ code (data.subaccount_id) for flutterwave_subaccount_id because that is what
        # goes into checkout subaccounts[{"id": "..."}] and what user sees in FW. Numeric data.id is
        # only for management (update/delete/fetch single). See update_subaccount_split fix in
        # services/flutterwave_service.py which now resolves RS_ via list+match to avoid "Merchant not found".
        rs_id = data.get("subaccount_id") or data.get("id")
        numeric_id = data.get("id")
        link.flutterwave_subaccount_id = str(rs_id) if rs_id else None
        link.flutterwave_subaccount_status = "active" if link.flutterwave_subaccount_id else "missing_id"
        link.flutterwave_subaccount_error = None if link.flutterwave_subaccount_id else str(subaccount)[:1000]
        if numeric_id:
            # best-effort: stash numeric in error field only if no real error (for future; harmless, visible in debug)
            if not link.flutterwave_subaccount_error:
                link.flutterwave_subaccount_error = f"numeric_id={numeric_id}"  # overwritten on real error; used by update resolver if needed later
        await log_payment_event(
            db,
            event_type="flutterwave.subaccount.create.completed",
            reference=link.code,
            status=link.flutterwave_subaccount_status,
            payload={"link_id": link.id, "subaccount_id": link.flutterwave_subaccount_id, "flutterwave": data},
        )
        # Best-effort update of sub default split for correct dashboard display.
        try:
            await update_subaccount(link.flutterwave_subaccount_id)
        except Exception:
            pass
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
    await log_payment_event(db, event_type="link.create.completed", reference=link.code, status="success", payload={"link_id": link.id, "sub_id": link.flutterwave_subaccount_id})
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
    await log_payment_event(db, event_type="link.update.started", status="started", payload={"link_id": link_id, "has_title": body.title is not None, "has_bank": bool(body.bank_code or body.bank_account)})

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
    if body.expires_days is not None:
        from datetime import timedelta
        link.expires_at = datetime.utcnow() + timedelta(days=body.expires_days) if body.expires_days > 0 else None

    await db.commit()

    # Always push current link.title as business_name + correct split (0.15% pool collection vs 0.25% personal).
    # This makes "I edited the name of the link" actually appear on the Flutterwave subaccount dashboard.
    if link.flutterwave_subaccount_id:
        try:
            sv = GROUP_FEE_PCT if (link.pool_id or link.family_id) else 0.0025
            await update_subaccount(
                link.flutterwave_subaccount_id,
                business_name=link.title,
                split_value=sv,
            )
        except Exception:
            pass

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
            sub_split_value = GROUP_FEE_PCT if (link.pool_id or link.family_id) else 0.0025
            subaccount = await create_collection_subaccount(
                account_bank=new_bank_code,
                account_number=new_bank_account,
                business_name=link.title,
                business_mobile=phone,
                business_email=creator_security.recovery_email if creator_security else None,
                split_type="percentage",
                split_value=sub_split_value,
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
            try:
                await update_subaccount(link.flutterwave_subaccount_id, business_name=link.title)
            except Exception:
                pass
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

    await log_payment_event(db, event_type="link.update.completed", reference=link.code, status="success", payload={"title_updated": body.title is not None, "bank_changed": bank_changed})
    return {"link": _link_dict(link, show_bank=True)}


@router.get("")
async def list_links(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    """
    Lists all payment links created by the authenticated user.
    """
    phone  = claims["phone"]
    # Return ALL links for the owner (no is_active filter). This ensures that even expired pool collection links
    # can always have their full data (history, totals, etc.) shown to the creator/admin via the dashboard / settlements.
    # Personal links that were manually deactivated are hard-deleted, so they won't appear.
    # (File: routers/web_payment_links.py:828 (approx after edit), error was owner couldn't view expired pool data after expire date.)
    result = await db.execute(
        select(PaymentLink).where(PaymentLink.created_by == phone).order_by(desc(PaymentLink.created_at)).limit(50)
    )
    links = result.scalars().all()
    return {"links": [_link_dict(l, show_bank=True) for l in links]}


@router.get("/{link_id}/settlements")
async def get_link_settlements(
    link_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
    page: int = 1,
    per_page: int = 10,
):
    """
    List all payments (transactions) received via this payment link, for the "Settlements"
    view in dashboard. Returns paginated list (10 per page), with full payment dicts.
    Includes action "view" on frontend to see details (e.g. full events or tx).
    """
    phone = claims["phone"]
    link_result = await db.execute(
        select(PaymentLink).where(PaymentLink.id == link_id, PaymentLink.created_by == phone)
    )
    link = link_result.scalar_one_or_none()
    if not link:
        raise HTTPException(status_code=404, detail="Link not found.")

    base_query = select(Transaction).where(
        Transaction.pool_id == link_id,
        Transaction.tx_type == "payment_link",
    ).order_by(desc(Transaction.created_at))

    # total count
    count_result = await db.execute(
        select(func.count()).select_from(Transaction).where(
            Transaction.pool_id == link_id,
            Transaction.tx_type == "payment_link",
        )
    )
    total = count_result.scalar_one() or 0

    offset = (page - 1) * per_page
    result = await db.execute(base_query.offset(offset).limit(per_page))
    txs = result.scalars().all()

    return {
        "payments": [_payment_dict(tx) for tx in txs],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if per_page else 1,
    }


@router.get("/resolve/{code}")
async def resolve_link(code: str, db: AsyncSession = Depends(get_db)):
    """
    Public endpoint to view a payment link by its unique code.
    Validates that the link exists, is active, has not expired, and has not reached max uses.
    For pool collection links (link.pool_id), includes recent_contributions so the public
    checkout page can show live ledger (who paid, when, how much, running context) as requested.
    """
    # Use for_payment=False for resolve so that expired *pool* collection links still return full data
    # (recent_contributions, totals, etc.) — payments are blocked but "can always show every other data concerning it".
    link = await _get_live_link(db, code, for_payment=False)
    resp = {"link": _link_dict(link)}
    if link.pool_id or link.family_id:
        # Public view of recent payments into this link (for transparency on checkout page)
        txs_res = await db.execute(
            select(Transaction).where(
                Transaction.pool_id == link.id,
                Transaction.tx_type == "payment_link",
            ).order_by(desc(Transaction.created_at)).limit(15)
        )
        recent = []
        for t in txs_res.scalars().all():
            recent.append({
                "date": t.created_at.isoformat() if t.created_at else None,
                "payer_name": t.payer_name or "",
                "payer_phone": t.payer_phone or "",
                "amount": t.net_amount or t.ngn_amount or t.amount,
                "reference": t.reference,
                "payment_description": t.payment_description,
                "status": t.status,
            })
        resp["recent_contributions"] = recent
        resp["pool_total_via_link"] = link.total_collected or 0
    return resp


@router.get("/public/{code}/contributions")
async def public_pool_contributions(
    code: str,
    db: AsyncSession = Depends(get_db),
    page: int = 1,
    per_page: int = 25,
):
    """
    Public, unauthenticated pool ledger endpoint.
    Returns paginated contribution history for pool links so the public payment
    page can show all payments without requiring login.
    """
    page = max(int(page or 1), 1)
    per_page = min(max(int(per_page or 25), 1), 100)

    link = await _get_live_link(db, code, for_payment=False)
    if not link.pool_id and not link.family_id:
        raise HTTPException(status_code=400, detail="This payment link does not have a public pool ledger.")

    count_result = await db.execute(
        select(func.count()).select_from(Transaction).where(
            Transaction.pool_id == link.id,
            Transaction.tx_type == "payment_link",
        )
    )
    total = count_result.scalar_one() or 0

    txs_res = await db.execute(
        select(Transaction).where(
            Transaction.pool_id == link.id,
            Transaction.tx_type == "payment_link",
        ).order_by(desc(Transaction.created_at)).offset((page - 1) * per_page).limit(per_page)
    )
    payments = [_public_pool_payment_dict(tx) for tx in txs_res.scalars().all()]

    return {
        "payments": payments,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if per_page else 1,
        "total_collected": link.total_collected or 0,
        "link": _link_dict(link),
    }


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
    # for_payment=True (default) so expired pool links (and personal) cannot accept payments.
    link = await _get_live_link(db, code)
    await log_payment_event(db, event_type="link.pay.started", reference=code, status="started", payload={"has_idempotency": bool(body.idempotency_key)})
    await _ensure_link_subaccount(db, link)

    # ERROR (pre-fix): after _ensure, pay proceeded even if no flutterwave_subaccount_id
    # (see _ensure:189 failed case + finalize transfer branch). This allowed non-split paths.
    # FIX (this check + the always-split in finalize): enforce that split must be ready before
    # any checkout is created for the link. Combined with bank edit support, you edit the
    # one-and-only personal link (per create_link:441) to a test bank -> sub recreated with
    # good config -> pay -> split at success. No new link per test, no transfer fallback.
    if not link.flutterwave_subaccount_id:
        await log_payment_event(db, event_type="pay.subaccount.missing", reference=code, status="failed")
        raise HTTPException(
            status_code=502,
            detail="We couldn't prepare this link for receiving payments right now. Please edit the bank details on the link and try again.",
        )

    recipient_amount = link.amount if not link.is_flexible else body.amount
    if not recipient_amount or recipient_amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid amount.")
    payment_description = (body.payment_description or body.note or "").strip()
    if not payment_description:
        raise HTTPException(status_code=400, detail="Payment description is required.")

    # tx override + fee: use 0.15% for pool collection links (link.pool_id set), 0.25% for personal
    link_fee_pct = GROUP_FEE_PCT if (link.pool_id or link.family_id) else FEE_PCT
    checkout_amount, fee, provider_fee_estimate = await _checkout_total_for_recipient(recipient_amount, fee_pct=link_fee_pct)
    platform_charge = round(checkout_amount - recipient_amount, 2)
    idempotency_key = body.idempotency_key or f"{code.upper()}:{body.phone or body.payer_phone or 'anon'}:{recipient_amount}:{payment_description}"
    await log_payment_event(
        db,
        event_type="checkout.quote.created",
        reference=None,
        status="created",
        payload={"recipient_amount": recipient_amount, "checkout_amount": checkout_amount, "qreek_fee": fee, "provider_fee_estimate": provider_fee_estimate, "platform_charge": platform_charge, "subaccount_id": link.flutterwave_subaccount_id},
    )

    # Proper idempotency: the provided key is tied to *one specific transaction/attempt* (1:1 with a tx ref).
    # Lookup *any* tx with this exact key (the unique constraint "ux_transactions_idempotency_key" enforces this).
    # If exists: return the existing tx's data (idempotent - whether it's a pending checkout to reuse,
    # or a completed one). This prevents duplicate tx creation for the same client key.
    # If the client sends a *fresh* key (as the updated frontend now does on every pay submit),
    # a new independent payment tx is created on the link -- exactly like multiple deposits
    # to one bank account number. The key is *not* "to the activity of the link" but to one tx.
    # See user note and frontend: always fresh crypto.randomUUID() per submit (no more sticky per phone/amount).
    # Old clients sending a previously-used key will get the previous tx's status (gracefully handled
    # in frontend to show receipt instead of error), but won't cause 500/unique violation.
    existing_result = await db.execute(
        select(Transaction).where(Transaction.idempotency_key == idempotency_key).with_for_update()
    )
    existing = existing_result.scalar_one_or_none()
    if existing:
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
        # even for completed/failed: return recorded status (idempotent), no new tx
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
            "checkout_url": existing.provider_checkout_url,
            "payment_url": existing.provider_checkout_url,
        }
    else:
        ref = "QRK_LNK_" + uuid.uuid4().hex[:10].upper()

    payer_name = (body.name or body.payer_name or "").strip()
    if not payer_name:
        raise HTTPException(status_code=400, detail="Payer name is required.")
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
            fee_pct=link_fee_pct,
            status="pending",
            provider="flutterwave",
            reference=ref,
            tx_ref=ref,
            idempotency_key=idempotency_key,
            payment_description=payment_description,
            payer_name=payer_name,
            payer_phone=payer_phone,
            pool_id=link.id,  # PaymentLink.id for per-link settlements / finalize lookup
            source_pool_id=link.pool_id,  # the actual pool id, for pool history even after link auto-delete on expire
            family_id=link.family_id,
            bank_account=link.bank_account,
            bank_code=link.bank_code,
            bank_name=link.bank_name,
        )
        db.add(tx)
        try:
            await db.commit()
        except IntegrityError:
            # Race: another request with the exact same fresh idempotency_key created the tx first.
            # Rollback and fetch the winner's tx (idempotent behaviour).
            await db.rollback()
            existing_result = await db.execute(
                select(Transaction).where(Transaction.idempotency_key == idempotency_key)
            )
            tx = existing_result.scalar_one()
            ref = tx.reference
        else:
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
    await log_payment_event(db, event_type="link.delete.started", status="started", payload={"link_id": link_id})
    result = await db.execute(
        select(PaymentLink).where(PaymentLink.id == link_id, PaymentLink.created_by == phone)
    )
    link = result.scalar_one_or_none()
    if not link:
        await log_payment_event(db, event_type="link.delete.not_found", status="failed")
        raise HTTPException(status_code=404, detail="Link not found.")
    # Hard delete so deactivated links are completely removed and not visible in dashboard (as requested).
    # Historical transactions referencing the link via pool_id remain for records.
    await db.delete(link)
    await db.commit()
    await log_payment_event(db, event_type="link.delete.completed", status="success", payload={"link_id": link_id})
    return {"message": "Payment link deleted."}
