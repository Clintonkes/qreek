from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Company, PayrollEntry, PayrollRun, Transaction
from database.session import get_db
from routers.web_payment_links import finalize_flutterwave_link_payment
from services.payment_event_logger import log_payment_event
from services.flutterwave_service import logger, verify_webhook_signature

router = APIRouter(prefix="/api/v1/flutterwave", tags=["flutterwave"])


@router.post("/webhook")
async def flutterwave_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    """
    Handles Flutterwave webhooks. The verif-hash header is checked before
    the backend verifies and mutates any Qreek ledger record.
    """
    payload_bytes = await request.body()
    signature = request.headers.get("flutterwave-signature")
    legacy_hash = request.headers.get("verif-hash")
    if not verify_webhook_signature(payload_bytes, signature, legacy_hash):
        await log_payment_event(db, event_type="flutterwave.webhook.invalid_signature", status="failed")
        return Response(status_code=401, content="Invalid Flutterwave signature")

    payload = await request.json()
    event = payload.get("event")
    data = payload.get("data", {})
    tx_ref = data.get("tx_ref")
    transaction_id = data.get("id")

    logger.info("Flutterwave webhook received: %s %s", event, tx_ref)
    await log_payment_event(
        db,
        event_type=f"flutterwave.webhook.{event or 'unknown'}",
        reference=tx_ref,
        transaction_id=transaction_id,
        status=data.get("status") or "received",
        payload={"event": event, "data": data},
    )

    if tx_ref and str(tx_ref).startswith("QRK_LNK_"):
        try:
            await finalize_flutterwave_link_payment(db, tx_ref, transaction_id)
        except Exception as exc:
            logger.exception("Could not finalize Flutterwave link payment %s: %s", tx_ref, exc)
            await log_payment_event(db, event_type="flutterwave.webhook.finalize_failed", reference=tx_ref, transaction_id=transaction_id, status="failed", message=str(exc)[:1000])
            await db.commit()
            return Response(status_code=500, content="Could not finalize payment")

    if tx_ref and str(tx_ref).startswith("QRK_WAL_"):
        try:
            await finalize_wallet_deposit(db, tx_ref, transaction_id)
        except Exception as exc:
            logger.exception("Could not finalize wallet deposit %s: %s", tx_ref, exc)
            await log_payment_event(db, event_type="flutterwave.webhook.wallet_deposit_failed", reference=tx_ref, transaction_id=transaction_id, status="failed", message=str(exc)[:1000])
            await db.commit()
            return Response(status_code=500, content="Could not finalize wallet deposit")

    if tx_ref and (str(tx_ref).startswith("QRK_PR_") or str(tx_ref).startswith("QRK_PR_RETRY_")):
        try:
            await finalize_payroll_transfer(db, tx_ref, transaction_id, payload)
        except Exception as exc:
            logger.exception("Could not finalize payroll transfer %s: %s", tx_ref, exc)
            await log_payment_event(db, event_type="flutterwave.webhook.payroll_transfer_failed", reference=tx_ref, transaction_id=transaction_id, status="failed", message=str(exc)[:1000])

    await db.commit()
    return Response(status_code=200, content="OK")


async def finalize_wallet_deposit(db: AsyncSession, tx_ref: str, transaction_id: str | int = None) -> dict:
    tx_result = await db.execute(select(Transaction).where(Transaction.reference == tx_ref).with_for_update())
    tx = tx_result.scalar_one_or_none()
    if not tx:
        await log_payment_event(db, event_type="wallet_deposit.finalize.missing_reference", reference=tx_ref, transaction_id=transaction_id, status="failed")
        raise HTTPException(status_code=404, detail="Wallet deposit reference not found.")

    if tx.status == "completed":
        return {"status": "already_completed"}

    from services.flutterwave_service import verify_transaction
    verified = await verify_transaction(transaction_id)
    data = verified.get("data", {})
    flw_status = str(data.get("status", "")).lower()

    if flw_status != "successful":
        tx.status = "failed"
        await db.commit()
        raise HTTPException(status_code=400, detail=f"Flutterwave payment status: {flw_status}")

    company_id = None
    if tx.event_metadata:
        company_id = tx.event_metadata.get("company_id")

    if not company_id:
        await log_payment_event(db, event_type="wallet_deposit.finalize.missing_company", reference=tx_ref, transaction_id=transaction_id, status="failed")
        raise HTTPException(status_code=400, detail="Company ID not found in deposit metadata.")

    co_result = await db.execute(select(Company).where(Company.id == company_id).with_for_update())
    co = co_result.scalar_one_or_none()
    if not co:
        raise HTTPException(status_code=404, detail="Company not found.")

    co.wallet_balance_ngn = round((co.wallet_balance_ngn or 0) + tx.amount, 2)
    tx.status = "completed"
    tx.provider_transaction_id = str(transaction_id)

    await log_payment_event(db, event_type="wallet_deposit.completed", reference=tx_ref, transaction_id=transaction_id, status="completed", payload={"company_id": company_id, "amount": tx.amount, "new_balance": co.wallet_balance_ngn})
    await db.commit()
    return {"status": "completed", "amount": tx.amount, "new_balance": co.wallet_balance_ngn}


async def finalize_payroll_transfer(db: AsyncSession, tx_ref: str, transaction_id: str | int, payload: dict) -> dict:
    data = payload.get("data", {})
    status = str(data.get("status", "")).lower()

    er_result = await db.execute(select(PayrollEntry).where(PayrollEntry.reference == tx_ref).with_for_update())
    entry = er_result.scalar_one_or_none()
    if not entry:
        return {"status": "ignored"}

    if status == "successful":
        entry.status = "completed"
        entry.provider_transaction_id = str(transaction_id) if transaction_id else None
    elif status in ("failed", "reversed"):
        entry.status = "failed"
        entry.error_msg = f"Transfer {status}: {data.get('complete_message', '')}"[:200]
    else:
        return {"status": "no_change"}

    run_result = await db.execute(select(PayrollRun).where(PayrollRun.id == entry.run_id))
    run = run_result.scalar_one_or_none()
    if run:
        if entry.status == "completed":
            run.paid_count = (run.paid_count or 0) + 1
        elif entry.status == "failed":
            run.failed_count = (run.failed_count or 0) + 1

        total_done = (run.paid_count or 0) + (run.failed_count or 0)
        if total_done >= run.entry_count:
            if run.failed_count == 0:
                run.status = "completed"
            elif run.paid_count > 0:
                run.status = "partial"
            else:
                run.status = "failed"
            run.completed_at = datetime.utcnow()

    await log_payment_event(db, event_type="payroll.transfer.updated", reference=tx_ref, transaction_id=transaction_id, status=entry.status, payload={"entry_id": entry.id, "employee": entry.employee_name})
    await db.commit()
    return {"status": entry.status}
