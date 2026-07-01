import asyncio, uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Company, Employee, PayrollEntry, PayrollRun, Transaction
from database.session import get_db
from core.payout import best_payout, settle_fee
from routers.web_payment_links import finalize_flutterwave_link_payment
from services.payment_event_logger import log_payment_event
import logging
from services.flutterwave_service import verify_webhook_signature, verify_transaction

logger = logging.getLogger(__name__)
from services.sms_service import send_sms as send_transfer_sms

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

    if tx_ref and str(tx_ref).startswith("QRK_PRCK_"):
        try:
            await finalize_payroll_checkout(db, tx_ref, transaction_id, payload)
        except Exception as exc:
            logger.exception("Could not finalize payroll checkout %s: %s", tx_ref, exc)
            await log_payment_event(db, event_type="flutterwave.webhook.payroll_checkout_failed", reference=tx_ref, transaction_id=transaction_id, status="failed", message=str(exc)[:1000])

    await db.commit()
    return Response(status_code=200, content="OK")


async def finalize_wallet_deposit(db: AsyncSession, tx_ref: str, transaction_id: str | int = None) -> dict:
    """
    Credits the company wallet when a QRK_WAL_ Flutterwave checkout succeeds.
    Verifies the transaction with Flutterwave before crediting to prevent replay attacks.
    """
    logger.info("wallet_deposit.finalize.start: ref=%s transaction_id=%s", tx_ref, transaction_id)
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
        logger.warning("wallet_deposit.verify.failed: ref=%s status=%s", tx_ref, flw_status)
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
    logger.info("wallet_deposit.completed: ref=%s company=%s amount=%.2f new_balance=%.2f", tx_ref, company_id, tx.amount, co.wallet_balance_ngn)

    await log_payment_event(db, event_type="wallet_deposit.completed", reference=tx_ref, transaction_id=transaction_id, status="completed", payload={"company_id": company_id, "amount": tx.amount, "new_balance": co.wallet_balance_ngn})
    await db.commit()
    return {"status": "completed", "amount": tx.amount, "new_balance": co.wallet_balance_ngn}


async def finalize_payroll_transfer(db: AsyncSession, tx_ref: str, transaction_id: str | int, payload: dict) -> dict:
    """
    Updates a single employee payout entry when Flutterwave confirms or fails a QRK_PR_ transfer.
    Handles run-level status rollup (completed/partial/failed) once all entries are resolved.
    """
    data = payload.get("data", {})
    status = str(data.get("status", "")).lower()
    logger.info("payroll.transfer.update: ref=%s status=%s", tx_ref, status)

    er_result = await db.execute(select(PayrollEntry).where(PayrollEntry.reference == tx_ref).with_for_update())
    entry = er_result.scalar_one_or_none()
    if not entry:
        return {"status": "ignored"}

    if status == "successful":
        entry.status = "completed"
        entry.provider_transaction_id = str(transaction_id) if transaction_id else None
        logger.info("payroll.transfer.confirmed: ref=%s employee=%r amount=%.2f", tx_ref, entry.employee_name, entry.gross_amount or 0)

        # SMS the employee the moment Flutterwave confirms the transfer
        try:
            emp_r = await db.execute(select(Employee).where(Employee.id == entry.employee_id))
            emp = emp_r.scalar_one_or_none()
            if emp and emp.phone:
                pr_r = await db.execute(select(PayrollRun).where(PayrollRun.id == entry.run_id))
                pr = pr_r.scalar_one_or_none()
                company_name = ""
                if pr:
                    co_r = await db.execute(select(Company).where(Company.id == pr.company_id))
                    co = co_r.scalar_one_or_none()
                    company_name = co.name if co else ""
                await send_transfer_sms(
                    phone=emp.phone,
                    message=f"Qreek: ₦{entry.gross_amount:,.0f} has been sent to your bank from {company_name or 'your employer'}. Ref: {entry.reference or tx_ref}.",
                    reference=entry.reference or tx_ref,
                    db=db,
                )
        except Exception:
            pass
    elif status in ("failed", "reversed"):
        entry.status = "failed"
        entry.error_msg = f"Transfer {status}: {data.get('complete_message', '')}"[:200]
        logger.warning("payroll.transfer.failed: ref=%s employee=%r status=%s reason=%s", tx_ref, entry.employee_name, status, data.get("complete_message", "")[:100])
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


async def finalize_payroll_checkout(db: AsyncSession, tx_ref: str, transaction_id: str | int, payload: dict) -> dict:
    """
    Called when a QRK_PRCK_ (payroll checkout) payment succeeds on Flutterwave.
    Fires all pending payouts for the associated payroll run.
    """
    data = payload.get("data", {})
    flw_status = str(data.get("status", "")).lower()

    if flw_status != "successful":
        return {"status": "ignored"}

    logger.info("payroll.checkout.finalize.start: ref=%s", tx_ref)
    # Find the transaction record
    tx_result = await db.execute(select(Transaction).where(Transaction.reference == tx_ref).with_for_update())
    tx = tx_result.scalar_one_or_none()
    if not tx or tx.status == "completed":
        return {"status": "already_completed"}

    # Verify with Flutterwave API
    verified = await verify_transaction(transaction_id)
    vdata = verified.get("data", {})
    vstatus = str(vdata.get("status", "")).lower()
    if vstatus != "successful":
        tx.status = "failed"
        await db.commit()
        logger.warning("Payroll checkout %s: Flutterwave status is %s", tx_ref, vstatus)
        return {"status": "failed"}

    company_id = None
    run_id = None
    if tx.event_metadata:
        company_id = tx.event_metadata.get("company_id")
        run_id = tx.event_metadata.get("run_id")

    if not run_id or not company_id:
        await log_payment_event(db, event_type="payroll_checkout.finalize.missing_metadata", reference=tx_ref, transaction_id=transaction_id, status="failed")
        tx.status = "failed"
        await db.commit()
        return {"status": "failed"}

    # Find the payroll run
    r_result = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == company_id).with_for_update())
    run = r_result.scalar_one_or_none()
    if not run:
        tx.status = "failed"
        await db.commit()
        logger.warning("Payroll checkout %s: run %s not found", tx_ref, run_id)
        return {"status": "failed"}

    if run.status not in ("pending",):
        await log_payment_event(db, event_type="payroll_checkout.finalize.already_processing", reference=tx_ref, transaction_id=transaction_id, status="ignored")
        return {"status": "already_processing"}

    # Find the company
    co_result = await db.execute(select(Company).where(Company.id == company_id))
    co = co_result.scalar_one_or_none()

    # Mark transaction as completed
    tx.status = "completed"
    tx.provider_transaction_id = str(transaction_id)

    # Mark run as processing
    run.status = "processing"
    await db.commit()

    # Fire all pending payouts
    er_result = await db.execute(select(PayrollEntry).where(PayrollEntry.run_id == run_id, PayrollEntry.status == "pending"))
    entries = er_result.scalars().all()

    # Audit log
    from routers.web_payroll import _log
    await _log(db, tx.user_phone, "payroll_run_executed", "payroll_run", run.id, run.total_gross, None,
               {"company": co.name if co else "", "period": run.period_label, "count": len(entries)})
    await db.commit()

    async def _fire_all():
        async with __import__("database.session", fromlist=["AsyncSessionLocal"]).AsyncSessionLocal() as sess:
            for entry in entries:
                bank = {"account_number": entry.bank_account, "bank_code": entry.bank_code}
                ref  = "QRK_PR_" + uuid.uuid4().hex[:10].upper()
                try:
                    logger.info("payroll.checkout.payout.start: run=%s ref=%s employee=%r amount=%.2f", run.id, ref, entry.employee_name, entry.gross_amount)
                    result = await best_payout(tx.user_phone, entry.gross_amount, bank, ref)
                    entry.status   = "completed"
                    entry.provider = result.get("provider")
                    entry.reference = ref
                    entry.paid_at  = datetime.utcnow()
                    entry.qreek_fee    = 0.0
                    entry.provider_fee = 0.0
                    run.paid_count = (run.paid_count or 0) + 1
                    logger.info("payroll.checkout.payout.ok: run=%s ref=%s employee=%r provider=%s", run.id, ref, entry.employee_name, result.get("provider"))

                    # SMS notification to employee when salary lands
                    try:
                        emp_r = await sess.execute(select(Employee).where(Employee.id == entry.employee_id))
                        emp = emp_r.scalar_one_or_none()
                        if emp and emp.phone:
                            await send_transfer_sms(
                                phone=emp.phone,
                                message=f"Qreek: ₦{entry.gross_amount:,.0f} salary for {run.period_label} from {(co.name if co else 'your employer')} has been sent to your bank. Ref: {ref}.",
                                reference=ref,
                                db=sess,
                            )
                    except Exception:
                        pass
                except Exception as e:
                    logger.warning("payroll.checkout.payout.fail: run=%s ref=%s employee=%r error=%s", run.id, ref, entry.employee_name, str(e)[:200])
                    entry.status    = "failed"
                    entry.error_msg = str(e)[:200]
                    entry.qreek_fee    = 0.0
                    entry.provider_fee = 0.0
                    run.failed_count = (run.failed_count or 0) + 1

                # Record transaction
                txx = Transaction(
                    user_phone=tx.user_phone, tx_type="payroll",
                    currency="NGN", amount=entry.gross_amount,
                    ngn_amount=entry.gross_amount, gross_amount=entry.gross_amount,
                    qreek_fee=0.0, provider_fee=0.0,
                    net_amount=entry.gross_amount, status=entry.status,
                    provider=entry.provider, reference=entry.reference,
                    payment_description=f"Payroll {run.period_label} — {entry.employee_name}",
                )
                sess.add(txx)
                sess.add(entry)
                await sess.flush()

            # Settle Qreek's fee for the entire run once after all employee payouts.
            run_fee = run.total_fee or 0
            if run_fee > 0:
                fee_ref = "QRK_PRF_" + uuid.uuid4().hex[:8].upper()
                logger.info("payroll.checkout.fee.settle: run=%s fee_ref=%s amount=%.2f", run.id, fee_ref, run_fee)
                try:
                    await settle_fee(tx.user_phone, run_fee, fee_ref)
                except Exception as fee_err:
                    # Employees are paid — log and reconcile separately if this fails.
                    logger.warning("payroll.checkout.fee.settle.fail: run=%s fee_ref=%s error=%s", run.id, fee_ref, str(fee_err)[:200])

            if run.failed_count and run.paid_count:
                run.status = "partial"
            elif run.failed_count == run.entry_count:
                run.status = "failed"
            else:
                run.status = "completed"
            run.completed_at = datetime.utcnow()

            co_r = await sess.execute(select(Company).where(Company.id == company_id))
            co2  = co_r.scalar_one_or_none()
            if co2:
                co2.total_paid_ngn = (co2.total_paid_ngn or 0) + run.total_net

            sess.add(run)
            await sess.commit()

    asyncio.create_task(_fire_all())

    await log_payment_event(db, event_type="payroll_checkout.completed", reference=tx_ref, transaction_id=transaction_id, status="completed",
                            payload={"run_id": run.id, "amount": run.total_gross, "entry_count": len(entries)})
    await db.commit()
    return {"status": "processing", "run_id": run.id}
