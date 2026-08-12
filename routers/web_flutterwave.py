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
        await db.commit()
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
    tx_result = await db.execute(select(Transaction).where(Transaction.reference == tx_ref).with_for_update())
    tx = tx_result.scalar_one_or_none()
    if not entry and not tx:
        return {"status": "ignored"}
    prior_entry_status = entry.status if entry else None

    if status == "successful":
        if entry and entry.payout_status == "settled":
            return {"status": "already_settled"}
        if tx and tx.payout_status == "settled":
            return {"status": "already_settled"}

        if entry:
            entry.status = "completed"
            entry.payout_status = "settled"
            entry.payout_reference = tx_ref
            entry.provider_transaction_id = str(transaction_id) if transaction_id else entry.provider_transaction_id
            entry.payout_error = None
        if tx:
            tx.status = "completed"
            tx.payout_status = "settled"
            tx.payout_reference = tx_ref
            tx.provider_transaction_id = str(transaction_id) if transaction_id else tx.provider_transaction_id
            tx.payout_error = None

        amount = entry.gross_amount if entry else (tx.amount if tx else 0)
        employee_name = entry.employee_name if entry else (tx.payment_description or tx_ref)
        logger.info("payroll.transfer.confirmed: ref=%s employee=%r amount=%.2f", tx_ref, employee_name, amount or 0)

        # SMS the employee the moment Flutterwave confirms the transfer
        if entry:
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
        if entry and entry.payout_status == "failed":
            return {"status": "already_failed"}

        if entry:
            entry.status = "failed"
            entry.payout_status = "failed"
            entry.payout_reference = tx_ref
            entry.error_msg = f"Transfer {status}: {data.get('complete_message', '')}"[:200]
            entry.payout_error = entry.error_msg
            logger.warning("payroll.transfer.failed: ref=%s employee=%r status=%s reason=%s", tx_ref, entry.employee_name, status, data.get("complete_message", "")[:100])
        if tx:
            tx.status = "failed"
            tx.payout_status = "failed"
            tx.payout_reference = tx_ref
            tx.payout_error = f"Transfer {status}: {data.get('complete_message', '')}"[:200]
    else:
        return {"status": "no_change"}

    run = None
    if entry:
        run_result = await db.execute(select(PayrollRun).where(PayrollRun.id == entry.run_id))
        run = run_result.scalar_one_or_none()
    if run:
        if entry and prior_entry_status == "failed" and entry.status == "completed":
            run.failed_count = max(0, (run.failed_count or 1) - 1)
        if entry and prior_entry_status != "completed" and entry.status == "completed":
            run.paid_count = (run.paid_count or 0) + 1
        elif entry and prior_entry_status != "failed" and entry.status == "failed":
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

    event_status = entry.status if entry else (tx.status if tx else status)
    await log_payment_event(
        db,
        event_type="payroll.transfer.updated",
        reference=tx_ref,
        transaction_id=transaction_id,
        status=event_status,
        payload={"entry_id": entry.id if entry else None, "employee": entry.employee_name if entry else None},
    )
    if status == "successful":
        logger.info(
            "payroll.checkout.payout.settled: ref=%s transaction_id=%s status=%s",
            tx_ref,
            transaction_id,
            event_status,
        )
        await log_payment_event(
            db,
            event_type="payroll_checkout.payout.settled",
            reference=tx_ref,
            transaction_id=transaction_id,
            status=event_status,
            payload={"entry_id": entry.id if entry else None, "amount": amount},
        )
    elif status in ("failed", "reversed"):
        await log_payment_event(
            db,
            event_type="payroll_checkout.payout.failed",
            reference=tx_ref,
            transaction_id=transaction_id,
            status=event_status,
            payload={"entry_id": entry.id if entry else None, "reason": data.get("complete_message", "")},
        )
    await db.commit()
    return {"status": event_status}


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
    tx_result = await db.execute(select(Transaction).where(Transaction.reference == tx_ref).with_for_update())
    tx = tx_result.scalar_one_or_none()
    if not tx or tx.status == "completed":
        return {"status": "already_completed"}

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

    co_result = await db.execute(select(Company).where(Company.id == company_id))
    co = co_result.scalar_one_or_none()

    # Capture all primitives NOW — before any commit expires these ORM objects.
    # _fire_all() runs as a background task after db.commit(), so any attribute
    # access on run/tx/co/entries after that point raises DetachedInstanceError.
    user_phone    = tx.user_phone
    period_label  = run.period_label
    total_gross   = run.total_gross
    total_net     = run.total_net
    total_fee     = run.total_fee or 0
    co_name       = co.name if co else ""
    entry_count   = run.entry_count

    tx.status = "completed"
    tx.provider_transaction_id = str(transaction_id)
    run.status = "processing"
    await db.commit()

    from routers.web_payroll import _log
    await _log(db, user_phone, "payroll_run_executed", "payroll_run", run_id, total_gross, None,
               {"company": co_name, "period": period_label, "count": entry_count})
    await db.commit()

    async def _fire_all():
        from database.session import AsyncSessionLocal
        async with AsyncSessionLocal() as sess:
            # Load run and entries fresh — the outer session's objects are expired
            # after db.commit(), so we must never access them inside this task.
            run_r = await sess.execute(select(PayrollRun).where(PayrollRun.id == run_id).with_for_update())
            run2 = run_r.scalar_one_or_none()
            if not run2:
                logger.warning("payroll.checkout.fire_all.run_missing: run=%s", run_id)
                return

            er_result = await sess.execute(select(PayrollEntry).where(
                PayrollEntry.run_id == run_id,
                PayrollEntry.status == "pending",
            ))
            fresh_entries = er_result.scalars().all()

            if not fresh_entries:
                logger.info("payroll.checkout.fire_all.no_pending: run=%s", run_id)
                return

            for entry in fresh_entries:
                bank = {"account_number": entry.bank_account, "bank_code": entry.bank_code}
                ref  = "QRK_PR_" + uuid.uuid4().hex[:10].upper()
                try:
                    logger.info("payroll.checkout.payout.start: run=%s ref=%s employee=%r amount=%.2f", run_id, ref, entry.employee_name, entry.gross_amount)
                    result = await best_payout(user_phone, entry.gross_amount, bank, ref)
                    provider_data = result.get("data", {}) if isinstance(result, dict) else {}
                    provider_tx_id = provider_data.get("id") or provider_data.get("transfer_id") or result.get("id") if isinstance(result, dict) else None
                    entry.status              = "processing"
                    entry.provider            = result.get("provider")
                    entry.reference           = ref
                    entry.provider_transaction_id = str(provider_tx_id) if provider_tx_id else None
                    entry.payout_status       = "submitted"
                    entry.payout_reference    = ref
                    entry.payout_error        = None
                    entry.paid_at             = datetime.utcnow()
                    entry.qreek_fee           = 0.0
                    entry.provider_fee        = 0.0
                    logger.info("payroll.checkout.payout.ok: run=%s ref=%s employee=%r provider=%s", run_id, ref, entry.employee_name, result.get("provider"))

                    try:
                        emp_r = await sess.execute(select(Employee).where(Employee.id == entry.employee_id))
                        emp = emp_r.scalar_one_or_none()
                        if emp and emp.phone:
                            await send_transfer_sms(
                                phone=emp.phone,
                                message=f"Qreek: ₦{entry.gross_amount:,.0f} salary for {period_label} from {co_name or 'your employer'} has been sent to your bank. Ref: {ref}.",
                                reference=ref,
                                db=sess,
                            )
                    except Exception:
                        pass
                except Exception as e:
                    logger.warning("payroll.checkout.payout.fail: run=%s ref=%s employee=%r error=%s", run_id, ref, entry.employee_name, str(e)[:200])
                    entry.status           = "failed"
                    entry.payout_status    = "failed"
                    entry.payout_reference = ref
                    entry.payout_error     = str(e)[:500]
                    entry.error_msg        = str(e)[:200]
                    entry.qreek_fee        = 0.0
                    entry.provider_fee     = 0.0
                    run2.failed_count = (run2.failed_count or 0) + 1

                txx = Transaction(
                    user_phone=user_phone, tx_type="payroll",
                    currency="NGN", amount=entry.gross_amount,
                    ngn_amount=entry.gross_amount, gross_amount=entry.gross_amount,
                    qreek_fee=0.0, provider_fee=0.0,
                    net_amount=entry.gross_amount, status=entry.status,
                    provider=entry.provider, reference=entry.reference,
                    payment_description=f"Payroll {period_label} — {entry.employee_name}",
                )
                sess.add(txx)
                await sess.flush()

            if total_fee > 0:
                fee_ref = "QRK_PRF_" + uuid.uuid4().hex[:8].upper()
                logger.info("payroll.checkout.fee.settle: run=%s fee_ref=%s amount=%.2f", run_id, fee_ref, total_fee)
                try:
                    await settle_fee(user_phone, total_fee, fee_ref)
                except Exception as fee_err:
                    logger.warning("payroll.checkout.fee.settle.fail: run=%s fee_ref=%s error=%s", run_id, fee_ref, str(fee_err)[:200])

            any_processing = any(e.status == "processing" for e in fresh_entries)
            any_failed     = any(e.status == "failed"     for e in fresh_entries)
            if any_processing:
                run2.status = "processing"
                run2.completed_at = None
            elif any_failed:
                run2.status = "failed" if all(e.status == "failed" for e in fresh_entries) else "partial"
                run2.completed_at = datetime.utcnow()
            else:
                run2.status = "completed"
                run2.completed_at = datetime.utcnow()

            co_r = await sess.execute(select(Company).where(Company.id == company_id))
            co2  = co_r.scalar_one_or_none()
            if co2:
                co2.total_paid_ngn = (co2.total_paid_ngn or 0) + total_net

            await sess.commit()

    asyncio.create_task(_fire_all())

    await log_payment_event(db, event_type="payroll_checkout.completed", reference=tx_ref, transaction_id=transaction_id, status="completed",
                            payload={"run_id": run_id, "amount": total_gross, "entry_count": entry_count})
    logger.info("payroll.checkout.completed: ref=%s run=%s amount=%.2f entry_count=%s", tx_ref, run_id, total_gross, entry_count)
    await db.commit()
    return {"status": "processing", "run_id": run_id}
