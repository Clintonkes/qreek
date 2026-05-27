from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

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

    await db.commit()
    return Response(status_code=200, content="OK")
