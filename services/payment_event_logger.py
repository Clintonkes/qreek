import logging
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from database.models import PaymentEvent

logger = logging.getLogger(__name__)


def _trim(value: Any, limit: int = 1800) -> Any:
    if isinstance(value, dict):
        return {k: _trim(v, limit) for k, v in value.items()}
    if isinstance(value, list):
        return [_trim(v, limit) for v in value[:25]]
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "...[truncated]"
    return value


async def log_payment_event(
    db: AsyncSession,
    *,
    event_type: str,
    reference: Optional[str] = None,
    transaction_id: Optional[str] = None,
    status: Optional[str] = None,
    message: Optional[str] = None,
    payload: Optional[dict] = None,
    provider: str = "flutterwave",
) -> None:
    """
    Stores a durable payment event and mirrors it to application logs.
    It never raises, so logging cannot break checkout or webhook processing.
    """
    safe_payload = _trim(payload or {})
    logger.info(
        "payment_event provider=%s type=%s reference=%s transaction_id=%s status=%s message=%s payload=%s",
        provider,
        event_type,
        reference,
        transaction_id,
        status,
        message,
        safe_payload,
    )
    try:
        db.add(PaymentEvent(
            provider=provider,
            reference=reference,
            transaction_id=str(transaction_id) if transaction_id is not None else None,
            event_type=event_type,
            status=status,
            message=message,
            payload=safe_payload,
        ))
        await db.flush()
    except Exception:
        logger.exception("Could not store payment event %s for %s", event_type, reference)
