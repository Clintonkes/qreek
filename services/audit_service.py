import uuid
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)

async def log_audit(
    db: AsyncSession,
    *,
    event_type: str,
    user_phone: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
    detail: str | None = None,
) -> None:
    try:
        await db.execute(
            text(
                "INSERT INTO audit_log (id, user_phone, event_type, ip_address, user_agent, detail, created_at)"
                " VALUES (:id, :phone, :event, :ip, :ua, :detail, :now)"
            ),
            {
                "id": uuid.uuid4().hex,
                "phone": user_phone,
                "event": event_type,
                "ip": ip_address,
                "ua": (user_agent or "")[:255],
                "detail": detail,
                "now": datetime.utcnow(),
            },
        )
        await db.commit()
    except Exception as exc:
        logger.warning("audit_log.write_failed: event=%s error=%s", event_type, str(exc)[:200])
