"""
SMS notification service for realtime alerts and receipts on payment events.

Used for link payments: notify the link owner (creator) the moment a payment lands,
and optionally send a confirmation/receipt to the payer's phone.

Provider: Termii (https://termii.com) - reliable for Nigerian phone numbers, fast delivery.
Falls back to structured logging + payment_event if no API key configured (for dev/CI).

Environment:
  TERMII_API_KEY   - required for live sends (get from Termii dashboard)
  TERMII_SENDER_ID - optional, defaults to "QreekPay" (must be approved sender or shortcode)

All sends are best-effort (fire and forget, never block payout or checkout).
Every attempt (success/skip/fail) is logged via payment_event for audit.
"""
import logging
import os
from typing import Optional

from services.payment_event_logger import log_payment_event

logger = logging.getLogger(__name__)

TERMII_API_KEY = os.getenv("TERMII_API_KEY")
TERMII_SENDER_ID = os.getenv("TERMII_SENDER_ID", "QreekPay")


def _normalize_phone(phone: Optional[str]) -> Optional[str]:
    """Ensure phone is in a Termii-friendly format (E.164 preferred, or 234... or 0...)."""
    if not phone:
        return None
    p = str(phone).strip()
    if p.startswith("+"):
        return p
    if p.startswith("234"):
        return "+" + p
    if p.startswith("0") and len(p) >= 10:
        return "+234" + p[1:]
    # assume already international without +
    if len(p) > 8:
        return "+" + p
    return p


async def send_sms(
    phone: str,
    message: str,
    reference: Optional[str] = None,
    db: Optional["AsyncSession"] = None,  # type: ignore
) -> bool:
    """
    Send SMS via Termii (or log-only if no key).
    Always records a payment_event for the attempt (success, skipped.no_key, failed).
    Never raises; safe to call from finalize paths.
    """
    norm_phone = _normalize_phone(phone)
    if not norm_phone:
        if db:
            await log_payment_event(
                db, event_type="sms.send.skipped.invalid_phone", reference=reference,
                status="skipped", message="no valid phone", payload={"raw_phone": phone}
            )
        return False

    if not TERMII_API_KEY:
        # Dev / not configured: still "succeed" the intent for event log, but no real send.
        log_line = f"SMS[SKIPPED no TERMII_API_KEY] to={norm_phone} ref={reference} msg={message[:80]}"
        logger.info(log_line)
        if db:
            await log_payment_event(
                db,
                event_type="sms.send.skipped.no_key",
                reference=reference,
                status="skipped",
                message="TERMII_API_KEY not set; SMS logged only",
                payload={"to": norm_phone, "message": message},
            )
        return False

    # Lazy import so bare python / tests without the project venv can still import the module for syntax.
    import httpx
    payload = {
        "api_key": TERMII_API_KEY,
        "to": norm_phone,
        "from": TERMII_SENDER_ID[:11],  # Termii limit
        "sms": message[:320],  # keep reasonable length
        "type": "plain",
        "channel": "generic",
    }

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.post("https://api.ng.termii.com/api/sms/send", json=payload)
            ok = resp.status_code == 200
            data = {}
            try:
                data = resp.json()
            except Exception:
                pass
            success = ok and (data.get("message", "").lower().startswith("success") or "sent" in str(data).lower())

            if db:
                await log_payment_event(
                    db,
                    event_type="sms.send.attempted",
                    reference=reference,
                    status="success" if success else "failed",
                    message=str(data)[:500] if not success else None,
                    payload={
                        "to": norm_phone,
                        "sender": TERMII_SENDER_ID,
                        "http_status": resp.status_code,
                        "provider_response": data or resp.text[:300],
                    },
                )

            if success:
                logger.info("SMS sent successfully to %s ref=%s", norm_phone, reference)
                return True
            else:
                logger.warning("SMS send failed to %s: %s %s", norm_phone, resp.status_code, str(data)[:200])
                return False
    except Exception as exc:
        logger.exception("SMS transport error to %s: %s", norm_phone, exc)
        if db:
            await log_payment_event(
                db,
                event_type="sms.send.failed",
                reference=reference,
                status="failed",
                message=str(exc)[:500],
                payload={"to": norm_phone},
            )
        return False


async def send_link_payment_received_sms(
    owner_phone: str,
    link_title: str,
    amount: float,
    reference: str,
    payer_name: Optional[str] = None,
    db: Optional["AsyncSession"] = None,
) -> bool:
    """Notify the link creator that money arrived (realtime to their phone)."""
    payer = (payer_name or "Someone").strip()
    msg = f"Qreek: {payer} paid ₦{amount:,.0f} via your link '{link_title[:30]}'. Ref: {reference}. Check your dashboard for details."
    return await send_sms(owner_phone, msg, reference=reference, db=db)


async def send_payment_receipt_sms(
    payer_phone: str,
    link_title: str,
    amount: float,
    reference: str,
    owner_bank_name: Optional[str] = None,
    db: Optional["AsyncSession"] = None,
) -> bool:
    """Send confirmation to the person who just paid (receipt + settlement note)."""
    bank_hint = f" to {owner_bank_name}" if owner_bank_name else ""
    msg = f"Thank you! Your ₦{amount:,.0f} payment for '{link_title[:30]}' via Qreek was received. Ref: {reference}. Funds will settle{bank_hint} shortly."
    return await send_sms(payer_phone, msg, reference=reference, db=db)
