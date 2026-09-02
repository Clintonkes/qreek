"""
SMS notification service for realtime alerts and receipts on payment events.

Used for link payments: notify the link owner (creator) the moment a payment lands,
and optionally send a confirmation/receipt to the payer's phone.

Provider: switchable via SMS_PROVIDER, since Nigerian SMS providers can sit in
business-verification limbo for days/weeks (or have their own dashboard/OTP outages)
and we don't want that to block sends.
  - "bulksmsnigeria" (default) - https://bulksmsnigeria.com - account is instant (email
    verification only); KYC is deferred to higher limits, not gating a first send. Picked
    after Termii, Sendchamp, KudiSMS, and Africa's Talking all stalled on account/sender-ID
    verification. NOTE: a *dedicated* branded sender ID still needs the same CAC-document
    review as every other provider (that's an NCC rule, not a vendor choice) - this just
    lets sends go out on a generic sender in the meantime.
  - "africastalking" - https://africastalking.com - pan-African, larger/more established infra.
  - "kudisms"   - https://kudisms.net  - cheapest verified option; parked after its own
    signup OTP repeatedly failed to verify.
  - "sendchamp" - https://sendchamp.com - parked after its dashboard login broke.
  - "termii"    - https://termii.com    - original provider; switch back if approved.
Falls back to structured logging + payment_event if no API key configured (for dev/CI).

Environment:
  SMS_PROVIDER              - "bulksmsnigeria" (default), "africastalking", "kudisms", "sendchamp", or "termii"
  BULKSMSNIGERIA_API_TOKEN   - required for live sends via BulkSMSNigeria
  BULKSMSNIGERIA_SENDER_ID   - optional, defaults to "QreekPay" (generic until a dedicated ID is approved)
  AFRICASTALKING_USERNAME    - required for live sends via Africa's Talking (your dashboard username)
  AFRICASTALKING_API_KEY     - required for live sends via Africa's Talking
  AFRICASTALKING_SENDER_ID   - optional; only if you have an approved alphanumeric sender ID/shortcode
  KUDISMS_API_KEY            - required for live sends via KudiSMS
  KUDISMS_SENDER_ID          - required for live sends via KudiSMS (an approved sender ID)
  KUDISMS_GATEWAY            - optional, only needed if KudiSMS assigns you a specific gateway id
  SENDCHAMP_API_KEY          - required for live sends via Sendchamp
  SENDCHAMP_SENDER_NAME      - optional, defaults to "Sendchamp" (must be an approved sender ID otherwise)
  TERMII_API_KEY             - required for live sends via Termii
  TERMII_SENDER_ID           - optional, defaults to "QreekPay" (must be approved sender or shortcode)

All sends are best-effort (fire and forget, never block payout or checkout).
Every attempt (success/skip/fail) is logged via payment_event for audit.
"""
import logging
import os
from typing import Optional

from services.payment_event_logger import log_payment_event

logger = logging.getLogger(__name__)

SMS_PROVIDER = os.getenv("SMS_PROVIDER", "bulksmsnigeria").strip().lower()

BULKSMSNIGERIA_API_TOKEN = os.getenv("BULKSMSNIGERIA_API_TOKEN")
BULKSMSNIGERIA_SENDER_ID = os.getenv("BULKSMSNIGERIA_SENDER_ID", "QreekPay")

TERMII_API_KEY = os.getenv("TERMII_API_KEY")
TERMII_SENDER_ID = os.getenv("TERMII_SENDER_ID", "QreekPay")

SENDCHAMP_API_KEY = os.getenv("SENDCHAMP_API_KEY")
SENDCHAMP_SENDER_NAME = os.getenv("SENDCHAMP_SENDER_NAME", "Sendchamp")

KUDISMS_API_KEY = os.getenv("KUDISMS_API_KEY")
KUDISMS_SENDER_ID = os.getenv("KUDISMS_SENDER_ID", "QreekPay")
KUDISMS_GATEWAY = os.getenv("KUDISMS_GATEWAY")  # optional; KudiSMS treats this as nullable

AFRICASTALKING_USERNAME = os.getenv("AFRICASTALKING_USERNAME")
AFRICASTALKING_API_KEY = os.getenv("AFRICASTALKING_API_KEY")
AFRICASTALKING_SENDER_ID = os.getenv("AFRICASTALKING_SENDER_ID")  # optional; omit to send from a shared AT number


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
    Send SMS via the configured provider (SMS_PROVIDER; or log-only if no key).
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

    if SMS_PROVIDER == "termii":
        return await _send_via_termii(norm_phone, message, reference, db)
    if SMS_PROVIDER == "sendchamp":
        return await _send_via_sendchamp(norm_phone, message, reference, db)
    if SMS_PROVIDER == "kudisms":
        return await _send_via_kudisms(norm_phone, message, reference, db)
    if SMS_PROVIDER == "africastalking":
        return await _send_via_africastalking(norm_phone, message, reference, db)
    return await _send_via_bulksmsnigeria(norm_phone, message, reference, db)


async def _send_via_bulksmsnigeria(
    norm_phone: str,
    message: str,
    reference: Optional[str],
    db: Optional["AsyncSession"],  # type: ignore
) -> bool:
    if not BULKSMSNIGERIA_API_TOKEN:
        logger.info(f"SMS[SKIPPED no BULKSMSNIGERIA_API_TOKEN] to={norm_phone} ref={reference} msg={message[:80]}")
        if db:
            await log_payment_event(
                db,
                event_type="sms.send.skipped.no_key",
                reference=reference,
                status="skipped",
                message="BULKSMSNIGERIA_API_TOKEN not set; SMS logged only",
                payload={"to": norm_phone, "message": message},
            )
        return False

    # Lazy import so bare python / tests without the project venv can still import the module for syntax.
    import httpx
    payload = {
        "from": BULKSMSNIGERIA_SENDER_ID[:11],
        "to": norm_phone.lstrip("+"),
        "body": message[:1530],
        "gateway": "direct-corporate",  # trying this after "otp" was silently ignored (gateway_used kept coming back "direct-refund")
    }
    headers = {"Authorization": f"Bearer {BULKSMSNIGERIA_API_TOKEN}"}

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.post("https://www.bulksmsnigeria.com/api/v2/sms", json=payload, headers=headers)
            data = {}
            try:
                data = resp.json()
            except Exception:
                pass
            success = resp.status_code == 200 and str(data.get("status", "")).lower() == "success"

            if db:
                await log_payment_event(
                    db,
                    event_type="sms.send.attempted",
                    reference=reference,
                    status="success" if success else "failed",
                    message=str(data)[:500] if not success else None,
                    payload={
                        "to": norm_phone,
                        "sender": BULKSMSNIGERIA_SENDER_ID,
                        "provider": "bulksmsnigeria",
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
                payload={"to": norm_phone, "provider": "bulksmsnigeria"},
            )
        return False


async def _send_via_africastalking(
    norm_phone: str,
    message: str,
    reference: Optional[str],
    db: Optional["AsyncSession"],  # type: ignore
) -> bool:
    if not AFRICASTALKING_USERNAME or not AFRICASTALKING_API_KEY:
        logger.info(f"SMS[SKIPPED no AFRICASTALKING_API_KEY] to={norm_phone} ref={reference} msg={message[:80]}")
        if db:
            await log_payment_event(
                db,
                event_type="sms.send.skipped.no_key",
                reference=reference,
                status="skipped",
                message="AFRICASTALKING_USERNAME/API_KEY not set; SMS logged only",
                payload={"to": norm_phone, "message": message},
            )
        return False

    # Lazy import so bare python / tests without the project venv can still import the module for syntax.
    import httpx
    data = {
        "username": AFRICASTALKING_USERNAME,
        "to": norm_phone,  # AT wants E.164 with "+"
        "message": message[:320],
        "bulkSMSMode": 1,
    }
    if AFRICASTALKING_SENDER_ID:
        data["from"] = AFRICASTALKING_SENDER_ID
    headers = {"Accept": "application/json", "apiKey": AFRICASTALKING_API_KEY}

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.post("https://api.africastalking.com/version1/messaging", data=data, headers=headers)
            payload = {}
            try:
                payload = resp.json()
            except Exception:
                pass
            recipients = (payload.get("SMSMessageData") or {}).get("Recipients") or []
            success = resp.status_code == 200 and bool(recipients) and str(recipients[0].get("status", "")).lower() == "success"

            if db:
                await log_payment_event(
                    db,
                    event_type="sms.send.attempted",
                    reference=reference,
                    status="success" if success else "failed",
                    message=str(payload)[:500] if not success else None,
                    payload={
                        "to": norm_phone,
                        "sender": AFRICASTALKING_SENDER_ID,
                        "provider": "africastalking",
                        "http_status": resp.status_code,
                        "provider_response": payload or resp.text[:300],
                    },
                )

            if success:
                logger.info("SMS sent successfully to %s ref=%s", norm_phone, reference)
                return True
            else:
                logger.warning("SMS send failed to %s: %s %s", norm_phone, resp.status_code, str(payload)[:200])
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
                payload={"to": norm_phone, "provider": "africastalking"},
            )
        return False


async def _send_via_kudisms(
    norm_phone: str,
    message: str,
    reference: Optional[str],
    db: Optional["AsyncSession"],  # type: ignore
) -> bool:
    if not KUDISMS_API_KEY:
        logger.info(f"SMS[SKIPPED no KUDISMS_API_KEY] to={norm_phone} ref={reference} msg={message[:80]}")
        if db:
            await log_payment_event(
                db,
                event_type="sms.send.skipped.no_key",
                reference=reference,
                status="skipped",
                message="KUDISMS_API_KEY not set; SMS logged only",
                payload={"to": norm_phone, "message": message},
            )
        return False

    # Lazy import so bare python / tests without the project venv can still import the module for syntax.
    import httpx
    params = {
        "token": KUDISMS_API_KEY,
        "senderID": KUDISMS_SENDER_ID,
        "message": message[:320],
        "recipient": norm_phone.lstrip("+"),
    }
    if KUDISMS_GATEWAY:
        params["gateway"] = KUDISMS_GATEWAY

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            # "corporate" is KudiSMS's transactional/OTP endpoint - the DND-safe route,
            # matching the "Corporate SMS" pricing tier rather than the bulk/promotional one.
            resp = await client.post("https://my.kudisms.net/api/corporate", params=params)
            data = {}
            try:
                data = resp.json()
            except Exception:
                pass
            success = resp.status_code == 200 and str(data.get("status", "")).lower() == "success" and str(data.get("error_code", "")) in ("0", "000")

            if db:
                await log_payment_event(
                    db,
                    event_type="sms.send.attempted",
                    reference=reference,
                    status="success" if success else "failed",
                    message=str(data)[:500] if not success else None,
                    payload={
                        "to": norm_phone,
                        "sender": KUDISMS_SENDER_ID,
                        "provider": "kudisms",
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
                payload={"to": norm_phone, "provider": "kudisms"},
            )
        return False


async def _send_via_sendchamp(
    norm_phone: str,
    message: str,
    reference: Optional[str],
    db: Optional["AsyncSession"],  # type: ignore
) -> bool:
    if not SENDCHAMP_API_KEY:
        logger.info(f"SMS[SKIPPED no SENDCHAMP_API_KEY] to={norm_phone} ref={reference} msg={message[:80]}")
        if db:
            await log_payment_event(
                db,
                event_type="sms.send.skipped.no_key",
                reference=reference,
                status="skipped",
                message="SENDCHAMP_API_KEY not set; SMS logged only",
                payload={"to": norm_phone, "message": message},
            )
        return False

    # Lazy import so bare python / tests without the project venv can still import the module for syntax.
    import httpx
    payload = {
        "to": [norm_phone.lstrip("+")],  # Sendchamp expects international format without "+"
        "message": message[:320],
        "sender_name": SENDCHAMP_SENDER_NAME,
        "route": "dnd",  # transactional (OTP/receipt) traffic must reach DND-registered numbers
    }
    headers = {"Authorization": f"Bearer {SENDCHAMP_API_KEY}", "Content-Type": "application/json"}

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.post("https://api.sendchamp.com/api/v1/sms/send", json=payload, headers=headers)
            data = {}
            try:
                data = resp.json()
            except Exception:
                pass
            success = resp.status_code == 200 and str(data.get("status", "")).lower() == "success"

            if db:
                await log_payment_event(
                    db,
                    event_type="sms.send.attempted",
                    reference=reference,
                    status="success" if success else "failed",
                    message=str(data)[:500] if not success else None,
                    payload={
                        "to": norm_phone,
                        "sender": SENDCHAMP_SENDER_NAME,
                        "provider": "sendchamp",
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
                payload={"to": norm_phone, "provider": "sendchamp"},
            )
        return False


async def _send_via_termii(
    norm_phone: str,
    message: str,
    reference: Optional[str],
    db: Optional["AsyncSession"],  # type: ignore
) -> bool:
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
                        "provider": "termii",
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
                payload={"to": norm_phone, "provider": "termii"},
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
