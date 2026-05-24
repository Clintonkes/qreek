import base64
import hashlib
import hmac
import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

FLW_SECRET_KEY = os.getenv("FLW_SECRET_KEY")
FLW_SECRET_HASH = os.getenv("FLW_SECRET_HASH")
FLW_BASE_URL = os.getenv("FLW_BASE_URL", "https://api.flutterwave.com/v3")
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://qreekfinance.org")


class FlutterwaveConfigError(Exception):
    pass


def _headers() -> dict:
    if not FLW_SECRET_KEY:
        raise FlutterwaveConfigError("FLW_SECRET_KEY is not configured")
    return {
        "Authorization": f"Bearer {FLW_SECRET_KEY}",
        "Content-Type": "application/json",
    }


def _client():
    return httpx.AsyncClient(timeout=20.0)


async def initialize_checkout(
    *,
    tx_ref: str,
    amount: float,
    customer_name: str,
    customer_phone: Optional[str],
    redirect_url: Optional[str],
    title: str,
    description: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> dict:
    fallback_redirect = FRONTEND_URL
    if metadata and metadata.get("code"):
        fallback_redirect = f"{FRONTEND_URL}/p/{metadata['code']}"

    payload = {
        "tx_ref": tx_ref,
        "amount": amount,
        "currency": "NGN",
        "redirect_url": redirect_url or fallback_redirect,
        "customer": {
            "email": f"{customer_phone or tx_ref}@qreekfinance.org",
            "phonenumber": customer_phone,
            "name": customer_name,
        },
        "customizations": {
            "title": "QreekPay",
            "description": description or title,
        },
        "meta": metadata or {},
    }

    async with _client() as client:
        response = await client.post(f"{FLW_BASE_URL}/payments", headers=_headers(), json=payload)
        response.raise_for_status()
        data = response.json()

    link = data.get("data", {}).get("link")
    if not link:
        raise RuntimeError(f"Flutterwave checkout link missing: {data}")
    return data


async def verify_transaction(transaction_id: str | int) -> dict:
    async with _client() as client:
        response = await client.get(f"{FLW_BASE_URL}/transactions/{transaction_id}/verify", headers=_headers())
        response.raise_for_status()
        return response.json()


async def create_transfer(
    *,
    amount: float,
    bank_code: str,
    account_number: str,
    reference: str,
    narration: str = "Qreek Finance Payout",
    beneficiary_name: Optional[str] = None,
) -> dict:
    payload = {
        "account_bank": bank_code,
        "account_number": account_number,
        "amount": amount,
        "currency": "NGN",
        "reference": reference,
        "narration": narration,
    }
    if beneficiary_name:
        payload["beneficiary_name"] = beneficiary_name

    async with _client() as client:
        response = await client.post(f"{FLW_BASE_URL}/transfers", headers=_headers(), json=payload)
        response.raise_for_status()
        data = response.json()
    return {"provider": "flutterwave", **data}


def verify_webhook_hash(verif_hash: Optional[str]) -> bool:
    if not FLW_SECRET_HASH:
        logger.error("FLW_SECRET_HASH missing, cannot verify Flutterwave webhook.")
        return False
    if not verif_hash:
        return False
    return hmac.compare_digest(verif_hash, FLW_SECRET_HASH)


def verify_webhook_signature(raw_body: bytes, signature: Optional[str], legacy_hash: Optional[str] = None) -> bool:
    """
    Verifies Flutterwave webhooks.

    Current Flutterwave webhooks sign the raw request body with HMAC-SHA256
    and send it in the flutterwave-signature header. Older integrations send
    the dashboard secret directly as verif-hash, so we accept both during the
    migration window.
    """
    if not FLW_SECRET_HASH:
        logger.error("FLW_SECRET_HASH missing, cannot verify Flutterwave webhook.")
        return False

    if signature:
        digest = hmac.new(FLW_SECRET_HASH.encode(), raw_body, hashlib.sha256).digest()
        expected = base64.b64encode(digest).decode()
        if hmac.compare_digest(signature, expected):
            return True

    return verify_webhook_hash(legacy_hash)
