import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
from typing import Optional
from urllib.parse import urlencode

import httpx

logger = logging.getLogger(__name__)

FLW_SECRET_KEY = os.getenv("FLW_SECRET_KEY")
FLW_SECRET_HASH = os.getenv("FLW_SECRET_HASH")
FLW_BASE_URL = os.getenv("FLW_BASE_URL", "https://api.flutterwave.com/v3")
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://qreekfinance.org")


class FlutterwaveConfigError(Exception):
    pass


class FlutterwaveAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, response_text: str | None = None, payload: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text
        self.payload = payload or {}

    def as_payload(self) -> dict:
        return {
            "status_code": self.status_code,
            "response_text": self.response_text,
            "request_payload": self.payload,
        }


def _headers() -> dict:
    if not FLW_SECRET_KEY:
        raise FlutterwaveConfigError("FLW_SECRET_KEY is not configured")
    return {
        "Authorization": f"Bearer {FLW_SECRET_KEY}",
        "Content-Type": "application/json",
    }


def _client():
    return httpx.AsyncClient(timeout=20.0)


async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict,
    json_body: dict,
    retries: int = 3,
    backoff: float = 1.0,
) -> httpx.Response:
    """
    POST with exponential backoff for transient 5xx errors (e.g. Flutterwave
    returning a 502 Cloudflare Bad Gateway during brief upstream outages).
    Only 5xx responses are retried; 4xx errors are returned immediately since
    they represent caller/config problems, not transient infrastructure issues.
    """
    last_response = None
    for attempt in range(retries):
        response = await client.post(url, headers=headers, json=json_body)
        if response.status_code < 500:          # 2xx, 3xx, 4xx — do not retry
            return response
        last_response = response
        if attempt < retries - 1:
            wait = backoff * (2 ** attempt)     # 1s, 2s, ...
            logger.warning(
                "Flutterwave returned %s on attempt %d/%d — retrying in %.1fs",
                response.status_code, attempt + 1, retries, wait,
            )
            await asyncio.sleep(wait)
    return last_response  # all retries exhausted, return last 5xx response


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
    subaccounts: Optional[list[dict]] = None,
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
    if subaccounts:
        payload["subaccounts"] = subaccounts

    async with _client() as client:
        response = await client.post(f"{FLW_BASE_URL}/payments", headers=_headers(), json=payload)
        response.raise_for_status()
        data = response.json()

    link = data.get("data", {}).get("link")
    if not link:
        raise RuntimeError(f"Flutterwave checkout link missing: {data}")
    return data


async def query_transaction_fee(amount: float, currency: str = "NGN") -> float:
    """
    Asks Flutterwave for the provider fee on a checkout amount so Qreek can
    charge the payer once and still settle the recipient's full amount.
    """
    query = urlencode({"amount": round(float(amount or 0), 2), "currency": currency})
    async with _client() as client:
        response = await client.get(f"{FLW_BASE_URL}/transactions/fee?{query}", headers=_headers())
        if response.is_error:
            logger.warning("Flutterwave fee lookup failed: %s %s", response.status_code, response.text[:300])
            return 0.0
        data = response.json().get("data", {})
    for key in ("fee", "app_fee", "merchant_fee", "charge_amount"):
        value = data.get(key)
        if value is not None:
            return round(float(value or 0), 2)
    return 0.0


async def create_collection_subaccount(
    *,
    account_bank: str,
    account_number: str,
    business_name: str,
    business_mobile: Optional[str] = None,
    business_email: Optional[str] = None,
    country: str = "NG",
    split_type: str = "flat",
    split_value: float = 0.0,
) -> dict:
    """
    Creates a Flutterwave collection subaccount for split payments.
    The checkout can then send the recipient's share directly to this account.
    """
    payload = {
        "account_bank": account_bank,
        "account_number": account_number,
        "business_name": business_name[:100],
        "business_mobile": business_mobile or "00000000000",
        "business_email": business_email or "noreply@qreek.app",
        "country": country,
        "split_type": split_type,
        "split_value": split_value,
    }
    async with _client() as client:
        response = await _post_with_retry(
            client,
            f"{FLW_BASE_URL}/subaccounts",
            headers=_headers(),
            json_body=payload,
        )
        if response.is_error:
            existing = await find_collection_subaccount(account_bank, account_number)
            if existing:
                logger.info(
                    "Reusing existing Flutterwave subaccount for bank %s account ****%s",
                    account_bank,
                    account_number[-4:],
                )
                return {
                    "data": existing,
                    "meta": {
                        "reused": True,
                        "reason": "existing_subaccount",
                    },
                }
            safe_payload = {**payload, "account_number": f"******{account_number[-4:]}"}
            # Raise with full context — the caller is responsible for structured
            # logging so we avoid double-logging the same failure event.
            raise FlutterwaveAPIError(
                f"Flutterwave subaccount creation failed ({response.status_code})",
                status_code=response.status_code,
                response_text=response.text[:1000],
                payload=safe_payload,
            )
        response.raise_for_status()
        return response.json()


async def find_collection_subaccount(account_bank: str, account_number: str) -> dict | None:
    """
    Finds an existing collection subaccount for the provided bank account.
    This is used to recover links where Flutterwave already has a subaccount
    for the bank/account pair, so we can reuse it instead of failing on create.
    """
    if not account_bank or not account_number:
        return None

    async with _client() as client:
        response = await client.get(f"{FLW_BASE_URL}/subaccounts", headers=_headers())
        if response.is_error:
            logger.warning("List subaccounts failed while searching for an existing collection subaccount: %s", response.text[:200])
            return None

        for item in (response.json().get("data") or []):
            if str(item.get("account_bank")) == str(account_bank) and str(item.get("account_number")) == str(account_number):
                return item
    return None


async def get_subaccount(subaccount_id: str) -> dict:
    """
    Fetch a subaccount. If given an RS_... subaccount_id (the code used for splits),
    we list all and match by subaccount_id (because single-fetch GET /subaccounts/{id}
    expects the *numeric* data.id per FW docs). Returns the item dict which includes
    both "id" (numeric, for update path) and "subaccount_id".
    This fixes "Merchant not found" on update when we only had the RS_ code stored.
    """
    sid = str(subaccount_id or "")
    async with _client() as client:
        if sid.startswith("RS_"):
            # List and match — single GET /subaccounts/RS_... fails (expects numeric)
            resp = await client.get(f"{FLW_BASE_URL}/subaccounts", headers=_headers())
            if resp.is_error:
                logger.warning("List subaccounts failed for RS lookup: %s", resp.text[:200])
                return None
            for item in (resp.json().get("data") or []):
                if item.get("subaccount_id") == sid:
                    return item
            return None
        # numeric id path
        response = await client.get(f"{FLW_BASE_URL}/subaccounts/{sid}", headers=_headers())
        if response.is_error:
            return None
        return response.json().get("data")


async def update_subaccount(
    subaccount_id: str,
    split_type: Optional[str] = "percentage",
    split_value: Optional[float] = 0.0025,
    business_name: Optional[str] = None,
) -> dict:
    """
    General updater for a collection subaccount (split config + business_name etc).
    - Resolves RS_ code -> numeric id via list (prevents "Merchant not found" on PUT path).
    - Carries over core fields from the fetched sub record (required by some FW update validations).
    - Used both for split correction (0.0025) and for propagating link.title -> sub business_name
      so that "edit name" on link actually updates what shows in the Flutterwave dashboard.
    The per-tx subaccounts[] override in checkout still wins for the actual payment split.
    """
    update_id = subaccount_id
    sub = None
    sid = str(subaccount_id or "")
    if sid.startswith("RS_"):
        sub = await get_subaccount(subaccount_id)
        if sub and sub.get("id"):
            update_id = sub["id"]
    payload: dict = {}
    if split_type is not None:
        payload["split_type"] = split_type
    if split_value is not None:
        payload["split_value"] = split_value
    if business_name:
        payload["business_name"] = business_name[:100]
    if sub:
        # carry over fields that update may require/validate to avoid merchant/validation errors
        for k in ("business_name", "business_email", "business_mobile", "account_bank", "account_number", "country"):
            if k in sub and k not in payload:
                payload[k] = sub[k]
    # If we are only updating name, still make sure we send a split (FW may expect it on some accounts)
    if "split_type" not in payload and sub:
        payload["split_type"] = sub.get("split_type", "percentage")
        payload["split_value"] = sub.get("split_value", 0.0025)
    async with _client() as client:
        response = await client.put(
            f"{FLW_BASE_URL}/subaccounts/{update_id}",
            headers=_headers(),
            json=payload,
        )
        if response.is_error:
            # Don't fail the payment if update fails; override at tx time is what matters.
            # (See pay_link subaccounts override + finalize always-split path.)
            logger.warning("Failed to update subaccount %s: %s", subaccount_id, response.text[:300])
            return {"status": "error", "message": response.text[:500]}
        response.raise_for_status()
        return response.json()


async def resolve_account(account_number: str, bank_code: str) -> dict:
    """
    Uses Flutterwave's accounts/resolve to verify bank account number + code before
    saving for a pool payment link. Returns the resolved account details (incl. account_name)
    or raises FlutterwaveAPIError on failure. This ensures the bank details for pool
    collection links are verified before being saved into our system (per user request).
    """
    if not account_number or not bank_code:
        raise ValueError("account_number and bank_code required")
    params = {"account_number": account_number, "bank_code": bank_code}
    async with _client() as client:
        response = await client.get(
            f"{FLW_BASE_URL}/accounts/resolve",
            headers=_headers(),
            params=params,
        )
        if response.is_error:
            raise FlutterwaveAPIError(
                f"Account verification failed ({response.status_code})",
                status_code=response.status_code,
                response_text=response.text[:1000],
                payload={"account_number": account_number[-4:], "bank_code": bank_code},
            )
        return response.json()


async def update_subaccount_split(
    subaccount_id: str,
    split_type: str = "percentage",
    split_value: float = 0.0025,
) -> dict:
    """Backward-compat wrapper (used by existing call sites in web_payment_links)."""
    return await update_subaccount(subaccount_id, split_type=split_type, split_value=split_value)


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
        if response.is_error:
            raise RuntimeError(f"Flutterwave transfer failed ({response.status_code}): {response.text[:500]}")
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
