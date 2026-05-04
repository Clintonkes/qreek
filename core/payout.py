import httpx, os, asyncio

YC_BASE    = os.getenv("YELLOWCARD_API_URL", "https://api.yellowcard.io/v1")
YC_KEY     = os.getenv("YELLOWCARD_API_KEY")
BREET_BASE = os.getenv("BREET_BASE_URL", "https://api.breet.io/v1")
BREET_KEY  = os.getenv("BREET_API_KEY")
FEE_BANK_ACCOUNT = os.getenv("QREEK_FEE_ACCOUNT_NUMBER")
FEE_BANK_CODE = os.getenv("QREEK_FEE_BANK_CODE")
_client    = None


def _c():
    global _client
    if not _client:
        _client = httpx.AsyncClient(timeout=12)
    return _client


async def _yc_payout(phone: str, amount: float, bank: dict, ref: str) -> dict:
    if not YC_KEY:
        raise RuntimeError("YELLOWCARD_API_KEY is not configured")
    h = {"Authorization": f"Bearer {YC_KEY}", "Content-Type": "application/json"}
    r = await _c().post(f"{YC_BASE}/payouts", headers=h, json={
        "amount": amount,
        "currency": "NGN",
        "reference": ref,
        "destination": {
            "accountNumber": bank["account_number"],
            "bankCode":      bank["bank_code"],
        },
    })
    r.raise_for_status()
    return r.json()


async def _breet_payout(phone: str, amount: float, bank: dict, ref: str) -> dict:
    if not BREET_KEY:
        raise RuntimeError("BREET_API_KEY is not configured")
    h = {"Authorization": f"Bearer {BREET_KEY}", "Content-Type": "application/json"}
    r = await _c().post(f"{BREET_BASE}/payouts", headers=h, json={
        "user_id":        phone,
        "amount":         amount,
        "currency":       "NGN",
        "bank_code":      bank["bank_code"],
        "account_number": bank["account_number"],
        "narration":      "Qreek Finance Payout",
    })
    r.raise_for_status()
    return r.json()


def _is_live_success(result: dict) -> bool:
    status = str(result.get("status") or result.get("state") or "").lower()
    return status in ("success", "successful", "pending", "processing", "completed", "queued")


async def best_payout(phone: str, amount: float, bank: dict, ref: str) -> dict:
    """Yellow Card first, Breet fallback. Called via asyncio.create_task."""
    if amount <= 0:
        raise ValueError("Payout amount must be greater than zero")

    last_error = None

    try:
        result = await asyncio.wait_for(_yc_payout(phone, amount, bank, ref), timeout=5.0)
        if _is_live_success(result):
            return {"provider": "yellowcard", **result}
        last_error = RuntimeError(f"Yellow Card rejected payout: {result}")
    except Exception as e:
        last_error = e
        print(f"YC failed ({e}), falling back to Breet for {ref}")

    try:
        result = await _breet_payout(phone, amount, bank, ref + "_b")
        if _is_live_success(result):
            return {"provider": "breet", **result}
        raise RuntimeError(f"Breet rejected payout: {result}")
    except Exception as e:
        raise RuntimeError(f"All payout providers failed for {ref}: {e}") from last_error


def fee_bank() -> dict:
    if not FEE_BANK_ACCOUNT or not FEE_BANK_CODE:
        raise RuntimeError("QREEK_FEE_ACCOUNT_NUMBER and QREEK_FEE_BANK_CODE must be configured")
    return {"account_number": FEE_BANK_ACCOUNT, "bank_code": FEE_BANK_CODE}


async def settle_fee(phone: str, amount: float, ref: str) -> dict | None:
    if amount <= 0:
        return None
    return await best_payout(phone, amount, fee_bank(), f"{ref}_FEE")


async def get_virtual_account(phone: str) -> dict:
    try:
        if BREET_KEY:
            h  = {"Authorization": f"Bearer {BREET_KEY}"}
            r  = await _c().get(f"{BREET_BASE}/wallets/{phone}", headers=h)
            if r.status_code == 200:
                d = r.json()
                if d.get("virtual_account_number"):
                    return {"account_number": d["virtual_account_number"], "bank_name": d.get("bank_name", "Breet")}
            cr = await _c().post(f"{BREET_BASE}/wallets", json={"user_id": phone, "currency": "NGN"}, headers=h)
            if cr.status_code in (200, 201):
                d = cr.json()
                if d.get("virtual_account_number"):
                    return {"account_number": d["virtual_account_number"], "bank_name": d.get("bank_name", "Breet")}
    except Exception as e:
        print(f"Virtual account error: {e}")
    return {"account_number": "Contact support", "bank_name": "Qreek Finance"}
