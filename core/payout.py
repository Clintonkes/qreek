import httpx, os, asyncio

YC_BASE    = os.getenv("YELLOWCARD_API_URL", "https://api.yellowcard.io/v1")
YC_KEY     = os.getenv("YELLOWCARD_API_KEY")
BREET_BASE = os.getenv("BREET_BASE_URL", "https://api.breet.io/v1")
BREET_KEY  = os.getenv("BREET_API_KEY")
_client    = None


def _c():
    global _client
    if not _client:
        _client = httpx.AsyncClient(timeout=12)
    return _client


async def _yc_payout(phone: str, amount: float, bank: dict, ref: str) -> dict:
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
    return r.json()


async def _breet_payout(phone: str, amount: float, bank: dict, ref: str) -> dict:
    h = {"Authorization": f"Bearer {BREET_KEY}", "Content-Type": "application/json"}
    r = await _c().post(f"{BREET_BASE}/payouts", headers=h, json={
        "user_id":        phone,
        "amount":         amount,
        "currency":       "NGN",
        "bank_code":      bank["bank_code"],
        "account_number": bank["account_number"],
        "narration":      "Qreek Finance Payout",
    })
    return r.json()


async def best_payout(phone: str, amount: float, bank: dict, ref: str) -> dict:
    """Yellow Card first, Breet fallback. Called via asyncio.create_task."""
    try:
        result = await asyncio.wait_for(_yc_payout(phone, amount, bank, ref), timeout=5.0)
        if result.get("status") in ("success", "pending", "processing"):
            return {"provider": "yellowcard", **result}
    except Exception as e:
        print(f"YC failed ({e}), falling back to Breet for {ref}")
    result = await _breet_payout(phone, amount, bank, ref + "_b")
    return {"provider": "breet", **result}


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
