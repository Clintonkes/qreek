"""
Redis-backed conversation state machine — shared with qreek_finance via same Redis instance.
Web app reads/writes the same state keys so flows started on WhatsApp continue seamlessly on web.
"""
import json, os
import redis.asyncio as aioredis

REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379")
STATE_TTL   = 1800   # 30 minutes
PENDING_TTL = 900    # 15 minutes
_redis      = None


class State:
    NEW                       = "new"
    AWAIT_BVN                 = "await_bvn"
    AWAIT_PIN_SETUP           = "await_pin_setup"
    VERIFIED                  = "verified"
    AWAIT_SELL_ACCOUNT        = "await_sell_account"
    AWAIT_SELL_CONFIRM        = "await_sell_confirm"
    AWAIT_SELL_PIN            = "await_sell_pin"
    AWAIT_SELL_PAYOUT_ACCOUNT = "await_sell_payout_account"
    AWAIT_BUY_CONFIRM         = "await_buy_confirm"
    AWAIT_BUY_PAID            = "await_buy_paid"
    AWAIT_SEND_CONFIRM        = "await_send_confirm"
    AWAIT_SEND_PIN            = "await_send_pin"
    AWAIT_FIAT_CONFIRM        = "await_fiat_confirm"
    AWAIT_FIAT_PIN            = "await_fiat_pin"
    AWAIT_BRIDGE_CONFIRM      = "await_bridge_confirm"
    AWAIT_BRIDGE_PIN          = "await_bridge_pin"
    AWAIT_CURRENCY            = "await_currency"
    AWAIT_POOL_TRADE_PIN      = "await_pool_trade_pin"
    AWAIT_NEW_PIN             = "await_new_pin"
    AWAIT_ESCROW_DEPOSIT      = "await_escrow_deposit"
    FROZEN                    = "frozen"


async def _r():
    global _redis
    if not _redis:
        _redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


async def get_state(phone: str) -> str:
    r   = await _r()
    val = await r.get(f"state:{phone}")
    return val or State.NEW


async def set_state(phone: str, state: str):
    r = await _r()
    await r.setex(f"state:{phone}", STATE_TTL, state)


async def clear_state(phone: str):
    await set_state(phone, State.VERIFIED)


async def save_pending(phone: str, key: str, data: dict):
    r = await _r()
    await r.setex(f"pending:{key}:{phone}", PENDING_TTL, json.dumps(data))


async def get_pending(phone: str, key: str) -> dict | None:
    r   = await _r()
    raw = await r.get(f"pending:{key}:{phone}")
    return json.loads(raw) if raw else None


async def clear_pending(phone: str, key: str):
    r = await _r()
    await r.delete(f"pending:{key}:{phone}")


async def increment_fail(phone: str) -> int:
    r   = await _r()
    key = f"pin_fail:{phone}"
    count = await r.incr(key)
    await r.expire(key, 3600)
    return count


async def reset_fail(phone: str):
    r = await _r()
    await r.delete(f"pin_fail:{phone}")
