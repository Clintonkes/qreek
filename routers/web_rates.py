from fastapi import APIRouter, Query
from core.rate_engine import get_all_rates
import redis.asyncio as aioredis
import json, os

router = APIRouter(prefix="/api/v1/rates", tags=["rates"])

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
_redis = None


async def _r():
    global _redis
    if not _redis:
        _redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


@router.get("")
async def get_rates(fiat: str = Query(default="NGN")):
    fiat      = fiat.upper()
    cache_key = f"web:rates:{fiat}"

    try:
        cached = await (await _r()).get(cache_key)
        if cached:
            return {"rates": json.loads(cached), "cached": True}
    except Exception:
        pass

    rates = await get_all_rates(fiat)

    try:
        await (await _r()).setex(cache_key, 30, json.dumps(rates))
    except Exception:
        pass

    return {"rates": rates, "cached": False}
