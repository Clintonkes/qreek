import httpx, asyncio, os
import redis.asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
_redis = None

BINANCE_SYMBOLS = {
    "BTC":  "BTCUSDT",
    "ETH":  "ETHUSDT",
    "BNB":  "BNBUSDT",
    "SOL":  "SOLUSDT",
    "USDC": "USDCUSDT",
}


async def _r():
    global _redis
    if not _redis:
        _redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


async def get_fiat_rate(fiat: str = "NGN") -> float:
    fiat      = fiat.upper()
    cache_key = f"rate:USD{fiat}"
    try:
        cached = await (await _r()).get(cache_key)
        if cached:
            return float(cached)
    except Exception:
        pass
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get("https://api.exchangerate-api.com/v4/latest/USD")
            if resp.status_code == 200:
                rate = float(resp.json().get("rates", {}).get(fiat, 0))
                if rate > 0:
                    await (await _r()).setex(cache_key, 300, str(rate))
                    return rate
    except Exception:
        pass
    try:
        cached = await (await _r()).get(cache_key)
        if cached:
            return float(cached)
    except Exception:
        pass
    return 0


async def get_rate(currency: str = "USDT", fiat: str = "NGN") -> float:
    currency  = currency.upper()
    fiat      = fiat.upper()
    key       = f"rate:{currency}:{fiat}"
    usd_fiat  = await get_fiat_rate(fiat)
    if usd_fiat <= 0:
        return 0
    if currency == "USDT":
        await (await _r()).setex(key, 60, str(usd_fiat))
        return usd_fiat
    try:
        cached = await (await _r()).get(key)
        if cached:
            return float(cached)
    except Exception:
        pass
    binance_sym = BINANCE_SYMBOLS.get(currency, f"{currency}USDT")
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            resp = await c.get("https://api.binance.com/api/v3/ticker/24hr", params={"symbol": binance_sym})
            if resp.status_code == 200:
                data      = resp.json()
                usd_price = float(data.get("lastPrice", 0))
                pct       = float(data.get("priceChangePercent", 0))
                if usd_price > 0:
                    rate = usd_price * usd_fiat
                    r    = await _r()
                    await r.setex(key, 60, str(rate))
                    await r.setex(f"chg:{currency}:{fiat}", 60, str(pct))
                    return rate
    except Exception:
        pass
    try:
        cached = await (await _r()).get(key)
        if cached:
            return float(cached)
    except Exception:
        pass
    return 0


async def get_all_rates(fiat: str = "NGN") -> dict:
    fiat     = fiat.upper()
    usd_fiat = await get_fiat_rate(fiat)
    output   = {}
    if usd_fiat <= 0:
        for sym in ["USDT", "BTC", "ETH", "BNB", "SOL", "USDC"]:
            output[sym] = {"rate": 0, "change": 0}
        return output
    output["USDT"] = {"rate": usd_fiat, "change": 0}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            tasks   = [c.get("https://api.binance.com/api/v3/ticker/24hr", params={"symbol": sym}) for sym in BINANCE_SYMBOLS.values()]
            symbols = list(BINANCE_SYMBOLS.keys())
            resps   = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, resp in zip(symbols, resps):
                if isinstance(resp, Exception):
                    cached = await (await _r()).get(f"rate:{sym}:{fiat}")
                    output[sym] = {"rate": float(cached) if cached else 0, "change": 0}
                elif resp.status_code == 200:
                    d     = resp.json()
                    price = float(d.get("lastPrice", 0))
                    pct   = float(d.get("priceChangePercent", 0))
                    output[sym] = {"rate": round(price * usd_fiat, 2), "change": round(pct, 2)} if price > 0 else {"rate": 0, "change": 0}
                else:
                    output[sym] = {"rate": 0, "change": 0}
    except Exception:
        for sym in BINANCE_SYMBOLS:
            try:
                cached = await (await _r()).get(f"rate:{sym}:{fiat}")
                output[sym] = {"rate": float(cached) if cached else 0, "change": 0}
            except Exception:
                output[sym] = {"rate": 0, "change": 0}
    return output


async def market_message(fiat: str = "NGN") -> str:
    fiat   = fiat.upper()
    rates  = await get_all_rates(fiat)
    symbol = {"NGN": "₦", "GHS": "₵", "PHP": "₱", "USD": "$"}.get(fiat, fiat)
    lines  = [f"📊 *Live Crypto Rates ({fiat})*", ""]
    for coin, d in rates.items():
        if d["rate"] <= 0:
            continue
        r     = d["rate"]
        arrow = "📈" if d["change"] >= 0 else "📉"
        sign  = "+" if d["change"] >= 0 else ""
        fmt   = f"{symbol}{r / 1_000_000:.2f}M" if r >= 1_000_000 else f"{symbol}{r:,.0f}"
        lines.append(f"{arrow} {coin}: {fmt}  ({sign}{d['change']}% 24h)")
    lines += ["", "📌 All fees shown before you confirm any trade."]
    return "\n".join(lines)
