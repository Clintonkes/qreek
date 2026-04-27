import httpx, os, json, re

OPENAI_KEY = os.getenv("OPENAI_API_KEY")

SYSTEM = '''You are Qreek Finance, a friendly Nigerian crypto fintech assistant.
Extract intent from the user message and return ONLY valid JSON. No preamble, no markdown.

Actions: sell | buy | send_crypto | rate | market | watch_price | my_alerts | cancel_alert
         create_pool | join_pool | create_fiat_pool | join_fiat_pool
         pool_send | individual_send | balance | portfolio | history | help | refer | bridge | chat

JSON schema:
{
  "action": string,
  "amount": number | null,
  "currency": string | null,
  "recipient": string | null,
  "pool_code": string | null,
  "pool_name": string | null,
  "target_price": number | null,
  "direction": "above" | "below" | null,
  "from_chain": string | null,
  "to_chain": string | null,
  "in_pool": boolean,
  "bank_account": string | null,
  "bank_name": string | null,
  "chat_reply": string | null
}

Rules:
- in_pool: true only if the user explicitly mentions pool trade
- For watch_price: direction is "above" if target > current, else "below"
- For chat/hello messages set action to "chat" with a short helpful chat_reply
- Return ONLY the JSON object. Nothing else.'''


async def parse_intent(text: str, phone: str) -> dict:
    if not OPENAI_KEY:
        return _fallback(text)
    try:
        h = {"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"}
        p = {
            "model": "gpt-4o-mini",
            "max_tokens": 200,
            "temperature": 0.0,
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user",   "content": text},
            ],
        }
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.post("https://api.openai.com/v1/chat/completions", json=p, headers=h)
        raw = r.json()["choices"][0]["message"]["content"].strip()
        return json.loads(raw)
    except Exception:
        return _fallback(text)


def _fallback(text: str) -> dict:
    t     = text.lower().strip()
    nums  = re.findall(r"\d+\.?\d*", t)
    amt   = float(nums[0]) if nums else None
    coins = ["usdt", "btc", "eth", "bnb", "sol", "usdc"]
    curr  = next((c.upper() for c in coins if c in t), None)
    if any(w in t for w in ["hi", "hello", "hey", "start", "menu", "help"]):
        return {"action": "help"}
    if "sell" in t:
        return {"action": "sell", "amount": amt, "currency": curr, "in_pool": False}
    if "buy" in t:
        return {"action": "buy", "amount": amt, "currency": curr, "in_pool": False}
    if "send" in t and "ngn" in t:
        return {"action": "individual_send", "amount": amt, "recipient": None}
    if "send" in t:
        return {"action": "send_crypto", "amount": amt, "currency": curr, "recipient": None}
    if "bridge" in t:
        return {"action": "bridge", "amount": amt, "currency": curr, "from_chain": "tron", "to_chain": "solana"}
    if "market" in t or "rate" in t:
        return {"action": "market"}
    if "watch" in t or "alert" in t:
        return {"action": "watch_price", "currency": curr, "target_price": amt}
    if "my alert" in t:
        return {"action": "my_alerts"}
    if "portfolio" in t or "balance" in t:
        return {"action": "portfolio"}
    if "history" in t:
        return {"action": "history"}
    if "create pool" in t:
        return {"action": "create_pool", "pool_name": text.replace("create pool", "").strip() or "My Pool"}
    if "join" in t:
        parts = t.split()
        code  = parts[-1].upper() if len(parts) > 1 else ""
        return {"action": "join_pool", "pool_code": code}
    if "refer" in t:
        return {"action": "refer"}
    if re.match(r"^\d{4,6}$", t):
        return {"action": "confirm_pin", "pin": t}
    return {"action": "help"}
