"""
WebSocket trade chat — connects the React frontend to the Qreek conversational flow.
Shares the same Redis state machine as qreek_finance so WhatsApp and web flows coexist.
"""
import asyncio, json, os, uuid, re
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from jose import jwt, JWTError
from sqlalchemy import select, desc, func
from database.session import AsyncSessionLocal
from database.models import User, Transaction, PriceAlert, Pool, PoolMember, Referral
from core.session import (
    get_state, set_state, save_pending, get_pending, clear_pending,
    increment_fail, reset_fail, State,
)
from core.ai_parser import parse_intent
from core.rate_engine import get_rate, get_all_rates, market_message
from core.payout import best_payout, get_virtual_account
from services.security_service import verify_pin, freeze_account, is_frozen

SECRET = os.getenv("JWT_SECRET", "qreek-change-this-in-production-use-openssl-rand-hex-32")
ALGO   = "HS256"

router = APIRouter(tags=["websocket"])

FEE_EXTERNAL = 0.004
FEE_POOL     = 0.0025
FEE_SEND     = 0.001


# ── helpers ──────────────────────────────────────────────────────────────────

def _ngn(v: float) -> str:
    if v >= 1_000_000:
        return f"₦{v / 1_000_000:.2f}M"
    return f"₦{v:,.2f}"


def _crypto(amount: float, currency: str) -> str:
    if currency in ("BTC", "ETH"):
        return f"{amount:.6f} {currency}"
    if currency in ("SOL", "BNB"):
        return f"{amount:.4f} {currency}"
    return f"{amount:.2f} {currency}"


def _out(message: str, step: str = "idle", pending: dict | None = None) -> str:
    return json.dumps({"message": message, "step": step, "pending": pending})


async def _user(db, phone: str) -> User | None:
    r = await db.execute(select(User).where(User.phone == phone))
    return r.scalar_one_or_none()


async def _in_pool(phone: str) -> bool:
    async with AsyncSessionLocal() as db:
        r = await db.execute(select(PoolMember).where(PoolMember.user_phone == phone))
        return r.scalar_one_or_none() is not None


# ── intent handlers ───────────────────────────────────────────────────────────

async def _sell(ws: WebSocket, phone: str, intent: dict):
    amount   = intent.get("amount")
    currency = (intent.get("currency") or "USDT").upper()

    if not amount or amount <= 0:
        await ws.send_text(_out("How much do you want to sell? Example: sell 100 USDT", "idle"))
        return

    rate = await get_rate(currency)
    if rate <= 0:
        await ws.send_text(_out(f"⚠️ Live rate for {currency} unavailable. Try again shortly.", "idle"))
        return

    in_p    = await _in_pool(phone)
    fee_pct = FEE_POOL if in_p else FEE_EXTERNAL
    gross   = amount * rate
    fee     = gross * fee_pct
    net     = gross - fee
    tag     = " (pool rate)" if in_p else ""

    msg = (
        f"💸 Sell {_crypto(amount, currency)}\n"
        f"{'─' * 30}\n"
        f"Live rate:     {_ngn(rate)}/{currency}\n"
        f"Fee ({fee_pct*100:.2f}%{tag}): {_ngn(fee)}\n"
        f"You receive:   {_ngn(net)}\n\n"
        f"Which account should we pay?\n"
        f"Reply: account_number bank_code\n"
        f"e.g.  0123456789 058"
    )

    await save_pending(phone, "sell", {
        "amount": amount, "currency": currency,
        "rate": rate, "fee": fee, "fee_pct": fee_pct,
        "net_ngn": net, "gross_ngn": gross,
    })
    await set_state(phone, State.AWAIT_SELL_ACCOUNT)
    await ws.send_text(_out(msg, "awaiting_account", {"amount": amount, "currency": currency, "net_ngn": net}))


async def _buy(ws: WebSocket, phone: str, intent: dict):
    amount   = intent.get("amount")
    currency = (intent.get("currency") or "USDT").upper()

    if not amount or amount <= 0:
        await ws.send_text(_out("How much do you want to buy? Example: buy 100 USDT", "idle"))
        return

    rate = await get_rate(currency)
    if rate <= 0:
        await ws.send_text(_out(f"⚠️ Live rate for {currency} unavailable.", "idle"))
        return

    buy_rate  = rate * 1.01          # 1% spread, disclosed
    total_ngn = amount * buy_rate
    va        = await get_virtual_account(phone)

    msg = (
        f"🛒 Buy {_crypto(amount, currency)}\n"
        f"{'─' * 30}\n"
        f"Buy rate:     {_ngn(buy_rate)}/{currency} (incl. 1% spread)\n"
        f"Total to pay: {_ngn(total_ngn)}\n\n"
        f"Pay to this account:\n"
        f"  Bank:    {va.get('bank_name', 'Qreek Finance')}\n"
        f"  Account: {va.get('account_number', '—')}\n\n"
        f"Reply PAID once you've transferred."
    )

    await save_pending(phone, "buy", {
        "amount": amount, "currency": currency,
        "buy_rate": buy_rate, "total_ngn": total_ngn,
    })
    await set_state(phone, State.AWAIT_BUY_PAID)
    await ws.send_text(_out(msg, "awaiting_crypto", {"amount": amount, "currency": currency, "total_ngn": total_ngn}))


async def _send(ws: WebSocket, phone: str, intent: dict):
    amount    = intent.get("amount")
    currency  = (intent.get("currency") or "USDT").upper()
    recipient = intent.get("recipient")

    if not amount or amount <= 0:
        await ws.send_text(_out("How much to send? Example: send 50 USDT to 08012345678", "idle"))
        return
    if not recipient:
        await ws.send_text(_out("Who are you sending to? Include their phone number.", "idle"))
        return

    rate = await get_rate(currency)
    fee  = amount * FEE_SEND
    net  = amount - fee

    msg = (
        f"📤 Send {_crypto(amount, currency)}\n"
        f"{'─' * 30}\n"
        f"To:             {recipient}\n"
        f"Fee (0.1%):     {_crypto(fee, currency)}\n"
        f"Recipient gets: {_crypto(net, currency)} ≈ {_ngn(net * rate)}\n\n"
        f"Type YES to confirm or CANCEL to abort."
    )

    await save_pending(phone, "send", {
        "amount": amount, "currency": currency,
        "recipient": recipient, "fee": fee, "net": net,
    })
    await set_state(phone, State.AWAIT_SEND_CONFIRM)
    await ws.send_text(_out(msg, "confirm", {"amount": amount, "currency": currency, "recipient": recipient}))


async def _portfolio(ws: WebSocket, phone: str):
    async with AsyncSessionLocal() as db:
        u = await _user(db, phone)
    rates = await get_all_rates("NGN")
    coins = {
        "NGN":  (u.balance_ngn, 1),
        "USDT": (u.balance_usdt, rates.get("USDT", {}).get("rate", 0)),
        "USDC": (u.balance_usdc, rates.get("USDC", {}).get("rate", 0)),
        "BTC":  (u.balance_btc,  rates.get("BTC",  {}).get("rate", 0)),
        "ETH":  (u.balance_eth,  rates.get("ETH",  {}).get("rate", 0)),
        "BNB":  (u.balance_bnb,  rates.get("BNB",  {}).get("rate", 0)),
        "SOL":  (u.balance_sol,  rates.get("SOL",  {}).get("rate", 0)),
    }
    lines = ["📊 *Your Portfolio*", "─" * 30]
    total = 0
    for coin, (bal, rate) in coins.items():
        if bal <= 0:
            continue
        ngn_val = bal * rate
        total  += ngn_val
        if coin == "NGN":
            lines.append(f"  NGN   {_ngn(bal)}")
        else:
            lines.append(f"  {coin}   {_crypto(bal, coin)} ≈ {_ngn(ngn_val)}")
    lines += ["─" * 30, f"Total: {_ngn(total)}"]
    await ws.send_text(_out("\n".join(lines), "idle"))


async def _history(ws: WebSocket, phone: str):
    async with AsyncSessionLocal() as db:
        r    = await db.execute(
            select(Transaction)
            .where(Transaction.user_phone == phone)
            .order_by(desc(Transaction.created_at))
            .limit(10)
        )
        txns = r.scalars().all()
    if not txns:
        await ws.send_text(_out("No transactions yet. Make your first trade!", "idle"))
        return
    lines = ["📋 *Last 10 Transactions*", "─" * 30]
    for t in txns:
        date   = t.created_at.strftime("%d %b %H:%M") if t.created_at else "—"
        icon   = "✅" if t.status == "completed" else ("⏳" if t.status in ("pending", "processing") else "❌")
        lines.append(f"{icon} {t.tx_type.upper()} {t.amount} {t.currency} → {_ngn(t.ngn_amount or 0)}  [{date}]")
    await ws.send_text(_out("\n".join(lines), "idle"))


async def _create_pool(ws: WebSocket, phone: str, intent: dict):
    name = (intent.get("pool_name") or "My Pool").strip()
    async with AsyncSessionLocal() as db:
        pool = Pool(name=name, creator_phone=phone, pool_type="crypto")
        db.add(pool)
        await db.flush()
        db.add(PoolMember(pool_id=pool.id, user_phone=phone, role="admin"))
        await db.commit()
        invite = pool.invite_code
        pname  = pool.name
    await ws.send_text(_out(
        f"🏦 Pool *{pname}* created!\n"
        f"Invite code: *{invite}*\n"
        f"Share this code with your group.\n"
        f"Pool members trade at 0.25% fee.",
        "idle"
    ))


async def _join_pool(ws: WebSocket, phone: str, intent: dict):
    code = (intent.get("pool_code") or "").strip().upper()
    if not code:
        await ws.send_text(_out("Provide a pool invite code. Example: join pool ABC123", "idle"))
        return
    async with AsyncSessionLocal() as db:
        pr   = await db.execute(select(Pool).where(Pool.invite_code == code))
        pool = pr.scalar_one_or_none()
        if not pool:
            await ws.send_text(_out(f"❌ No pool with code {code}.", "idle"))
            return
        ex = await db.execute(select(PoolMember).where(PoolMember.pool_id == pool.id, PoolMember.user_phone == phone))
        if ex.scalar_one_or_none():
            await ws.send_text(_out(f"You're already in pool *{pool.name}*!", "idle"))
            return
        pool.member_count = (pool.member_count or 1) + 1
        db.add(PoolMember(pool_id=pool.id, user_phone=phone, role="member"))
        await db.commit()
        pname = pool.name
    await ws.send_text(_out(
        f"🎉 Joined pool *{pname}*!\nYour trades now qualify for the 0.25% pool fee.",
        "idle"
    ))


async def _watch_price(ws: WebSocket, phone: str, intent: dict):
    currency     = (intent.get("currency") or "BTC").upper()
    target_price = intent.get("target_price")
    if not target_price:
        await ws.send_text(_out("Specify a target price. Example: alert me when BTC hits ₦150M", "idle"))
        return
    direction = intent.get("direction")
    if not direction:
        current   = await get_rate(currency)
        direction = "above" if target_price > current else "below"
    async with AsyncSessionLocal() as db:
        alert = PriceAlert(user_phone=phone, currency=currency, target_price=target_price, direction=direction)
        db.add(alert)
        await db.commit()
    await ws.send_text(_out(
        f"🔔 Alert set! Notify when {currency} goes {direction} {_ngn(target_price)}.",
        "idle"
    ))


async def _my_alerts(ws: WebSocket, phone: str):
    async with AsyncSessionLocal() as db:
        r      = await db.execute(
            select(PriceAlert).where(PriceAlert.user_phone == phone, PriceAlert.triggered == False)
        )
        alerts = r.scalars().all()
    if not alerts:
        await ws.send_text(_out("No active alerts. Set one: alert me when BTC hits ₦150M", "idle"))
        return
    lines = ["🔔 *Active Price Alerts*", "─" * 30]
    for a in alerts:
        lines.append(f"  {a.currency}  {a.direction}  {_ngn(a.target_price)}")
    await ws.send_text(_out("\n".join(lines), "idle"))


async def _refer(ws: WebSocket, phone: str):
    async with AsyncSessionLocal() as db:
        u     = await _user(db, phone)
        cr    = await db.execute(select(func.count()).where(Referral.referrer_phone == phone))
        count = cr.scalar() or 0
    await ws.send_text(_out(
        f"🎁 *Your Referral Code*\n{u.referral_code}\n\n"
        f"Referrals made: {count}",
        "idle"
    ))


def _help() -> str:
    return (
        "👋 *Qreek Finance Commands*\n"
        "─" * 30 + "\n"
        "💸 sell 100 USDT\n"
        "🛒 buy 50 USDT\n"
        "📤 send 20 USDT to 08012345678\n"
        "📊 market  —  live rates\n"
        "💼 portfolio\n"
        "📋 history\n"
        "🏦 create pool My Group\n"
        "🔗 join pool ABC123\n"
        "🔔 alert me when BTC hits ₦150M\n"
        "🔔 my alerts\n"
        "🎁 refer\n"
        "─" * 30 + "\n"
        "Just type naturally — our AI understands you."
    )


# ── pending-state handler ─────────────────────────────────────────────────────

async def _handle_pending(ws: WebSocket, phone: str, state: str, text: str) -> bool:
    t = text.strip()

    if t.lower() in ("cancel", "stop", "abort"):
        for key in ("sell", "buy", "send"):
            await clear_pending(phone, key)
        await set_state(phone, State.VERIFIED)
        await ws.send_text(_out("✅ Cancelled. What else can I help with?", "idle"))
        return True

    # ── SELL ACCOUNT ──────────────────────────────────────────────────────────
    if state == State.AWAIT_SELL_ACCOUNT:
        parts   = t.split()
        account = bank_code = None

        if len(parts) >= 2 and re.match(r"^\d{10}$", parts[0]):
            account, bank_code = parts[0], parts[1]
        elif len(parts) == 1 and re.match(r"^\d{10}$", parts[0]):
            p = await get_pending(phone, "sell")
            if p and p.get("bank_code"):
                account, bank_code = parts[0], p["bank_code"]

        if not account or not bank_code:
            await ws.send_text(_out(
                "Please provide account number and bank code.\nExample: 0123456789 058",
                "awaiting_account"
            ))
            return True

        from core.banks import resolve_bank
        bank      = resolve_bank(bank_code)
        bank_name = bank["name"] if bank else bank_code
        pending   = await get_pending(phone, "sell")
        if not pending:
            await set_state(phone, State.VERIFIED)
            await ws.send_text(_out("Session expired. Please start a new sell.", "idle"))
            return True

        pending.update({"bank_account": account, "bank_code": bank_code, "bank_name": bank_name})
        await save_pending(phone, "sell", pending)
        await set_state(phone, State.AWAIT_SELL_CONFIRM)

        masked = "****" + account[-4:]
        await ws.send_text(_out(
            f"✅ Confirm Sale\n{'─' * 30}\n"
            f"Amount:  {_crypto(pending['amount'], pending['currency'])}\n"
            f"Rate:    {_ngn(pending['rate'])}/{pending['currency']}\n"
            f"Fee:     {_ngn(pending['fee'])} ({pending['fee_pct']*100:.2f}%)\n"
            f"Receive: {_ngn(pending['net_ngn'])}\n"
            f"Bank:    {bank_name}  {masked}\n\n"
            f"Type YES to confirm or CANCEL to abort.",
            "confirm", pending,
        ))
        return True

    # ── SELL CONFIRM ──────────────────────────────────────────────────────────
    if state == State.AWAIT_SELL_CONFIRM:
        if t.upper() not in ("YES", "Y", "CONFIRM", "OK"):
            await ws.send_text(_out("Type YES to confirm or CANCEL to abort.", "confirm"))
            return True
        await set_state(phone, State.AWAIT_SELL_PIN)
        await ws.send_text(_out("🔐 Enter your PIN to authorise.", "pin"))
        return True

    # ── SELL PIN ──────────────────────────────────────────────────────────────
    if state == State.AWAIT_SELL_PIN:
        if not re.match(r"^\d{4,6}$", t):
            await ws.send_text(_out("Enter your 4–6 digit PIN.", "pin"))
            return True

        async with AsyncSessionLocal() as db:
            if await is_frozen(db, phone):
                await ws.send_text(_out("🚫 Account frozen. Contact support.", "frozen"))
                return True
            ok = await verify_pin(db, phone, t)
            if not ok:
                fails = await increment_fail(phone)
                if fails >= 5:
                    await freeze_account(db, phone)
                    await set_state(phone, State.FROZEN)
                    await ws.send_text(_out("🚫 Account frozen after 5 failed PIN attempts.", "frozen"))
                    return True
                await ws.send_text(_out(f"❌ Wrong PIN. {5 - fails} attempt(s) remaining.", "pin"))
                return True
            await reset_fail(phone)
            pending = await get_pending(phone, "sell")
            if not pending:
                await set_state(phone, State.VERIFIED)
                await ws.send_text(_out("Session expired. Please start a new sell.", "idle"))
                return True
            ref  = "QRK_" + uuid.uuid4().hex[:10].upper()
            bank = {"account_number": pending["bank_account"], "bank_code": pending["bank_code"]}
            txn  = Transaction(
                user_phone=phone, tx_type="sell",
                currency=pending["currency"], amount=pending["amount"],
                ngn_amount=pending["net_ngn"], rate=pending["rate"],
                fee=pending["fee"], fee_pct=pending["fee_pct"],
                status="processing", reference=ref,
                bank_account=pending["bank_account"],
                bank_code=pending["bank_code"],
                bank_name=pending["bank_name"],
            )
            db.add(txn)
            await db.commit()

        asyncio.create_task(best_payout(phone, pending["net_ngn"], bank, ref))
        await clear_pending(phone, "sell")
        await set_state(phone, State.VERIFIED)
        await ws.send_text(_out(
            f"🎉 Done! Payout processing.\n{'─' * 30}\n"
            f"Amount: {_ngn(pending['net_ngn'])}\n"
            f"Bank:   {pending['bank_name']}  ****{pending['bank_account'][-4:]}\n"
            f"Ref:    {ref}\n\n"
            f"Arrives in under 5 minutes.",
            "done", {"reference": ref},
        ))
        return True

    # ── BUY PAID ──────────────────────────────────────────────────────────────
    if state == State.AWAIT_BUY_PAID:
        if t.upper() not in ("PAID", "SENT", "DONE", "YES"):
            await ws.send_text(_out("Reply PAID once you've made the bank transfer.", "awaiting_crypto"))
            return True
        pending = await get_pending(phone, "buy")
        if not pending:
            await set_state(phone, State.VERIFIED)
            await ws.send_text(_out("Session expired. Please restart the buy.", "idle"))
            return True
        ref = "QRK_B_" + uuid.uuid4().hex[:8].upper()
        async with AsyncSessionLocal() as db:
            txn = Transaction(
                user_phone=phone, tx_type="buy",
                currency=pending["currency"], amount=pending["amount"],
                ngn_amount=pending["total_ngn"], rate=pending["buy_rate"],
                status="pending", reference=ref,
            )
            db.add(txn)
            await db.commit()
        await clear_pending(phone, "buy")
        await set_state(phone, State.VERIFIED)
        await ws.send_text(_out(
            f"✅ Payment noted! Ref: {ref}\n"
            f"We're verifying your transfer. {pending['currency']} will be credited once confirmed.",
            "done"
        ))
        return True

    # ── SEND CONFIRM ──────────────────────────────────────────────────────────
    if state == State.AWAIT_SEND_CONFIRM:
        if t.upper() not in ("YES", "Y", "CONFIRM", "OK"):
            await ws.send_text(_out("Type YES to confirm or CANCEL to abort.", "confirm"))
            return True
        await set_state(phone, State.AWAIT_SEND_PIN)
        await ws.send_text(_out("🔐 Enter your PIN to send.", "pin"))
        return True

    # ── SEND PIN ──────────────────────────────────────────────────────────────
    if state == State.AWAIT_SEND_PIN:
        if not re.match(r"^\d{4,6}$", t):
            await ws.send_text(_out("Enter your 4–6 digit PIN.", "pin"))
            return True

        async with AsyncSessionLocal() as db:
            ok = await verify_pin(db, phone, t)
            if not ok:
                fails = await increment_fail(phone)
                if fails >= 5:
                    await freeze_account(db, phone)
                    await set_state(phone, State.FROZEN)
                    await ws.send_text(_out("🚫 Account frozen.", "frozen"))
                    return True
                await ws.send_text(_out(f"❌ Wrong PIN. {5 - fails} remaining.", "pin"))
                return True
            await reset_fail(phone)
            pending = await get_pending(phone, "send")
            if not pending:
                await set_state(phone, State.VERIFIED)
                await ws.send_text(_out("Session expired. Please start again.", "idle"))
                return True

            sender  = await _user(db, phone)
            bal     = getattr(sender, f"balance_{pending['currency'].lower()}", 0)
            if bal < pending["amount"]:
                await clear_pending(phone, "send")
                await set_state(phone, State.VERIFIED)
                await ws.send_text(_out(f"❌ Insufficient {pending['currency']} balance.", "idle"))
                return True

            setattr(sender, f"balance_{pending['currency'].lower()}", bal - pending["amount"])

            rr        = await db.execute(select(User).where(User.phone == pending["recipient"]))
            recipient = rr.scalar_one_or_none()
            if recipient:
                rec_bal = getattr(recipient, f"balance_{pending['currency'].lower()}", 0)
                setattr(recipient, f"balance_{pending['currency'].lower()}", rec_bal + pending["net"])

            ref = "QRK_S_" + uuid.uuid4().hex[:8].upper()
            db.add(Transaction(
                user_phone=phone, tx_type="crypto_send",
                currency=pending["currency"], amount=pending["amount"],
                fee=pending["fee"], fee_pct=FEE_SEND,
                status="completed", reference=ref,
            ))
            await db.commit()

        await clear_pending(phone, "send")
        await set_state(phone, State.VERIFIED)
        await ws.send_text(_out(
            f"✅ Sent {_crypto(pending['net'], pending['currency'])} to {pending['recipient']}!\nRef: {ref}",
            "done"
        ))
        return True

    return False


# ── WebSocket endpoint ────────────────────────────────────────────────────────

@router.websocket("/ws/trade")
async def trade_ws(websocket: WebSocket):
    token = websocket.query_params.get("token")
    phone = None

    try:
        payload = jwt.decode(token, SECRET, algorithms=[ALGO])
        phone   = payload.get("phone")
        if not phone:
            raise ValueError("no phone")
    except Exception:
        await websocket.close(code=4001)
        return

    await websocket.accept()

    async with AsyncSessionLocal() as db:
        u = await _user(db, phone)

    if not u:
        await websocket.send_text(_out("Account not found.", "idle"))
        await websocket.close()
        return

    first = (u.name or "").split()[0] if u.name else "there"
    await websocket.send_text(_out(
        f"👋 Welcome back, {first}!\n\n"
        f"Type anything to get started.\n"
        f"Examples: sell 100 USDT  ·  market  ·  portfolio",
        "idle"
    ))

    try:
        while True:
            raw  = await websocket.receive_text()
            text = raw.strip()
            if not text:
                continue

            state = await get_state(phone)

            if state == State.FROZEN:
                await websocket.send_text(_out("🚫 Account frozen. Contact support.", "frozen"))
                continue

            # Delegate to pending-state handler first
            if await _handle_pending(websocket, phone, state, text):
                continue

            # Parse intent from idle state
            intent = await parse_intent(text, phone)
            action = intent.get("action", "help")

            if action == "sell":
                await _sell(websocket, phone, intent)
            elif action == "buy":
                await _buy(websocket, phone, intent)
            elif action in ("send_crypto", "send"):
                await _send(websocket, phone, intent)
            elif action in ("market", "rate"):
                await websocket.send_text(_out(await market_message(), "idle"))
            elif action in ("portfolio", "balance"):
                await _portfolio(websocket, phone)
            elif action == "history":
                await _history(websocket, phone)
            elif action == "create_pool":
                await _create_pool(websocket, phone, intent)
            elif action == "join_pool":
                await _join_pool(websocket, phone, intent)
            elif action in ("watch_price", "price_alert"):
                await _watch_price(websocket, phone, intent)
            elif action == "my_alerts":
                await _my_alerts(websocket, phone)
            elif action == "refer":
                await _refer(websocket, phone)
            elif action == "chat":
                reply = intent.get("chat_reply") or "How can I help? Type 'help' for a list of commands."
                await websocket.send_text(_out(reply, "idle"))
            else:
                await websocket.send_text(_out(_help(), "idle"))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[WS error] {phone}: {e}")
        try:
            await websocket.send_text(_out("⚠️ Something went wrong. Please reconnect.", "idle"))
        except Exception:
            pass
