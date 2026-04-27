from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from database.session import get_db
from database.models import User, Transaction
from core.web_jwt import decode_token
from core.rate_engine import get_all_rates

router = APIRouter(prefix="/api/v1/wallet", tags=["wallet"])


def _mask(account: str | None) -> str | None:
    if not account or len(account) < 4:
        return account
    return "****" + account[-4:]


@router.get("/balances")
async def get_balances(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone  = claims["phone"]
    result = await db.execute(select(User).where(User.phone == phone))
    user   = result.scalar_one_or_none()
    if not user:
        return {"balances": {}}
    return {
        "balances": {
            "NGN":  user.balance_ngn,
            "USDT": user.balance_usdt,
            "USDC": user.balance_usdc,
            "BTC":  user.balance_btc,
            "ETH":  user.balance_eth,
            "BNB":  user.balance_bnb,
            "SOL":  user.balance_sol,
        }
    }


@router.get("/history")
async def get_history(
    page:   int = Query(default=1, ge=1),
    limit:  int = Query(default=20, ge=1, le=100),
    claims: dict = Depends(decode_token),
    db:     AsyncSession = Depends(get_db),
):
    phone  = claims["phone"]
    offset = (page - 1) * limit
    result = await db.execute(
        select(Transaction)
        .where(Transaction.user_phone == phone)
        .order_by(desc(Transaction.created_at))
        .offset(offset)
        .limit(limit)
    )
    txns = result.scalars().all()
    return {
        "transactions": [
            {
                "id":          t.id,
                "tx_type":     t.tx_type,
                "currency":    t.currency,
                "amount":      t.amount,
                "ngn_amount":  t.ngn_amount,
                "fee":         t.fee,
                "fee_pct":     t.fee_pct,
                "status":      t.status,
                "reference":   t.reference,
                "bank_name":   t.bank_name,
                "bank_account":_mask(t.bank_account),
                "created_at":  t.created_at.isoformat() if t.created_at else None,
            }
            for t in txns
        ],
        "page":     page,
        "limit":    limit,
        "has_more": len(txns) == limit,
    }


@router.get("/portfolio-value")
async def get_portfolio_value(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone  = claims["phone"]
    result = await db.execute(select(User).where(User.phone == phone))
    user   = result.scalar_one_or_none()
    if not user:
        return {"total_ngn": 0, "breakdown": {}}

    rates = await get_all_rates("NGN")
    coins = {
        "USDT": user.balance_usdt,
        "USDC": user.balance_usdc,
        "BTC":  user.balance_btc,
        "ETH":  user.balance_eth,
        "BNB":  user.balance_bnb,
        "SOL":  user.balance_sol,
    }

    breakdown = {"NGN": {"balance": user.balance_ngn, "ngn_value": user.balance_ngn, "rate": 1}}
    total     = user.balance_ngn

    for coin, bal in coins.items():
        rate    = rates.get(coin, {}).get("rate", 0)
        ngn_val = bal * rate
        breakdown[coin] = {"balance": bal, "ngn_value": round(ngn_val, 2), "rate": rate}
        total += ngn_val

    return {"total_ngn": round(total, 2), "breakdown": breakdown}
