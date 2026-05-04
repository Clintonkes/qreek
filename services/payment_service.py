from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import User


async def debit_ngn_or_reject(db: AsyncSession, phone: str, amount: float) -> User:
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Debit amount must be greater than zero.")

    result = await db.execute(select(User).where(User.phone == phone).with_for_update())
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    if (user.balance_ngn or 0) < amount:
        raise HTTPException(status_code=402, detail="Insufficient NGN balance for this payment.")

    user.balance_ngn = round((user.balance_ngn or 0) - amount, 2)
    return user


async def refund_ngn(db: AsyncSession, phone: str, amount: float) -> None:
    if amount <= 0:
        return

    result = await db.execute(select(User).where(User.phone == phone).with_for_update())
    user = result.scalar_one_or_none()
    if user:
        user.balance_ngn = round((user.balance_ngn or 0) + amount, 2)
