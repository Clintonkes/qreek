from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import User, Company


async def debit_ngn_or_reject(db: AsyncSession, phone: str, amount: float) -> User:
    """
    Attempts to debit a specified amount from a user's NGN balance.
    Uses 'with_for_update' to lock the user row for atomic updates.
    Raises HTTPException if balance is insufficient or user is not found.
    """
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
    """
    Refunds a specified amount to a user's NGN balance.
    Uses 'with_for_update' to ensure consistency during the update.
    """
    if amount <= 0:
        return

    result = await db.execute(select(User).where(User.phone == phone).with_for_update())
    user = result.scalar_one_or_none()
    if user:
        user.balance_ngn = round((user.balance_ngn or 0) + amount, 2)


async def debit_company_wallet_or_reject(db: AsyncSession, company_id: str, amount: float) -> Company:
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Debit amount must be greater than zero.")
    result = await db.execute(select(Company).where(Company.id == company_id).with_for_update())
    co = result.scalar_one_or_none()
    if not co:
        raise HTTPException(status_code=404, detail="Company not found.")
    if (co.wallet_balance_ngn or 0) < amount:
        raise HTTPException(status_code=402, detail="Insufficient company wallet balance for this payment. Please deposit funds.")
    co.wallet_balance_ngn = round((co.wallet_balance_ngn or 0) - amount, 2)
    return co


async def refund_company_wallet(db: AsyncSession, company_id: str, amount: float) -> None:
    if amount <= 0:
        return
    result = await db.execute(select(Company).where(Company.id == company_id).with_for_update())
    co = result.scalar_one_or_none()
    if co:
        co.wallet_balance_ngn = round((co.wallet_balance_ngn or 0) + amount, 2)
