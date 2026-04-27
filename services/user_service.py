from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from database.models import User, UserSecurity, Referral


async def get_or_create_user(db: AsyncSession, phone: str) -> User:
    result = await db.execute(select(User).where(User.phone == phone))
    user   = result.scalar_one_or_none()
    if not user:
        user = User(phone=phone)
        sec  = UserSecurity(phone=phone)
        db.add(user)
        db.add(sec)
        await db.commit()
        await db.refresh(user)
    return user


async def save_bank(db: AsyncSession, phone: str, account: str, code: str, name: str) -> User:
    result = await db.execute(select(User).where(User.phone == phone))
    user   = result.scalar_one_or_none()
    if user:
        user.bank_account = account
        user.bank_code    = code
        user.bank_name    = name
        await db.commit()
    return user


async def apply_referral(db: AsyncSession, new_phone: str, referral_code: str):
    result   = await db.execute(select(User).where(User.referral_code == referral_code))
    referrer = result.scalar_one_or_none()
    if referrer and referrer.phone != new_phone:
        ref = Referral(referrer_phone=referrer.phone, referred_phone=new_phone)
        db.add(ref)
        result2   = await db.execute(select(User).where(User.phone == new_phone))
        new_user  = result2.scalar_one_or_none()
        if new_user:
            new_user.referred_by = referrer.phone
        await db.commit()
    return referrer


async def check_pool_membership(db: AsyncSession, phone: str) -> bool:
    from database.models import PoolMember
    result = await db.execute(select(PoolMember).where(PoolMember.user_phone == phone))
    return result.scalar_one_or_none() is not None
