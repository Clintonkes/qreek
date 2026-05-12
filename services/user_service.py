from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from database.models import User, UserSecurity, Referral


async def get_or_create_user(db: AsyncSession, phone: str) -> User:
    """
    Retrieves an existing user by their phone number or creates a new user if not found.
    Also initializes the UserSecurity entry for new users.
    """
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
    """
    Updates a user's bank account details (account number, bank code, and bank name).
    """
    result = await db.execute(select(User).where(User.phone == phone))
    user   = result.scalar_one_or_none()
    if user:
        user.bank_account = account
        user.bank_code    = code
        user.bank_name    = name
        await db.commit()
    return user


async def apply_referral(db: AsyncSession, new_phone: str, referral_code: str):
    """
    Applies a referral code to a new user. 
    Links the new user to the referrer and records the referral in the database.
    """
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
    """
    Checks if a user is a member of any investment pool.
    """
    from database.models import PoolMember
    result = await db.execute(select(PoolMember).where(PoolMember.user_phone == phone))
    return result.scalar_one_or_none() is not None
