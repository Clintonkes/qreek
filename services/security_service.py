from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from database.models import UserSecurity

pwd_ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


async def set_pin(db: AsyncSession, phone: str, pin: str):
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    if not sec:
        sec = UserSecurity(phone=phone)
        db.add(sec)
    sec.pin_hash = pwd_ctx.hash(pin)
    await db.commit()


async def verify_pin(db: AsyncSession, phone: str, pin: str) -> bool:
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    if not sec or not sec.pin_hash:
        return False
    return pwd_ctx.verify(pin, sec.pin_hash)


async def is_frozen(db: AsyncSession, phone: str) -> bool:
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    return bool(sec and sec.account_frozen)


async def freeze_account(db: AsyncSession, phone: str):
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    if sec:
        sec.account_frozen = True
        await db.commit()


async def unfreeze_account(db: AsyncSession, phone: str):
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    if sec:
        sec.account_frozen   = False
        sec.failed_pin_count = 0
        await db.commit()
