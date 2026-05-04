from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from database.models import UserSecurity

pwd_ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
MAX_PIN_ATTEMPTS = 5


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


async def verify_transaction_pin(db: AsyncSession, phone: str, pin: str) -> bool:
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec = result.scalar_one_or_none()
    if not sec or not sec.pin_hash or sec.account_frozen:
        return False

    ok = pwd_ctx.verify(pin, sec.pin_hash)
    if ok:
        sec.failed_pin_count = 0
        await db.commit()
        return True

    sec.failed_pin_count = (sec.failed_pin_count or 0) + 1
    if sec.failed_pin_count >= MAX_PIN_ATTEMPTS:
        sec.account_frozen = True
    await db.commit()
    return False


async def pin_attempts_remaining(db: AsyncSession, phone: str) -> int:
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec = result.scalar_one_or_none()
    if not sec:
        return 0
    return max(0, MAX_PIN_ATTEMPTS - (sec.failed_pin_count or 0))


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
