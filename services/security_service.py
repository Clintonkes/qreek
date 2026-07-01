from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from database.models import UserSecurity, Company

pwd_ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
MAX_PIN_ATTEMPTS = 5


async def set_pin(db: AsyncSession, phone: str, pin: str):
    """
    Sets or updates the transaction PIN for a user identified by their phone number.
    Hashes the PIN before storing it in the database.
    """
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    if not sec:
        sec = UserSecurity(phone=phone)
        db.add(sec)
    sec.pin_hash = pwd_ctx.hash(pin)
    await db.commit()


async def verify_pin(db: AsyncSession, phone: str, pin: str) -> bool:
    """
    Verifies the provided PIN against the hashed PIN stored in the database for a user.
    Returns True if the PIN is correct, False otherwise.
    """
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    if not sec or not sec.pin_hash:
        return False
    return pwd_ctx.verify(pin, sec.pin_hash)


async def verify_transaction_pin(db: AsyncSession, phone: str, pin: str) -> bool:
    """
    Verifies a transaction PIN and manages failed attempts.
    If the PIN is correct, resets the failed attempt counter.
    If incorrect, increments the counter and freezes the account if the maximum attempts are reached.
    Returns False if the account is already frozen.
    """
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
    """
    Returns the number of remaining PIN attempts for a user before their account is frozen.
    """
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec = result.scalar_one_or_none()
    if not sec:
        return 0
    return max(0, MAX_PIN_ATTEMPTS - (sec.failed_pin_count or 0))


async def is_frozen(db: AsyncSession, phone: str) -> bool:
    """
    Checks if a user's account is currently frozen due to security concerns or failed PIN attempts.
    """
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    return bool(sec and sec.account_frozen)


async def freeze_account(db: AsyncSession, phone: str):
    """
    Manually freezes a user's account.
    """
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    if sec:
        sec.account_frozen = True
        await db.commit()


async def unfreeze_account(db: AsyncSession, phone: str):
    """
    Unfreezes a user's account and resets their failed PIN attempt counter.
    """
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec    = result.scalar_one_or_none()
    if sec:
        sec.account_frozen   = False
        sec.failed_pin_count = 0
        await db.commit()


async def set_company_payment_pin(db: AsyncSession, company_id: str, pin: str):
    """
    Sets or updates the payment/transaction PIN for a company.
    Hashes the PIN before storing it in the database.
    """
    result = await db.execute(select(Company).where(Company.id == company_id))
    co = result.scalar_one_or_none()
    if not co:
        return
    co.payment_pin_hash = pwd_ctx.hash(pin)
    await db.commit()


async def verify_company_payment_pin(db: AsyncSession, company_id: str, pin: str) -> bool:
    """
    Verifies the provided PIN against the company's payment PIN hash.
    Returns True if the PIN is correct, False otherwise.
    """
    result = await db.execute(select(Company).where(Company.id == company_id))
    co = result.scalar_one_or_none()
    if not co or not co.payment_pin_hash:
        return False
    return pwd_ctx.verify(pin, co.payment_pin_hash)
