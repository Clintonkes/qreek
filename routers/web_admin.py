import logging
import os
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from sqlalchemy import select, func, and_, or_, text, desc
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import (
    Company, FiatPool, PaymentLink, Transaction, User, UserSecurity
)
from database.session import get_db

logger = logging.getLogger(__name__)

ADMIN_JWT_SECRET = os.getenv("ADMIN_JWT_SECRET", "qreek-admin-dev-change-me")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ALGO = "HS256"
ADMIN_TOKEN_HOURS = 8

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])
_bearer = HTTPBearer()


def _get_admin_password() -> str:
    pw = os.getenv("ADMIN_PASSWORD")
    if not pw:
        raise HTTPException(status_code=503, detail="Admin credentials not configured.")
    return pw


def _create_admin_token(username: str) -> str:
    now = datetime.utcnow()
    payload = {
        "sub": username,
        "role": "admin",
        "iat": now,
        "exp": now + timedelta(hours=ADMIN_TOKEN_HOURS),
    }
    return jwt.encode(payload, ADMIN_JWT_SECRET, algorithm=ALGO)


async def require_admin(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    try:
        claims = jwt.decode(creds.credentials, ADMIN_JWT_SECRET, algorithms=[ALGO])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired admin token.")
    if claims.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required.")
    return claims


# ── Auth ─────────────────────────────────────────────────────────────────────

class LoginIn(BaseModel):
    username: str
    password: str


@router.post("/auth/login")
async def admin_login(body: LoginIn):
    admin_pw = _get_admin_password()
    username_ok = secrets.compare_digest(body.username, ADMIN_USERNAME)
    password_ok = secrets.compare_digest(body.password, admin_pw)
    if not username_ok or not password_ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin credentials.")
    token = _create_admin_token(body.username)
    logger.info("admin.login: username=%s", body.username)
    return {"token": token, "admin": body.username}


# ── Stats ─────────────────────────────────────────────────────────────────────

@router.get("/stats")
async def admin_stats(
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    today = datetime.utcnow()
    today_start = datetime(today.year, today.month, today.day)

    user_total = (await db.execute(select(func.count()).select_from(User))).scalar_one() or 0
    user_today = (await db.execute(
        select(func.count()).select_from(User).where(User.created_at >= today_start)
    )).scalar_one() or 0

    tx_total = (await db.execute(select(func.count()).select_from(Transaction))).scalar_one() or 0
    tx_volume = (await db.execute(select(func.sum(Transaction.ngn_amount)).select_from(Transaction))).scalar_one() or 0.0
    tx_today_count = (await db.execute(
        select(func.count()).select_from(Transaction).where(Transaction.created_at >= today_start)
    )).scalar_one() or 0
    tx_today_volume = (await db.execute(
        select(func.sum(Transaction.ngn_amount)).select_from(Transaction).where(Transaction.created_at >= today_start)
    )).scalar_one() or 0.0

    pl_total = (await db.execute(select(func.count()).select_from(PaymentLink))).scalar_one() or 0
    pl_active = (await db.execute(
        select(func.count()).select_from(PaymentLink).where(PaymentLink.is_active == True)
    )).scalar_one() or 0
    pl_collected = (await db.execute(select(func.sum(PaymentLink.total_collected)).select_from(PaymentLink))).scalar_one() or 0.0

    co_total = (await db.execute(select(func.count()).select_from(Company))).scalar_one() or 0
    co_verified = (await db.execute(
        select(func.count()).select_from(Company).where(Company.is_verified == True)
    )).scalar_one() or 0

    frozen_count = (await db.execute(
        select(func.count()).select_from(UserSecurity).where(UserSecurity.account_frozen == True)
    )).scalar_one() or 0

    return {
        "users": {"total": user_total, "today": user_today},
        "transactions": {
            "total": tx_total,
            "volume_ngn": round(float(tx_volume), 2),
            "today_count": tx_today_count,
            "today_volume": round(float(tx_today_volume), 2),
        },
        "payment_links": {
            "total": pl_total,
            "active": pl_active,
            "total_collected": round(float(pl_collected), 2),
        },
        "companies": {"total": co_total, "verified": co_verified},
        "frozen_accounts": frozen_count,
    }


# ── Users ─────────────────────────────────────────────────────────────────────

@router.get("/users")
async def admin_list_users(
    page: int = 1,
    limit: int = 25,
    search: Optional[str] = None,
    frozen: Optional[str] = None,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    page = max(page, 1)
    limit = min(max(limit, 1), 100)
    offset = (page - 1) * limit

    base = (
        select(User, UserSecurity)
        .outerjoin(UserSecurity, User.phone == UserSecurity.phone)
    )

    filters = []
    if search:
        pattern = f"%{search}%"
        filters.append(or_(User.phone.ilike(pattern), User.name.ilike(pattern)))
    if frozen is not None:
        want_frozen = frozen.lower() == "true"
        filters.append(UserSecurity.account_frozen == want_frozen)

    if filters:
        base = base.where(and_(*filters))

    count_q = select(func.count()).select_from(
        select(User).outerjoin(UserSecurity, User.phone == UserSecurity.phone)
        .where(and_(*filters) if filters else True)
        .subquery()
    )
    total = (await db.execute(count_q)).scalar_one() or 0

    result = await db.execute(base.order_by(desc(User.created_at)).offset(offset).limit(limit))
    rows = result.all()

    users = []
    for u, s in rows:
        users.append({
            "phone": u.phone,
            "name": u.name,
            "bank_account": u.bank_account,
            "bank_name": u.bank_name,
            "kyc_verified": u.kyc_verified,
            "is_merchant": u.is_merchant,
            "account_frozen": s.account_frozen if s else False,
            "failed_pin_count": s.failed_pin_count if s else 0,
            "last_active": s.last_active.isoformat() if s and s.last_active else None,
            "created_at": u.created_at.isoformat() if u.created_at else None,
        })

    pages = (total + limit - 1) // limit if limit else 1
    return {"users": users, "total": total, "page": page, "pages": pages}


@router.get("/users/{phone}")
async def admin_get_user(
    phone: str,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    u_result = await db.execute(select(User).where(User.phone == phone))
    user = u_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    s_result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec = s_result.scalar_one_or_none()

    tx_result = await db.execute(
        select(Transaction)
        .where(Transaction.user_phone == phone)
        .order_by(desc(Transaction.created_at))
        .limit(20)
    )
    txs = tx_result.scalars().all()

    pl_count_result = await db.execute(
        select(func.count()).select_from(PaymentLink)
        .where(PaymentLink.created_by == phone, PaymentLink.is_active == True)
    )
    active_links = pl_count_result.scalar_one() or 0

    co_result = await db.execute(select(Company).where(Company.owner_phone == phone))
    company = co_result.scalar_one_or_none()

    return {
        "phone": user.phone,
        "name": user.name,
        "balance_ngn": user.balance_ngn,
        "balance_usdt": user.balance_usdt,
        "bank_account": user.bank_account,
        "bank_code": user.bank_code,
        "bank_name": user.bank_name,
        "kyc_verified": user.kyc_verified,
        "is_merchant": user.is_merchant,
        "onboarding_done": user.onboarding_done,
        "referral_code": user.referral_code,
        "referred_by": user.referred_by,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "security": {
            "recovery_email": sec.recovery_email if sec else None,
            "bvn_verified": sec.bvn_verified if sec else False,
            "account_frozen": sec.account_frozen if sec else False,
            "failed_pin_count": sec.failed_pin_count if sec else 0,
            "last_active": sec.last_active.isoformat() if sec and sec.last_active else None,
        } if sec else None,
        "recent_transactions": [
            {
                "id": tx.id,
                "tx_type": tx.tx_type,
                "currency": tx.currency,
                "amount": tx.amount,
                "ngn_amount": tx.ngn_amount,
                "status": tx.status,
                "reference": tx.reference,
                "created_at": tx.created_at.isoformat() if tx.created_at else None,
            }
            for tx in txs
        ],
        "active_payment_links": active_links,
        "company": {
            "id": company.id,
            "name": company.name,
            "is_verified": company.is_verified,
            "employee_count": company.employee_count,
            "total_paid_ngn": company.total_paid_ngn,
        } if company else None,
    }


@router.patch("/users/{phone}/freeze")
async def admin_freeze_user(
    phone: str,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec = result.scalar_one_or_none()
    if not sec:
        raise HTTPException(status_code=404, detail="User security record not found.")
    sec.account_frozen = True
    await db.commit()
    logger.info("admin.freeze_account: phone=%s", phone)
    return {"message": f"Account {phone} has been frozen."}


@router.patch("/users/{phone}/unfreeze")
async def admin_unfreeze_user(
    phone: str,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(UserSecurity).where(UserSecurity.phone == phone))
    sec = result.scalar_one_or_none()
    if not sec:
        raise HTTPException(status_code=404, detail="User security record not found.")
    sec.account_frozen = False
    sec.failed_pin_count = 0
    await db.commit()
    logger.info("admin.unfreeze_account: phone=%s", phone)
    return {"message": f"Account {phone} has been unfrozen."}


# ── Transactions ──────────────────────────────────────────────────────────────

@router.get("/transactions")
async def admin_list_transactions(
    page: int = 1,
    limit: int = 25,
    status: Optional[str] = None,
    tx_type: Optional[str] = None,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    page = max(page, 1)
    limit = min(max(limit, 1), 100)
    offset = (page - 1) * limit

    filters = []
    if status:
        filters.append(Transaction.status == status)
    if tx_type:
        filters.append(Transaction.tx_type == tx_type)

    where_clause = and_(*filters) if filters else True

    total = (await db.execute(
        select(func.count()).select_from(Transaction).where(where_clause)
    )).scalar_one() or 0

    result = await db.execute(
        select(Transaction)
        .where(where_clause)
        .order_by(desc(Transaction.created_at))
        .offset(offset)
        .limit(limit)
    )
    txs = result.scalars().all()

    pages = (total + limit - 1) // limit if limit else 1
    return {
        "transactions": [
            {
                "id": tx.id,
                "user_phone": tx.user_phone,
                "tx_type": tx.tx_type,
                "currency": tx.currency,
                "amount": tx.amount,
                "ngn_amount": tx.ngn_amount,
                "gross_amount": tx.gross_amount,
                "qreek_fee": tx.qreek_fee,
                "provider_fee": tx.provider_fee,
                "net_amount": tx.net_amount,
                "rate": tx.rate,
                "fee": tx.fee,
                "status": tx.status,
                "provider": tx.provider,
                "reference": tx.reference,
                "tx_ref": tx.tx_ref,
                "payout_status": tx.payout_status,
                "pool_id": tx.pool_id,
                "payer_name": tx.payer_name,
                "payer_phone": tx.payer_phone,
                "payment_description": tx.payment_description,
                "created_at": tx.created_at.isoformat() if tx.created_at else None,
            }
            for tx in txs
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


# ── Payment Links ─────────────────────────────────────────────────────────────

@router.get("/payment-links")
async def admin_list_payment_links(
    page: int = 1,
    limit: int = 25,
    active: Optional[str] = None,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    page = max(page, 1)
    limit = min(max(limit, 1), 100)
    offset = (page - 1) * limit

    filters = []
    if active is not None:
        filters.append(PaymentLink.is_active == (active.lower() == "true"))

    where_clause = and_(*filters) if filters else True

    total = (await db.execute(
        select(func.count()).select_from(PaymentLink).where(where_clause)
    )).scalar_one() or 0

    result = await db.execute(
        select(PaymentLink)
        .where(where_clause)
        .order_by(desc(PaymentLink.created_at))
        .offset(offset)
        .limit(limit)
    )
    links = result.scalars().all()

    pages = (total + limit - 1) // limit if limit else 1
    return {
        "links": [
            {
                "id": l.id,
                "code": l.code,
                "title": l.title,
                "created_by": l.created_by,
                "bank_name": l.bank_name,
                "amount": l.amount,
                "is_flexible": l.is_flexible,
                "total_collected": l.total_collected,
                "use_count": l.use_count,
                "is_active": l.is_active,
                "expires_at": l.expires_at.isoformat() if l.expires_at else None,
                "flutterwave_subaccount_id": l.flutterwave_subaccount_id,
                "created_at": l.created_at.isoformat() if l.created_at else None,
            }
            for l in links
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


@router.patch("/payment-links/{link_id}/deactivate")
async def admin_deactivate_link(
    link_id: str,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(PaymentLink).where(PaymentLink.id == link_id))
    link = result.scalar_one_or_none()
    if not link:
        raise HTTPException(status_code=404, detail="Payment link not found.")
    link.is_active = False
    await db.commit()
    logger.info("admin.deactivate_link: link_id=%s", link_id)
    return {"message": f"Payment link {link_id} deactivated."}


@router.patch("/payment-links/{link_id}/activate")
async def admin_activate_link(
    link_id: str,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(PaymentLink).where(PaymentLink.id == link_id))
    link = result.scalar_one_or_none()
    if not link:
        raise HTTPException(status_code=404, detail="Payment link not found.")
    link.is_active = True
    await db.commit()
    logger.info("admin.activate_link: link_id=%s", link_id)
    return {"message": f"Payment link {link_id} activated."}


# ── Companies ─────────────────────────────────────────────────────────────────

@router.get("/companies")
async def admin_list_companies(
    page: int = 1,
    limit: int = 25,
    verified: Optional[str] = None,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    page = max(page, 1)
    limit = min(max(limit, 1), 100)
    offset = (page - 1) * limit

    filters = []
    if verified is not None:
        filters.append(Company.is_verified == (verified.lower() == "true"))

    where_clause = and_(*filters) if filters else True

    total = (await db.execute(
        select(func.count()).select_from(Company).where(where_clause)
    )).scalar_one() or 0

    result = await db.execute(
        select(Company)
        .where(where_clause)
        .order_by(desc(Company.created_at))
        .offset(offset)
        .limit(limit)
    )
    companies = result.scalars().all()

    pages = (total + limit - 1) // limit if limit else 1
    return {
        "companies": [
            {
                "id": c.id,
                "name": c.name,
                "owner_phone": c.owner_phone,
                "industry": c.industry,
                "rc_number": c.rc_number,
                "email": c.email,
                "employee_count": c.employee_count,
                "total_paid_ngn": c.total_paid_ngn,
                "wallet_balance_ngn": c.wallet_balance_ngn,
                "is_verified": c.is_verified,
                "created_at": c.created_at.isoformat() if c.created_at else None,
            }
            for c in companies
        ],
        "total": total,
        "page": page,
        "pages": pages,
    }


@router.patch("/companies/{company_id}/verify")
async def admin_verify_company(
    company_id: str,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Company).where(Company.id == company_id))
    company = result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found.")
    company.is_verified = True
    await db.commit()
    logger.info("admin.verify_company: company_id=%s", company_id)
    return {"message": f"Company {company_id} verified."}


@router.patch("/companies/{company_id}/unverify")
async def admin_unverify_company(
    company_id: str,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Company).where(Company.id == company_id))
    company = result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found.")
    company.is_verified = False
    await db.commit()
    logger.info("admin.unverify_company: company_id=%s", company_id)
    return {"message": f"Company {company_id} unverified."}


# ── Pools ─────────────────────────────────────────────────────────────────────

@router.get("/pools")
async def admin_list_pools(
    page: int = 1,
    limit: int = 25,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    page = max(page, 1)
    limit = min(max(limit, 1), 100)
    offset = (page - 1) * limit

    total = (await db.execute(select(func.count()).select_from(FiatPool))).scalar_one() or 0

    result = await db.execute(
        select(FiatPool)
        .order_by(desc(FiatPool.created_at))
        .offset(offset)
        .limit(limit)
    )
    pools = result.scalars().all()

    return {
        "pools": [
            {
                "id": p.id,
                "name": p.name,
                "creator_phone": p.creator_phone,
                "total_contributed": p.total_contributed,
                "total_volume": p.total_volume,
                "total_fees": p.total_fees,
                "member_count": p.member_count,
                "is_active": p.is_active,
                "created_at": p.created_at.isoformat() if p.created_at else None,
            }
            for p in pools
        ],
        "total": total,
    }


# ── Audit Log ─────────────────────────────────────────────────────────────────

@router.get("/audit-log")
async def admin_audit_log(
    page: int = 1,
    limit: int = 50,
    event_type: Optional[str] = None,
    _: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    page = max(page, 1)
    limit = min(max(limit, 1), 200)
    offset = (page - 1) * limit

    where = "WHERE event_type = :event_type" if event_type else ""

    count_sql = text(f"SELECT COUNT(*) FROM audit_log {where}")
    count_params = {"event_type": event_type} if event_type else {}
    total = (await db.execute(count_sql, count_params)).scalar_one() or 0

    rows_sql = text(
        f"SELECT id, user_phone, event_type, ip_address, user_agent, detail, created_at "
        f"FROM audit_log {where} ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
    )
    rows_params = {**count_params, "limit": limit, "offset": offset}
    rows = (await db.execute(rows_sql, rows_params)).mappings().all()

    entries = [
        {
            "id": r["id"],
            "user_phone": r["user_phone"],
            "event_type": r["event_type"],
            "ip_address": r["ip_address"],
            "user_agent": r["user_agent"],
            "detail": r["detail"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]

    pages = (total + limit - 1) // limit if limit else 1
    return {"entries": entries, "total": total, "page": page, "pages": pages}
