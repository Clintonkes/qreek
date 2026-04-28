"""
Shared database models — identical schema to qreek_finance.
Both apps connect to the same Supabase PostgreSQL database.
Extended with enterprise payroll, fiat pool transactions, and payment links.
"""
from sqlalchemy import Column, String, Float, Boolean, Integer, DateTime, ForeignKey, Text, JSON
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime
import uuid

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    phone            = Column(String, primary_key=True)
    name             = Column(String, nullable=True)
    balance_ngn      = Column(Float, default=0.0)
    balance_usdt     = Column(Float, default=0.0)
    balance_usdc     = Column(Float, default=0.0)
    balance_btc      = Column(Float, default=0.0)
    balance_eth      = Column(Float, default=0.0)
    balance_bnb      = Column(Float, default=0.0)
    balance_sol      = Column(Float, default=0.0)
    bank_account     = Column(String, nullable=True)
    bank_code        = Column(String, nullable=True)
    bank_name        = Column(String, nullable=True)
    kyc_verified     = Column(Boolean, default=False)
    is_merchant      = Column(Boolean, default=False)
    onboarding_done  = Column(Boolean, default=False)
    referral_code    = Column(String, unique=True, default=lambda: uuid.uuid4().hex[:8].upper())
    referred_by      = Column(String, nullable=True)
    created_at       = Column(DateTime, default=datetime.utcnow)


class UserSecurity(Base):
    __tablename__        = "user_security"
    phone                = Column(String, ForeignKey("users.phone"), primary_key=True)
    pin_hash             = Column(String, nullable=True)
    recovery_email       = Column(String, nullable=True)
    bvn_verified         = Column(Boolean, default=False)
    account_frozen       = Column(Boolean, default=False)
    failed_pin_count     = Column(Integer, default=0)
    last_active          = Column(DateTime, default=datetime.utcnow)


class Transaction(Base):
    __tablename__   = "transactions"
    id              = Column(String, primary_key=True, default=lambda: "tx_" + uuid.uuid4().hex[:12])
    user_phone      = Column(String, ForeignKey("users.phone"))
    tx_type         = Column(String)
    currency        = Column(String, default="USDT")
    amount          = Column(Float)
    ngn_amount      = Column(Float, nullable=True)
    rate            = Column(Float, nullable=True)
    fee             = Column(Float, default=0.0)
    fee_pct         = Column(Float, default=0.0)
    status          = Column(String, default="pending")
    provider        = Column(String, nullable=True)
    reference       = Column(String, nullable=True)
    pool_id         = Column(String, nullable=True)
    in_pool         = Column(Boolean, default=False)
    bank_account    = Column(String, nullable=True)
    bank_code       = Column(String, nullable=True)
    bank_name       = Column(String, nullable=True)
    escrow_address  = Column(String, nullable=True)
    monitoring_id   = Column(String, nullable=True)
    created_at      = Column(DateTime, default=datetime.utcnow)


class Pool(Base):
    __tablename__         = "pools"
    id                    = Column(String, primary_key=True, default=lambda: "pool_" + uuid.uuid4().hex[:8].upper())
    name                  = Column(String)
    creator_phone         = Column(String, ForeignKey("users.phone"))
    invite_code           = Column(String, unique=True, default=lambda: uuid.uuid4().hex[:6].upper())
    pool_type             = Column(String, default="crypto")
    total_volume          = Column(Float, default=0.0)
    total_fees_collected  = Column(Float, default=0.0)
    member_count          = Column(Integer, default=1)
    is_active             = Column(Boolean, default=True)
    created_at            = Column(DateTime, default=datetime.utcnow)
    members               = relationship("PoolMember", back_populates="pool")


class PoolMember(Base):
    __tablename__ = "pool_members"
    id            = Column(String, primary_key=True, default=lambda: uuid.uuid4().hex)
    pool_id       = Column(String, ForeignKey("pools.id"))
    user_phone    = Column(String, ForeignKey("users.phone"))
    role          = Column(String, default="member")
    joined_at     = Column(DateTime, default=datetime.utcnow)
    pool          = relationship("Pool", back_populates="members")


class FiatPool(Base):
    __tablename__       = "fiat_pools"
    id                  = Column(String, primary_key=True, default=lambda: "fp_" + uuid.uuid4().hex[:8].upper())
    name                = Column(String)
    creator_phone       = Column(String, ForeignKey("users.phone"))
    invite_code         = Column(String, unique=True, default=lambda: uuid.uuid4().hex[:6].upper())
    balance_ngn         = Column(Float, default=0.0)
    total_contributed   = Column(Float, default=0.0)
    total_volume        = Column(Float, default=0.0)
    total_fees          = Column(Float, default=0.0)
    member_count        = Column(Integer, default=1)
    is_active           = Column(Boolean, default=True)
    created_at          = Column(DateTime, default=datetime.utcnow)


class FiatPoolMember(Base):
    __tablename__ = "fiat_pool_members"
    id            = Column(String, primary_key=True, default=lambda: uuid.uuid4().hex)
    pool_id       = Column(String, ForeignKey("fiat_pools.id"))
    user_phone    = Column(String, ForeignKey("users.phone"))
    role          = Column(String, default="member")
    joined_at     = Column(DateTime, default=datetime.utcnow)


class PriceAlert(Base):
    __tablename__  = "price_alerts"
    id             = Column(String, primary_key=True, default=lambda: uuid.uuid4().hex)
    user_phone     = Column(String, ForeignKey("users.phone"))
    currency       = Column(String)
    target_price   = Column(Float)
    direction      = Column(String)
    triggered      = Column(Boolean, default=False)
    created_at     = Column(DateTime, default=datetime.utcnow)


class Referral(Base):
    __tablename__   = "referrals"
    id              = Column(String, primary_key=True, default=lambda: uuid.uuid4().hex)
    referrer_phone  = Column(String, ForeignKey("users.phone"))
    referred_phone  = Column(String, ForeignKey("users.phone"))
    reward_paid     = Column(Boolean, default=False)
    created_at      = Column(DateTime, default=datetime.utcnow)


# ── Enterprise / Payroll models ───────────────────────────────────────────────

class Company(Base):
    """An organisation registered on Qreek for enterprise payroll/payments."""
    __tablename__       = "companies"
    id                  = Column(String, primary_key=True, default=lambda: "co_" + uuid.uuid4().hex[:10].upper())
    owner_phone         = Column(String, ForeignKey("users.phone"), nullable=False)
    name                = Column(String, nullable=False)
    industry            = Column(String, nullable=True)
    rc_number           = Column(String, nullable=True)   # CAC registration number
    email               = Column(String, nullable=True)
    address             = Column(String, nullable=True)
    logo_url            = Column(String, nullable=True)
    payment_pin_hash    = Column(String, nullable=True)   # separate payroll PIN if desired
    total_paid_ngn      = Column(Float, default=0.0)
    employee_count      = Column(Integer, default=0)
    is_verified         = Column(Boolean, default=False)  # admin-verified for higher limits
    created_at          = Column(DateTime, default=datetime.utcnow)
    employees           = relationship("Employee", back_populates="company", lazy="dynamic")
    payroll_runs        = relationship("PayrollRun", back_populates="company", lazy="dynamic")


class Employee(Base):
    """An employee on a company payroll roster."""
    __tablename__    = "employees"
    id               = Column(String, primary_key=True, default=lambda: "emp_" + uuid.uuid4().hex[:10])
    company_id       = Column(String, ForeignKey("companies.id"), nullable=False)
    name             = Column(String, nullable=False)
    email            = Column(String, nullable=True)
    phone            = Column(String, nullable=True)
    bank_account     = Column(String, nullable=False)
    bank_code        = Column(String, nullable=False)
    bank_name        = Column(String, nullable=False)
    department       = Column(String, nullable=True)
    job_title        = Column(String, nullable=True)
    salary           = Column(Float, nullable=False)   # base monthly salary in NGN
    is_active        = Column(Boolean, default=True)
    qreek_phone      = Column(String, nullable=True)   # linked Qreek account if any
    created_at       = Column(DateTime, default=datetime.utcnow)
    company          = relationship("Company", back_populates="employees")


class PayrollRun(Base):
    """A single payroll execution batch."""
    __tablename__    = "payroll_runs"
    id               = Column(String, primary_key=True, default=lambda: "pr_" + uuid.uuid4().hex[:10].upper())
    company_id       = Column(String, ForeignKey("companies.id"), nullable=False)
    initiated_by     = Column(String, ForeignKey("users.phone"), nullable=False)
    period_label     = Column(String, nullable=False)   # e.g. "April 2026"
    total_gross      = Column(Float, default=0.0)
    total_fee        = Column(Float, default=0.0)
    total_net        = Column(Float, default=0.0)
    entry_count      = Column(Integer, default=0)
    paid_count       = Column(Integer, default=0)
    failed_count     = Column(Integer, default=0)
    # pending | processing | completed | partial | failed
    status           = Column(String, default="pending")
    note             = Column(String, nullable=True)
    scheduled_at     = Column(DateTime, nullable=True)  # for scheduled payrolls
    completed_at     = Column(DateTime, nullable=True)
    created_at       = Column(DateTime, default=datetime.utcnow)
    company          = relationship("Company", back_populates="payroll_runs")
    entries          = relationship("PayrollEntry", back_populates="run", lazy="dynamic")


class PayrollEntry(Base):
    """A single employee payment within a payroll run."""
    __tablename__    = "payroll_entries"
    id               = Column(String, primary_key=True, default=lambda: "pe_" + uuid.uuid4().hex[:10])
    run_id           = Column(String, ForeignKey("payroll_runs.id"), nullable=False)
    employee_id      = Column(String, ForeignKey("employees.id"), nullable=False)
    employee_name    = Column(String, nullable=False)   # snapshot at time of payment
    bank_account     = Column(String, nullable=False)
    bank_code        = Column(String, nullable=False)
    bank_name        = Column(String, nullable=False)
    gross_amount     = Column(Float, nullable=False)
    fee              = Column(Float, default=0.0)
    net_amount       = Column(Float, nullable=False)
    # pending | processing | completed | failed
    status           = Column(String, default="pending")
    reference        = Column(String, nullable=True)
    provider         = Column(String, nullable=True)
    error_msg        = Column(String, nullable=True)
    paid_at          = Column(DateTime, nullable=True)
    created_at       = Column(DateTime, default=datetime.utcnow)
    run              = relationship("PayrollRun", back_populates="entries")


# ── Fiat pool payments ────────────────────────────────────────────────────────

class PoolTransaction(Base):
    """A facilitated NGN payment through a fiat pool. No funds held."""
    __tablename__          = "pool_transactions"
    id                     = Column(String, primary_key=True, default=lambda: "pt_" + uuid.uuid4().hex[:12])
    pool_id                = Column(String, nullable=False)   # FiatPool.id
    sender_phone           = Column(String, ForeignKey("users.phone"), nullable=False)
    recipient_name         = Column(String, nullable=True)
    recipient_phone        = Column(String, nullable=True)
    recipient_bank_account = Column(String, nullable=False)
    recipient_bank_code    = Column(String, nullable=False)
    recipient_bank_name    = Column(String, nullable=False)
    amount                 = Column(Float, nullable=False)
    fee                    = Column(Float, default=0.0)
    fee_pct                = Column(Float, default=0.003)
    net_amount             = Column(Float, nullable=False)
    status                 = Column(String, default="pending")
    reference              = Column(String, nullable=True)
    provider               = Column(String, nullable=True)
    note                   = Column(String, nullable=True)
    created_at             = Column(DateTime, default=datetime.utcnow)


class PaymentRequest(Base):
    """A payment request sent to pool members (ajo-style collection)."""
    __tablename__   = "payment_requests"
    id              = Column(String, primary_key=True, default=lambda: "req_" + uuid.uuid4().hex[:10])
    pool_id         = Column(String, nullable=False)
    requested_by    = Column(String, ForeignKey("users.phone"), nullable=False)
    title           = Column(String, nullable=False)
    amount          = Column(Float, nullable=False)
    note            = Column(String, nullable=True)
    due_date        = Column(DateTime, nullable=True)
    # active | completed | cancelled
    status          = Column(String, default="active")
    paid_count      = Column(Integer, default=0)
    total_collected = Column(Float, default=0.0)
    created_at      = Column(DateTime, default=datetime.utcnow)


class PaymentLink(Base):
    """A shareable payment link that anyone can pay — pool-aware or standalone."""
    __tablename__  = "payment_links"
    id             = Column(String, primary_key=True, default=lambda: uuid.uuid4().hex)
    code           = Column(String, unique=True, default=lambda: uuid.uuid4().hex[:8].upper())
    created_by     = Column(String, ForeignKey("users.phone"), nullable=False)
    pool_id        = Column(String, nullable=True)
    title          = Column(String, nullable=False)
    description    = Column(String, nullable=True)
    # Fixed amount or flexible (payer enters amount)
    amount         = Column(Float, nullable=True)
    is_flexible    = Column(Boolean, default=False)
    # Payout destination for the link creator
    bank_account   = Column(String, nullable=True)
    bank_code      = Column(String, nullable=True)
    bank_name      = Column(String, nullable=True)
    max_uses       = Column(Integer, nullable=True)
    use_count      = Column(Integer, default=0)
    total_collected= Column(Float, default=0.0)
    expires_at     = Column(DateTime, nullable=True)
    is_active      = Column(Boolean, default=True)
    created_at     = Column(DateTime, default=datetime.utcnow)


class AuditLog(Base):
    """Immutable audit trail for all sensitive financial actions."""
    __tablename__ = "audit_logs"
    id            = Column(String, primary_key=True, default=lambda: uuid.uuid4().hex)
    actor_phone   = Column(String, ForeignKey("users.phone"), nullable=False)
    action        = Column(String, nullable=False)   # e.g. "payroll_run_executed"
    entity_type   = Column(String, nullable=True)   # "payroll_run", "pool_transaction"
    entity_id     = Column(String, nullable=True)
    amount        = Column(Float, nullable=True)
    ip_address    = Column(String, nullable=True)
    metadata      = Column(JSON, nullable=True)
    created_at    = Column(DateTime, default=datetime.utcnow)
