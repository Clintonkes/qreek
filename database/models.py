"""
Shared database models — identical schema to qreek_finance.
Both apps connect to the same Supabase PostgreSQL database.
"""
from sqlalchemy import Column, String, Float, Boolean, Integer, DateTime, ForeignKey
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
