from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from database.models import Base
from dotenv import load_dotenv
import os, uuid

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set. Please configure it in your Railway project variables.")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)


engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    poolclass=NullPool,
    connect_args={
        "statement_cache_size": 0,
        "prepared_statement_cache_size": 0,
        "prepared_statement_name_func": lambda: f"__stmt_{uuid.uuid4().hex}__",
    },
)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db():
    """
    Initializes the database by creating all tables defined in the SQLAlchemy models.
    """
    async with engine.begin() as conn:
        await conn.exec_driver_sql("SELECT pg_advisory_lock(774411)")
        try:
            await conn.run_sync(Base.metadata.create_all)
            await _ensure_ledger_columns(conn)
        finally:
            await conn.exec_driver_sql("SELECT pg_advisory_unlock(774411)")
    print("Database ready.")


async def _ensure_ledger_columns(conn):
    """
    Idempotently adds explicit payment ledger columns to existing databases.

    create_all() creates missing tables but does not alter already-created
    tables, so Railway/Supabase deployments need these ADD COLUMN IF NOT EXISTS
    statements before the API can insert the new ledger fields.
    """
    statements = [
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS gross_amount DOUBLE PRECISION",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS qreek_fee DOUBLE PRECISION DEFAULT 0.0",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS provider_fee DOUBLE PRECISION DEFAULT 0.0",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS provider_settled_amount DOUBLE PRECISION",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS net_amount DOUBLE PRECISION",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS tx_ref VARCHAR",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS provider_transaction_id VARCHAR",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS provider_checkout_url VARCHAR",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS payment_description VARCHAR",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS payer_name VARCHAR",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS payer_phone VARCHAR",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS payout_status VARCHAR",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS payout_reference VARCHAR",
        "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS payout_error TEXT",
        "CREATE INDEX IF NOT EXISTS ix_transactions_reference ON transactions (reference)",
        "CREATE INDEX IF NOT EXISTS ix_transactions_tx_ref ON transactions (tx_ref)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_idempotency_key ON transactions (idempotency_key) WHERE idempotency_key IS NOT NULL",
        "CREATE TABLE IF NOT EXISTS payment_events (id VARCHAR PRIMARY KEY, provider VARCHAR DEFAULT 'flutterwave', reference VARCHAR, transaction_id VARCHAR, event_type VARCHAR NOT NULL, status VARCHAR, message TEXT, payload JSON, created_at TIMESTAMP WITHOUT TIME ZONE)",
        "CREATE INDEX IF NOT EXISTS ix_payment_events_reference ON payment_events (reference)",
        "CREATE INDEX IF NOT EXISTS ix_payment_events_created_at ON payment_events (created_at)",
        "ALTER TABLE payment_links ADD COLUMN IF NOT EXISTS flutterwave_subaccount_id VARCHAR",
        "ALTER TABLE payment_links ADD COLUMN IF NOT EXISTS flutterwave_subaccount_status VARCHAR",
        "ALTER TABLE payment_links ADD COLUMN IF NOT EXISTS flutterwave_subaccount_error TEXT",
        "CREATE INDEX IF NOT EXISTS ix_payment_links_flutterwave_subaccount_id ON payment_links (flutterwave_subaccount_id)",
        "ALTER TABLE pool_transactions ADD COLUMN IF NOT EXISTS gross_amount DOUBLE PRECISION",
        "ALTER TABLE pool_transactions ADD COLUMN IF NOT EXISTS qreek_fee DOUBLE PRECISION DEFAULT 0.0",
        "ALTER TABLE pool_transactions ADD COLUMN IF NOT EXISTS provider_fee DOUBLE PRECISION DEFAULT 0.0",
        "ALTER TABLE pool_transactions ADD COLUMN IF NOT EXISTS tx_ref VARCHAR",
        "ALTER TABLE pool_transactions ADD COLUMN IF NOT EXISTS provider_transaction_id VARCHAR",
        "ALTER TABLE pool_transactions ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR",
        "CREATE INDEX IF NOT EXISTS ix_pool_transactions_reference ON pool_transactions (reference)",
        "CREATE INDEX IF NOT EXISTS ix_pool_transactions_tx_ref ON pool_transactions (tx_ref)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_pool_transactions_idempotency_key ON pool_transactions (idempotency_key) WHERE idempotency_key IS NOT NULL",
        "ALTER TABLE payroll_entries ADD COLUMN IF NOT EXISTS qreek_fee DOUBLE PRECISION DEFAULT 0.0",
        "ALTER TABLE payroll_entries ADD COLUMN IF NOT EXISTS provider_fee DOUBLE PRECISION DEFAULT 0.0",
        "ALTER TABLE payroll_entries ADD COLUMN IF NOT EXISTS tx_ref VARCHAR",
        "ALTER TABLE payroll_entries ADD COLUMN IF NOT EXISTS provider_transaction_id VARCHAR",
        "ALTER TABLE payroll_entries ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR",
        "CREATE INDEX IF NOT EXISTS ix_payroll_entries_reference ON payroll_entries (reference)",
        "CREATE INDEX IF NOT EXISTS ix_payroll_entries_tx_ref ON payroll_entries (tx_ref)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_payroll_entries_idempotency_key ON payroll_entries (idempotency_key) WHERE idempotency_key IS NOT NULL",
    ]
    for statement in statements:
        await conn.exec_driver_sql(statement)


async def get_db():
    """
    Dependency function that provides an asynchronous database session.
    Ensures the session is rolled back on error and closed after use.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
