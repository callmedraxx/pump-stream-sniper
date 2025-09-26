import os
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from .token import Base

load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL") or os.getenv("DATABASE_URL")

# Fail fast with helpful message when DATABASE_URL isn't provided or is empty.
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. Set SUPABASE_HOST (and POSTGRES_*) or provide a full DATABASE_URL in the environment."
    )

connect_args = {}
if "sslmode=require" in DATABASE_URL or "sslmode=prefer" in DATABASE_URL:
    connect_args["sslmode"] = "require"

# Allow tuning pool size via environment to match Supabase limits.
# Use conservative defaults to avoid exhausting provider-side pool limits when
# multiple worker processes are used. You can override via env when needed.
DEFAULT_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "40"))
# Allow configuring max_overflow via env, but default to a small value to
# avoid exhausting provider-side session limits when many worker processes
# are used. Operators can increase this deliberately if they understand
# the provider's pool semantics.
DEFAULT_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "5"))
DEFAULT_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
DEFAULT_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))

# Create engine with connection pooling
try:
    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_size=DEFAULT_POOL_SIZE,
        max_overflow=DEFAULT_MAX_OVERFLOW,
        pool_timeout=DEFAULT_POOL_TIMEOUT,
        pool_recycle=DEFAULT_POOL_RECYCLE,
        # Protect against stale connections
        pool_pre_ping=True,
        echo=False,
        # Always pass a dict (can be empty).
        connect_args=connect_args,
    )
except Exception as e:
    raise RuntimeError(f"Failed to create DB engine from DATABASE_URL: {e}") from e

# Semaphore to limit concurrent DB-using threads/tasks inside this process.
# Use a semaphore sized to pool_size so that we don't create more simultaneous
# DB sessions/threads than the DB server accepts.
from asyncio import Semaphore
DB_CONNECTION_SEMAPHORE = Semaphore(DEFAULT_POOL_SIZE)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency function to get database session
    Use this in FastAPI route dependencies
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        try:
            db.close()
        except Exception as e:
            print(f"⚠️  Warning: Error closing database session: {e}")


def get_connection_pool_status():
    """
    Get current status of the database connection pool
    Useful for monitoring and debugging connection issues
    """
    pool = engine.pool
    # Probe safely - QueuePool exposes these methods but be defensive
    try:
        size = pool.size()
    except Exception:
        size = None
    try:
        checkedin = pool.checkedin()
    except Exception:
        checkedin = None
    try:
        checkedout = pool.checkedout()
    except Exception:
        checkedout = None
    try:
        overflow = pool.overflow()
    except Exception:
        overflow = None
    # 'invalid' may not be present; if missing, approximate it non-negatively
    try:
        invalid = pool.invalid()
    except Exception:
        if None not in (size, checkedin, checkedout):
            # invalid ~= size - (checkedin + checkedout)
            try:
                invalid = max(0, (size or 0) - (checkedin or 0) - (checkedout or 0))
            except Exception:
                invalid = None
        else:
            invalid = None

    # Normalize overflow/invalid to avoid negative confusing values in logs
    try:
        if overflow is not None and isinstance(overflow, int) and overflow < 0:
            overflow = 0
    except Exception:
        pass
    try:
        if invalid is not None and isinstance(invalid, int) and invalid < 0:
            invalid = 0
    except Exception:
        pass

    return {
        "pool_size": size,
        "checkedin": checkedin,
        "checkedout": checkedout,
        "overflow": overflow,
        "invalid": invalid,
    }


def log_pool_status(logger, context: str = ""):
    """Log a concise snapshot of the connection pool state with context."""
    try:
        pool = engine.pool
        # Probe common pool metrics safely; some pool implementations expose
        # methods with different names or omit specific metrics.
        def _call_if_callable(obj, name):
            try:
                attr = getattr(obj, name)
            except Exception:
                return "N/A"
            try:
                return attr() if callable(attr) else attr
            except Exception:
                return "N/A"

        size = _call_if_callable(pool, "size")
        checkedout = _call_if_callable(pool, "checkedout")
        checkedin = _call_if_callable(pool, "checkedin")
        overflow = _call_if_callable(pool, "overflow")
        # 'invalid' isn't guaranteed; try 'invalid' then fallback to computation
        invalid = _call_if_callable(pool, "invalid")
        if invalid == "N/A":
            try:
                invalid = max(0, (size or 0) - (checkedin or 0) - (checkedout or 0))
            except Exception:
                invalid = "N/A"

        # Normalize negative values which can appear due to timing/race windows
        try:
            if isinstance(overflow, int) and overflow < 0:
                overflow = 0
        except Exception:
            pass
        try:
            if isinstance(invalid, int) and invalid < 0:
                invalid = 0
        except Exception:
            pass

        logger.info(
            "DB Pool Status%s: size=%s checkedout=%s checkedin=%s overflow=%s invalid=%s",
            (f' [{context}]' if context else ''),
            size,
            checkedout,
            checkedin,
            overflow,
            invalid,
        )
    except Exception as e:
        # Don't raise from logging helper; report and continue
        try:
            logger.warning("Failed to read DB pool status: %s", e)
        except Exception:
            print(f"Failed to read DB pool status: {e}")


def create_tables():
    """
    Create all database tables
    Call this once when the application starts
    """
    # Create any new tables (won't alter existing tables' columns)
    Base.metadata.create_all(bind=engine)

    # Try to ensure token columns exist (best-effort simple migration)
    try:
        ensure_token_columns()
    except Exception as e:
        print(f"⚠️ Warning: failed to ensure token columns exist: {e}")

    print("✅ Database tables created successfully")


def ensure_token_columns():
    """Ensure required columns exist on the tokens table (idempotent).

    This is a small, best-effort migration helper to add the `candle_data` JSON
    column if it's missing. It uses `ALTER TABLE ... IF NOT EXISTS` so it's safe
    to call repeatedly.
    """
    inspector = inspect(engine)
    if 'tokens' not in inspector.get_table_names():
        # Table isn't present yet; metadata.create_all should handle creation.
        return

    columns = {c['name'] for c in inspector.get_columns('tokens')}

    with engine.begin() as conn:
        if 'candle_data' not in columns:
            # Use IF NOT EXISTS for safety across Postgres versions
            conn.execute(text('ALTER TABLE tokens ADD COLUMN IF NOT EXISTS candle_data JSON'))
            print("✅ Added missing column: tokens.candle_data")
        # Add live_since timestamp if missing (tokens.live_since was added later)
        if 'live_since' not in columns:
            # Use TIMESTAMP WITH TIME ZONE for portability; fall back to TIMESTAMP if not supported
            try:
                conn.execute(text('ALTER TABLE tokens ADD COLUMN IF NOT EXISTS live_since TIMESTAMP WITH TIME ZONE'))
            except Exception:
                conn.execute(text('ALTER TABLE tokens ADD COLUMN IF NOT EXISTS live_since TIMESTAMP'))
            print("✅ Added missing column: tokens.live_since")
        # Ensure dev_activity JSON column exists (added in recent schema updates)
        if 'dev_activity' not in columns:
            try:
                conn.execute(text("ALTER TABLE tokens ADD COLUMN IF NOT EXISTS dev_activity JSON"))
                print("✅ Added missing column: tokens.dev_activity")
            except Exception as e:
                print(f"⚠️ Warning: failed to add tokens.dev_activity: {e}")

        # Ensure created_coin_count integer column exists
        if 'created_coin_count' not in columns:
            try:
                conn.execute(text("ALTER TABLE tokens ADD COLUMN IF NOT EXISTS created_coin_count INTEGER DEFAULT 0 NOT NULL"))
                print("✅ Added missing column: tokens.created_coin_count")
            except Exception as e:
                print(f"⚠️ Warning: failed to add tokens.created_coin_count: {e}")

        # Ensure creator balance columns exist
        if 'creator_balance_sol' not in columns:
            try:
                conn.execute(text("ALTER TABLE tokens ADD COLUMN IF NOT EXISTS creator_balance_sol DOUBLE PRECISION"))
                print("✅ Added missing column: tokens.creator_balance_sol")
            except Exception as e:
                print(f"⚠️ Warning: failed to add tokens.creator_balance_sol: {e}")

        if 'creator_balance_usd' not in columns:
            try:
                conn.execute(text("ALTER TABLE tokens ADD COLUMN IF NOT EXISTS creator_balance_usd DOUBLE PRECISION"))
                print("✅ Added missing column: tokens.creator_balance_usd")
            except Exception as e:
                print(f"⚠️ Warning: failed to add tokens.creator_balance_usd: {e}")


def drop_tables():
    """
    Drop all database tables
    Use with caution - this will delete all data
    """
    Base.metadata.drop_all(bind=engine)
    print("⚠️ Database tables dropped")


def init_db():
    """
    Initialize database with tables and any initial data
    """
    create_tables()
    print("🎯 Database initialized successfully")


# Database health check
def check_db_connection() -> bool:
    """
    Check if database connection is working
    Returns True if connection is successful, False otherwise
    """
    try:
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


# Database statistics
def get_db_stats():
    """
    Get database connection pool statistics
    """
    return {
        "pool_size": engine.pool.size(),
        "checked_in": engine.pool.checkedin(),
        "checked_out": engine.pool.checkedout(),
        "overflow": engine.pool.overflow(),
        "invalid": engine.pool.invalid(),
    }
