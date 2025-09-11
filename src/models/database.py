import os
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from .token import Base

load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/db")

# Create engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,  # Increased from 10 to handle more concurrent connections
    max_overflow=30,  # Increased from 20 to provide more flexibility
    pool_timeout=60,  # Increased timeout from 30 to 60 seconds
    pool_recycle=3600,  # Recycle connections after 1 hour
    echo=False,  # Set to True for SQL query logging in development
)

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
    return {
        "pool_size": pool.size(),
        "checkedin": pool.checkedin(),
        "checkedout": pool.checkedout(),
        "overflow": pool.overflow(),
        "invalid": pool.invalid(),
    }


def create_tables():
    """
    Create all database tables
    Call this once when the application starts
    """
    Base.metadata.create_all(bind=engine)
    print("✅ Database tables created successfully")


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
