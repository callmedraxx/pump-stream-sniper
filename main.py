import asyncio
from contextlib import asynccontextmanager
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from src.services.stream import  connected_clients, fetch_and_relay_axiom
from src.services.fetch_live import poll_live_tokens
from src.services.fetch_ath import start_background_loop, stop_background_loop
from src.services.creator_count_service import CreatorCountService
from src.models import get_db, Token
from src.models.database import create_tables
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Load environment variables from both files
load_dotenv('.env.docker')  # Docker-safe variables
load_dotenv('.env.local')   # Sensitive credentials

# Global variables for background tasks
live_tokens_task = None
livestream_task = None
unified_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global live_tokens_task
    # Startup
    print("🚀 Starting application...")

    # Create database tables if they don't exist.
    # In multi-worker setups (gunicorn), avoid running migrations/create_all from every worker.
    # Only run create_tables when explicitly requested via the environment variable
    # `RUN_DB_MIGRATIONS_ON_STARTUP=1` to prevent exhausting DB connections during worker boot.
    run_migrations = os.getenv("RUN_DB_MIGRATIONS_ON_STARTUP", "0") == "1"
    if run_migrations:
        print("🗄️  Ensuring database tables exist (RUN_DB_MIGRATIONS_ON_STARTUP=1)...")
        create_tables()
    else:
        print("�️  Skipping create_tables on worker startup (set RUN_DB_MIGRATIONS_ON_STARTUP=1 to enable)")

    print("�📊 Starting live tokens polling...")
    live_tokens_task = asyncio.create_task(poll_live_tokens())

    # Start inactive-token cleanup loop (run inside DatabaseSyncService created per-poll)
    # We create a dedicated DatabaseSyncService here with a short-lived DB session factory
    try:
        from src.models.database import SessionLocal
        from src.services.database_sync_service import DatabaseSyncService

        # Create a dedicated short-lived DB session for housekeeping service and
        # ensure we close it on shutdown. Prefer passing a session instance that
        # will be explicitly closed rather than calling next(get_db()) without
        # a corresponding close.
        housekeeping_db = SessionLocal()
        housekeeping_sync_service = DatabaseSyncService(db=housekeeping_db, max_workers=5)
        housekeeping_task = asyncio.create_task(housekeeping_sync_service.remove_inactive_tokens_loop(interval_seconds=5, grace_seconds=5))
        print("🧹 Started inactive-token cleanup loop")
    except Exception as e:
        print(f"⚠️ Failed to start inactive-token cleanup loop: {e}")

    # # Start WebSocket connections to pump.fun
    # print("🔗 Starting pump.fun WebSocket connections...")
    # try:
    #     # Start Axiom websocket (non-blocking)
    #     unified_task = asyncio.create_task(fetch_and_relay_axiom())
    #     print("🔗 Started Axiom websocket background task")
    # except Exception as e:
    #     print(f"⚠️ Failed to start Axiom websocket: {e}")

    # Start ATH background loop (fetch ATH for complete==True every 60s)
    # try:
    #     ath_task = start_background_loop(interval_seconds=60, batch_size=100, delay_between_batches=0.5)
    #     print("⚡ Started ATH background loop")
    # except Exception as e:
    #     print(f"⚠️ Failed to start ATH background loop: {e}")

    # Start CreatorCountService (refresh created_coin_count every 10 minutes)
    # try:
    #     creator_count_service = CreatorCountService()
    #     creator_count_task = asyncio.create_task(creator_count_service.run_loop())
    #     print("🔢 Started CreatorCountService background loop")
    # except Exception as e:
    #     print(f"⚠️ Failed to start CreatorCountService: {e}")


    yield
    # Shutdown
    print("🛑 Shutting down background tasks...")
    if live_tokens_task:
        live_tokens_task.cancel()
        try:
            await live_tokens_task
        except asyncio.CancelledError:
            pass

    # Shutdown housekeeping task
    try:
        if 'housekeeping_task' in locals() and housekeeping_task:
            housekeeping_task.cancel()
            try:
                await housekeeping_task
            except asyncio.CancelledError:
                pass
            try:
                housekeeping_sync_service.cleanup()
            except Exception:
                pass
            # Close the housekeeping DB session if we created one
            try:
                if 'housekeeping_db' in locals() and housekeeping_db:
                    housekeeping_db.close()
            except Exception:
                pass
            print("🧹 Stopped inactive-token cleanup loop")
    except Exception:
        pass

    if 'unified_task' in locals():
        unified_task.cancel()
        try:
            await unified_task
        except asyncio.CancelledError:
            pass

    # Stop ATH background loop if running
    try:
        await stop_background_loop()
        print("⚡ Stopped ATH background loop")
    except Exception:
        pass
    # Stop CreatorCountService
    try:
        creator_count_task.cancel()
        try:
            await creator_count_task
        except asyncio.CancelledError:
            pass
        print("🔢 Stopped CreatorCountService")
    except Exception:
        pass

    # Stop Redis subscriber bridge
    try:
        if '_redis_subscriber_task' in globals() and _redis_subscriber_task:
            _redis_subscriber_stop_event.set()
            _redis_subscriber_task.cancel()
            try:
                await _redis_subscriber_task
            except asyncio.CancelledError:
                pass
            print("🔁 Stopped Redis pubsub subscriber bridge")
    except Exception:
        pass

app = FastAPI(lifespan=lifespan)



@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    from src.models.database import SessionLocal

    live_tokens_count = 0
    last_update_time = None

    try:
        db = SessionLocal()
        try:
            live_tokens_count = db.query(Token).filter(Token.is_live == True).count()
            last_update = db.query(Token.updated_at).order_by(Token.updated_at.desc()).first()
            last_update_time = last_update[0].isoformat() if last_update else None
        finally:
            try:
                db.close()
            except Exception:
                pass
    except Exception:
        # If DB is unavailable, we still return a 200 health but with zeros so monitoring
        # can detect degraded state. Avoid raising to keep the endpoint lightweight.
        live_tokens_count = 0
        last_update_time = None

    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "pumpfun-backend",
        "version": "1.0.0",
        "environment": os.getenv("ENVIRONMENT", "development"),
        "live_tokens_count": live_tokens_count,
        "last_update": last_update_time,
    }