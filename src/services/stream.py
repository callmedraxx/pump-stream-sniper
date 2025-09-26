import asyncio
import json
import logging
from datetime import datetime

import aiohttp
from aiohttp import WSMsgType
import os
from typing import Optional
from yarl import URL


async def _refresh_axiom_tokens(session: aiohttp.ClientSession, cookie_jar: Optional[aiohttp.CookieJar]) -> bool:
    """Call the Axiom refresh-access-token endpoint to rotate cookies.

    This will attempt to send the refresh cookie and update cookie_jar from the response cookies.
    Returns True if refresh succeeded and cookie_jar was updated.
    """
    refresh_url = os.getenv("AXIOM_REFRESH_URL", "https://api8.axiom.trade/refresh-access-token")
    try:
        # Build Cookie header from cookie_jar if possible (some servers require explicit Cookie header)
        cookie_header = ""
        try:
            if cookie_jar is not None:
                url_obj = URL(refresh_url)
                stored = cookie_jar.filter_cookies(url_obj)
                if stored:
                    cookie_header = "; ".join(f"{k}={v.value}" for k, v in stored.items())
        except Exception:
            cookie_header = ""

        # Fallback to environment-provided cookie strings
        if not cookie_header:
            cookie_header = "auth-refresh-token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyZWZyZXNoVG9rZW5JZCI6IjgwZWNlMDgyLTFkYjEtNDY3MS1hMmE0LWZmMmFmMTZiOTMyOSIsImlhdCI6MTc1ODgyOTA4Mn0.dba3VWTI_bT4wckzx_hO_cgRz0SW3TNYkddmtf-dze8; auth-access-token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdXRoZW50aWNhdGVkVXNlcklkIjoiYTBmOWYyYmYtM2ViNy00NTJjLTgwMTEtODI5NzIzNjkyZjAxIiwiaWF0IjoxNzU4ODUwNTc3LCJleHAiOjE3NTg4NTE1Mzd9.aZiGZ1GsDetnI50_teoaWBslgLg2yh90Uzj8wRjwB4I"

        headers = {"Cookie": cookie_header} if cookie_header else None

        async with session.post(refresh_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            text = await resp.text()
            if resp.status == 200:
                cookies_found = False
                try:
                    # aiohttp exposes resp.cookies as mapping of Morsel-like objects
                    url_obj = URL(refresh_url)
                    for name, morsel in resp.cookies.items():
                        if cookie_jar is not None:
                            cookie_jar.update_cookies({name: morsel.value}, response_url=url_obj)
                            cookies_found = True
                except Exception:
                    logger.exception("Failed to parse cookies from refresh response")

                if cookies_found:
                    logger.info("Axiom token refresh succeeded: %s", text.strip())
                    return True
                else:
                    logger.warning("Axiom refresh returned 200 but no cookies were set: %s", text.strip())
                    return False
            else:
                logger.warning("Axiom refresh failed: status=%s text=%s", resp.status, text.strip())
                return False
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Exception while refreshing Axiom tokens")
        return False

from src.models.database import SessionLocal, DB_CONNECTION_SEMAPHORE, engine, log_pool_status
from sqlalchemy import text
from src.models.token import Token
from sqlalchemy.exc import OperationalError
import time as _time

logger = logging.getLogger(__name__)
connected_clients = set()


# Current SOL price (USD) seen from the sol_price room. Protected by an asyncio.Lock.
_current_sol_price: float | None = None
_sol_price_lock: asyncio.Lock | None = None


def _ensure_sol_price_lock():
    global _sol_price_lock
    if _sol_price_lock is None:
        _sol_price_lock = asyncio.Lock()


async def set_sol_price(value: float):
    _ensure_sol_price_lock()
    async with _sol_price_lock:
        global _current_sol_price
        _current_sol_price = float(value)
        logger.info("Updated local SOL price: %s", _current_sol_price)


async def get_sol_price() -> float | None:
    _ensure_sol_price_lock()
    try:
        async with _sol_price_lock:
            val = _current_sol_price
    except Exception:
        # In rare cases the lock may not be usable from this context; fall back to direct access
        val = _current_sol_price
    logger.debug("get_sol_price returning %s", val)
    return val


async def _process_pump_live_tokens(entries: list[dict]):
    """Process a batch of pump-live-stream-tokens entries.

    Steps:
    - Read the current sol price (if missing, skip and log)
    - Build a list of mint addresses and computed USD mcaps
    - Query DB once for existing tokens matching those mints
    - For tokens whose current mcap differs from new mcap, call update_token_mcap
      using the DB_CONNECTION_SEMAPHORE and asyncio.to_thread to avoid exhausting DB pool.
    - Log summary information.
    """
    try:
        sol_price = await get_sol_price()
        if sol_price is None:
            logger.warning("Sol price unknown; skipping pump-live-stream-tokens processing of %d entries", len(entries))
            return

        # Build mapping mint -> marketCapUSD
        mint_to_mcap = {}
        for item in entries:
            mint = item.get("tokenAddress") or item.get("mintAddress")
            mcap_sol = item.get("marketCapSol") or item.get("market_cap_sol") or item.get("marketCap") or item.get("marketCapSol")
            # guard: ensure numeric
            try:
                mcap_sol = float(mcap_sol)
            except Exception:
                continue
            mcap_usd = mcap_sol * float(sol_price)
            mint_to_mcap[mint] = mcap_usd

        if not mint_to_mcap:
            logger.debug("No valid mcaps parsed from pump-live-stream-tokens payload")
            return

        mint_list = [m for m in mint_to_mcap.keys() if m]

        # Log pool status before fetching existing tokens
        try:
            log_pool_status(logger, "before_fetch_existing")
        except Exception:
            pass

        # Query DB for existing tokens matching these mints in a thread
        def _fetch_existing(mints: list[str]) -> dict:
            db = SessionLocal()
            try:
                rows = db.query(Token.mint_address, Token.mcap).filter(Token.mint_address.in_(mints)).all()
                return {r[0]: float(r[1] or 0.0) for r in rows}
            finally:
                db.close()

        existing = await asyncio.to_thread(_fetch_existing, mint_list)

        # Log pool status after fetching existing tokens
        try:
            log_pool_status(logger, "after_fetch_existing")
        except Exception:
            pass

        total = len(entries)
        matched = len(existing)
        to_update = []
        for mint, new_mcap in mint_to_mcap.items():
            if not mint:
                continue
            old = existing.get(mint)
            # compare with small epsilon to avoid float jitter
            if old is None:
                # token not in DB
                continue
            if abs((old or 0.0) - (new_mcap or 0.0)) > 0.0001:
                to_update.append((mint, new_mcap))

        updated = 0
        failed = 0

        # Instead of updating each token in its own DB session (which can exhaust
        # the DB connection pool), perform bulk updates in batches using a single
        # connection per batch. This reduces connection churn and prevents
        # "too many connections" errors.
        async def _do_bulk_update(batch: list[tuple[str, float]]):
            try:
                async with DB_CONNECTION_SEMAPHORE:
                    # Log pool status before bulk update
                    try:
                        log_pool_status(logger, f"before_bulk_update size={len(batch)}")
                    except Exception:
                        pass
                    # Run the blocking DB work in a thread to avoid blocking the loop
                    res = await asyncio.to_thread(bulk_update_tokens, batch)
                    try:
                        log_pool_status(logger, f"after_bulk_update size={len(batch)}")
                    except Exception:
                        pass
                    return res
            except Exception:
                logger.exception("Failed to perform bulk update for batch size %d", len(batch))
                return 0

        # Chunk updates to a reasonable size to avoid super-long SQL statements
        CHUNK_SIZE = 20
        if to_update:
            batches = [to_update[i : i + CHUNK_SIZE] for i in range(0, len(to_update), CHUNK_SIZE)]
            results = []
            for batch in batches:
                count = await _do_bulk_update(batch)
                results.append(count)
            for c in results:
                updated += int(c or 0)

        logger.info(
            "Processed pump-live-stream-tokens: total=%d matched_in_db=%d scheduled_updates=%d updated=%d failed=%d",
            total,
            matched,
            len(to_update),
            updated,
            failed,
        )

    except Exception:
        try:
            log_pool_status(logger, "error_in_process_pump_live_tokens")
        except Exception:
            pass
        logger.exception("Error in _process_pump_live_tokens")


# In-memory tracker for recently-updated tokens (mint addresses).
# We'll store (timestamp, mint_address) tuples and periodically count unique mints in the last window.
_recent_updates: "collections.deque[tuple[float, str]]" = None
_recent_updates_lock: asyncio.Lock | None = None
_main_loop: asyncio.AbstractEventLoop | None = None


def _ensure_tracker():
    """Lazy initialize tracking structures."""
    global _recent_updates, _recent_updates_lock
    if _recent_updates is None:
        import collections

        _recent_updates = collections.deque()
    if _recent_updates_lock is None:
        _recent_updates_lock = asyncio.Lock()


def record_token_update(mint_address: str):
    """Record that `mint_address` was updated at current time (epoch seconds).

    This is safe to call from threads because it only appends to an in-memory deque
    via asyncio.run_coroutine_threadsafe when called from non-async contexts.
    """
    _ensure_tracker()

    async def _append():
        now = asyncio.get_event_loop().time()
        async with _recent_updates_lock:
            _recent_updates.append((now, mint_address))

    try:
        # If we're in an event loop, schedule directly
        try:
            running_loop = asyncio.get_running_loop()
            # we're inside the main loop; schedule the coroutine
            running_loop.call_soon_threadsafe(lambda: asyncio.create_task(_append()))
            return
        except RuntimeError:
            # Not in a running loop (likely a background thread). Try to submit to the main loop if known.
            main_loop = globals().get("_main_loop")
            if main_loop and main_loop.is_running():
                asyncio.run_coroutine_threadsafe(_append(), main_loop)
                return

        # Fallback: create a temporary event loop in this thread to run the coroutine synchronously
        temp_loop = asyncio.new_event_loop()
        try:
            temp_loop.run_until_complete(_append())
        finally:
            temp_loop.close()
    except Exception:
        # Best-effort: if anything goes wrong, swallow to avoid breaking callers
        logger.exception("Failed to record token update for %s", mint_address)



async def track_updated_tokens(window_seconds: int = 10):
    """Background task: every `window_seconds`, count unique tokens updated within the last window and log it."""
    _ensure_tracker()
    while True:
        try:
            await asyncio.sleep(window_seconds)
            cutoff = asyncio.get_event_loop().time() - window_seconds
            unique = set()
            async with _recent_updates_lock:
                # Remove old entries from left
                while _recent_updates and _recent_updates[0][0] < cutoff:
                    _recent_updates.popleft()
                for ts, mint in _recent_updates:
                    unique.add(mint)

            logger.info("Unique tokens updated in last %ds: %d", window_seconds, len(unique))
        except Exception:
            logger.exception("Error in track_updated_tokens loop")


# 


def update_token_mcap(mint_address: str, market_cap: float):
    """Update the market cap of a token in the database if it exists."""
    db = SessionLocal()
    try:
        token = db.query(Token).filter(Token.mint_address == mint_address).first()
        if token:
            # Update mcap and recalculate progress relative to current ATH
            old_mcap = token.mcap
            old_progress = token.progress

            #logger.info(f"Found token {mint_address}; current mcap {old_mcap} -> incoming {market_cap}")

            # If the incoming market cap is the same as the stored one, skip updating to avoid unnecessary commits
            try:
                new_mcap = float(market_cap)
            except Exception:
                # If conversion fails, log and skip
                logger.warning("Invalid market_cap for %s: %s", mint_address, market_cap)
                return

            if old_mcap == new_mcap:
                logger.debug("mcap for %s unchanged (%s); skipping update", mint_address, new_mcap)
                return

            token.mcap = new_mcap

            # Recalculate progress: (mcap / ath) * 100, guard against zero/None ATH
            try:
                if token.ath and token.ath > 0:
                    token.progress = (token.mcap / token.ath) * 100
                else:
                    token.progress = 0.0
            except Exception:
                # Defensive: if any numeric error occurs, leave progress unchanged
                logger.exception("Failed to recalculate progress for %s", mint_address)

            # Only commit if mcap or progress changed significantly
            progress_changed = abs((token.progress or 0.0) - (old_progress or 0.0)) > 0.0001
            if token.mcap != old_mcap or progress_changed:
                db.commit()
                try:
                    record_token_update(mint_address)
                except Exception:
                    logger.exception("Failed to record update after mcap commit for %s", mint_address)
                # logger.info(
                #     "Updated mcap/progress for %s -> mcap=%s progress=%.4f",
                #     mint_address,
                #     token.mcap,
                #     token.progress or 0.0,
                # )
            else:
                logger.debug("mcap/progress unchanged for %s; no commit", mint_address)
        else:
            pass
    except Exception as e:
        print(f"Error updating token mcap: {e}")
    finally:
        db.close()


def update_token_ath_if_needed(mint_address: str, market_cap: float):
    """Update ATH and progress if mcap exceeds current ATH."""
    # Use an atomic DB update to ensure ATH never decreases and to avoid races.
    # We only set ath = :market_cap when the existing ath < :market_cap.
    from sqlalchemy import text

    db = SessionLocal()
    try:
        changed = False

        # Atomic conditional update: only set ath to new market_cap if it's greater than current ath
        try:
            stmt = text(
                "UPDATE tokens SET ath = :new_ath WHERE mint_address = :mint AND (ath IS NULL OR ath < :new_ath)"
            )
            res = db.execute(stmt, {"new_ath": float(market_cap), "mint": mint_address})
            # If a row was affected, we updated ATH
            if res.rowcount and res.rowcount > 0:
                # Recalculate progress using the new ATH (which equals market_cap here)
                try:
                    # Ensure the Token object is loaded to update progress column via ORM
                    token = db.query(Token).filter(Token.mint_address == mint_address).first()
                    if token:
                        token.progress = (market_cap / token.ath) * 100 if token.ath and token.ath > 0.0 else 0.0
                        db.commit()
                        try:
                            record_token_update(mint_address)
                        except Exception:
                            logger.exception("Failed to record update after ATH commit for %s", mint_address)
                        changed = True
                except Exception:
                    db.rollback()
                    logger.exception("Failed to finalize ATH update for %s", mint_address)

        except Exception:
            db.rollback()
            logger.exception("Atomic ATH update failed for %s", mint_address)

        # Always ensure progress is recalculated even if ATH wasn't changed by the atomic update
        try:
            token = db.query(Token).filter(Token.mint_address == mint_address).first()
            if token:
                old_progress = token.progress
                token.progress = (market_cap / token.ath) * 100 if token.ath and token.ath > 0.0 else 0.0
                if abs((old_progress or 0.0) - (token.progress or 0.0)) > 0.01:
                    db.commit()
                    try:
                        record_token_update(mint_address)
                    except Exception:
                        logger.exception("Failed to record update after progress commit for %s", mint_address)
                    changed = True
        except Exception:
            db.rollback()
            logger.exception("Failed recalculating progress for %s", mint_address)

    except Exception as e:
        logger.exception("Error updating token ATH/progress: %s for %s", e, mint_address)
    finally:
        db.close()


def backfill_ath_from_mcap(log_limit: int = 1000):
    """One-time helper: find tokens where mcap > ath and set ath = mcap (atomic).

    This function updates rows in batches and logs up to `log_limit` changes. It should
    be safe to run at startup or manually to repair existing inconsistent data.
    """
    # Perform a single, atomic bulk UPDATE in one DB connection to avoid opening
    # multiple sessions/connections and to eliminate per-row roundtrips.
    # This sets ath = mcap where mcap > ath (or ath IS NULL), and sets progress = 100
    # for those rows because ath will equal mcap after the update.
    from sqlalchemy import text

    updated = 0
    try:
        with engine.begin() as conn:
            update_sql = text(
                """
                UPDATE tokens
                SET ath = mcap,
                    progress = 100
                WHERE mcap IS NOT NULL
                  AND (ath IS NULL OR mcap > ath)
                """
            )
            res = conn.execute(update_sql)
            # rowcount gives number of rows affected by the UPDATE
            try:
                updated = int(res.rowcount or 0)
            except Exception:
                updated = 0

            # Log up to `log_limit` sample rows that were fixed for audit. We'll query only if any were updated.
            if updated > 0:
                try:
                    samples = conn.execute(text(
                        "SELECT mint_address, mcap, ath FROM tokens WHERE mcap IS NOT NULL AND (ath IS NULL OR mcap = ath) LIMIT :lim"
                    ), {"lim": min(log_limit, 50)}).fetchall()
                    for mint, mcap, ath in samples[:log_limit]:
                        logger.info("backfill: fixed %s (mcap=%.2f ath=%.2f)", mint, float(mcap), float(ath or 0.0))
                except Exception:
                    logger.exception("Failed to fetch backfill samples")

        logger.info("backfill_ath_from_mcap: updated %d rows", updated)
        return updated
    except Exception:
        logger.exception("Error running backfill_ath_from_mcap")
        return updated


def bulk_update_tokens(pairs: list[tuple[str, float]]) -> int:
    """Bulk update tokens' mcap/progress and atomically set ATH when crossed.

    pairs: list of (mint_address, mcap)
    Returns number of rows updated (mcap or ath/progress changed).
    This uses a single DB connection/transaction to avoid opening many connections.
    """
    if not pairs:
        return 0

    updated = 0
    try:
        # Use a single connection/transaction for the entire batch. Wrap in a
        # small retry loop to tolerate transient OperationalError from the
        # provider-side pooler (e.g., MaxClientsInSessionMode). This function
        # runs in a thread via asyncio.to_thread, so using time.sleep is fine.
        max_retries = 3
        backoff = 0.5
        last_exc = None
        for attempt in range(1, max_retries + 1):
            try:
                with engine.begin() as conn:
                    # We'll prepare a temporary table-like VALUES clause and perform
                    # a single UPDATE joining against it. Use PostgreSQL's FROM (VALUES ...)
                    # pattern which is efficient and avoids multiple roundtrips.
                    
                    # Build VALUES parameter list
                    vals = []
                    params = {}
                    for idx, (mint, mcap) in enumerate(pairs):
                        key_mint = f"m{idx}"
                        key_mcap = f"c{idx}"
                        # Use plain placeholders; avoid inline casts which can confuse the SQL parser
                        vals.append(f"(:{key_mint}, :{key_mcap})")
                        params[key_mint] = mint
                        params[key_mcap] = float(mcap)

                    values_clause = ",".join(vals)

                    # Perform an UPDATE that sets mcap, conditionally updates ATH when mcap > ath,
                    # and recalculates progress. We match tokens by mint_address.
                    sql = text(f"""
                        WITH incoming(mint, new_mcap) AS (VALUES {values_clause})
                        UPDATE tokens
                        SET
                            mcap = incoming.new_mcap,
                            ath = CASE WHEN tokens.ath IS NULL OR incoming.new_mcap > tokens.ath THEN incoming.new_mcap ELSE tokens.ath END,
                            progress = CASE WHEN (CASE WHEN tokens.ath IS NULL OR incoming.new_mcap > tokens.ath THEN incoming.new_mcap ELSE tokens.ath END) > 0
                                            THEN (incoming.new_mcap / (CASE WHEN tokens.ath IS NULL OR incoming.new_mcap > tokens.ath THEN incoming.new_mcap ELSE tokens.ath END)) * 100
                                            ELSE 0 END
                        FROM incoming
                        WHERE tokens.mint_address = incoming.mint
                    """)

                    res = conn.execute(sql, params)
                    try:
                        updated = int(res.rowcount or 0)
                    except Exception:
                        updated = 0

                    # Record updates for audit/logging by selecting changed rows (limit small)
                    if updated > 0:
                        try:
                            # Build parameter list for IN clause
                            mint_keys = []
                            mint_params = {}
                            for idx, (mint, _) in enumerate(pairs[:50]):
                                k = f"m_sample_{idx}"
                                mint_keys.append(f":{k}")
                                mint_params[k] = mint

                            in_clause = ",".join(mint_keys) or "''"
                            sample_sql = text(f"SELECT mint_address, mcap, ath, progress FROM tokens WHERE mint_address IN ({in_clause}) LIMIT 50")
                            sample = conn.execute(sample_sql, mint_params)
                            for r in sample.fetchall():
                                logger.debug("bulk_update: %s mcap=%.2f ath=%.2f progress=%.2f", r[0], float(r[1] or 0.0), float(r[2] or 0.0), float(r[3] or 0.0))
                        except Exception:
                            logger.exception("Failed to fetch bulk update samples")

                    return updated

            except OperationalError as oe:
                last_exc = oe
                logger.warning("OperationalError during bulk_update_tokens attempt %d/%d: %s", attempt, max_retries, oe)
                if attempt < max_retries:
                    _time.sleep(backoff)
                    backoff *= 2
                    continue
                else:
                    # re-raise after exhausting retries so caller sees the failure
                    raise
            # We'll prepare a temporary table-like VALUES clause and perform
            # a single UPDATE joining against it. Use PostgreSQL's FROM (VALUES ...)
            # pattern which is efficient and avoids multiple roundtrips.

        # If we get here something bad happened and exception bubbled up
        return updated
    except Exception:
        logger.exception("Error running bulk_update_tokens")
        return updated


def update_token_dev_activity(mint_address: str, user_address: str, trade: dict):
    """Update the token.dev_activity JSON when the creator/dev performs a trade.

    Stores a small object with type, amountUSD, amountSOL, timestamp, and tx.
    """
    db = SessionLocal()
    try:
        token = db.query(Token).filter(Token.mint_address == mint_address).first()
        if not token:
            return

        # Only update if the trade is from the creator
        if token.creator and token.creator == user_address:
            dev_obj = {
                "type": trade.get("type"),
                "amountUSD": float(trade.get("amountUsd") or trade.get("amountUSD") or 0) if (trade.get("amountUsd") or trade.get("amountUSD")) else None,
                "amountSOL": float(trade.get("amountSol") or trade.get("amountSOL") or 0) if (trade.get("amountSol") or trade.get("amountSOL")) else None,
                "timestamp": trade.get("timestamp"),
                "userAddress": user_address,
                "tx": trade.get("tx"),
            }

            # Only update DB if dev_activity changed to avoid frequent balance calls
            if token.dev_activity != dev_obj:
                token.dev_activity = dev_obj

                # Fetch creator balances for this mint (best-effort)
                try:
                    from .fetch_activities import fetch_creator_balance_for_mint
                    balance_info = asyncio.run(fetch_creator_balance_for_mint(user_address, mint_address))
                    if balance_info:
                        token.creator_balance_sol = balance_info.get("balance")
                        token.creator_balance_usd = balance_info.get("balanceUSD")
                        # logger.info(
                        #     f"Updated creator balance for {mint_address} after dev_activity change: "
                        #     f"sol={token.creator_balance_sol} usd={token.creator_balance_usd} (creator={user_address})"
                        # )
                except Exception as e:
                    # If asyncio.run fails inside thread or network error, just log and continue
                    logger.warning(f"Failed to fetch creator balance during dev_activity update for {mint_address}: {e}")

                db.commit()
                logger.info(f"Updated dev_activity for {mint_address}: {dev_obj}")
    except Exception as e:
        print(f"Error updating dev_activity for {mint_address}: {e}")
    finally:
        db.close()




async def fetch_and_relay_axiom():
    """Connect to Axiom cluster and log room messages.

    Connects to: wss://cluster-global2.axiom.trade/
    Joins rooms: pump-live-stream-tokens and sol_price
    Sends ping payloads as {"method":"ping"} periodically and responds to pings/pongs.
    """
    base_uri = os.getenv("AXIOM_URI", "wss://cluster-global2.axiom.trade/")
    axiom_query = os.getenv("AXIOM_QUERY", "")
    
    # Build URI with query parameters
    if axiom_query:
        if base_uri.endswith("?") or base_uri.endswith("&"):
            uri = base_uri + axiom_query
        elif "?" in base_uri:
            uri = base_uri + "&" + axiom_query
        else:
            uri = base_uri + "?" + axiom_query
    else:
        uri = base_uri

    # Setup headers for authentication; we'll manage cookies in `cookie_jar` and refresh tokens when needed
    headers = {}
    cookie_jar = aiohttp.CookieJar()

    # Allow overriding initial cookie via env for manual ops
    env_cookie = os.getenv("AXIOM_COOKIE")
    if env_cookie:
        # env_cookie is expected to be a cookie string like 'auth-refresh-token=...; auth-access-token=...'
        # parse and add to cookie jar for the axiom domain
        try:
            url_obj = URL(uri)
        except Exception:
            url_obj = None
        for pair in [c.strip() for c in env_cookie.split(";") if c.strip()]:
            if "=" in pair:
                k, v = pair.split("=", 1)
                if url_obj is not None:
                    cookie_jar.update_cookies({k: v}, response_url=url_obj)
                else:
                    # fallback: try simple update without response_url
                    cookie_jar.update_cookies({k: v})

    # Add browser-like headers to match your HTTP request
    headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Origin": "https://axiom.trade",
    })

    # Room configuration
    join_payload = {"action": "join", "room": os.getenv("AXIOM_ROOM", "pump-live-stream-tokens")}
    join_sol = {"action": "join", "room": os.getenv("AXIOM_SOL_ROOM", "sol_price")}
    ping_payload = {"method": "ping"}
    ping_interval = 25

    # create session with cookie_jar so cookies are sent to the refresh endpoint automatically
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None), cookie_jar=cookie_jar) as session:
        # Periodically refresh tokens in background to avoid expiry (every 12 minutes)
        refresh_task = None
        async def _periodic_refresh():
            # run until cancelled
            try:
                while True:
                    try:
                        await asyncio.sleep(12 * 60)  # 12 minutes
                        await _refresh_axiom_tokens(session, cookie_jar)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("Periodic Axiom token refresh failed")
            finally:
                logger.debug("Periodic Axiom token refresh task exiting")

        # start the periodic refresh task so tokens are rotated regularly
        refresh_task = asyncio.create_task(_periodic_refresh())

        try:
            while True:
                ping_task = None
                try:
                    logger.info("Connecting to Axiom %s", uri)
                
                    # Attach cookies to the session before connecting
                    if cookie_jar:
                        session._cookie_jar = cookie_jar

                    async with session.ws_connect(uri, headers=headers) as ws:
                        logger.info("Connected to Axiom successfully")

                        # Join rooms
                        await ws.send_json(join_payload)
                        await ws.send_json(join_sol)
                        logger.info("Sent join payloads to Axiom rooms")

                        # Define ping coroutine
                        async def axiom_ping():
                            while True:
                                try:
                                    await asyncio.sleep(ping_interval)
                                    await ws.send_json(ping_payload)
                                    logger.debug("Sent ping to Axiom")
                                except Exception as e:
                                    logger.debug("Ping task stopped: %s", e)
                                    break

                        # Start ping task
                        ping_task = asyncio.create_task(axiom_ping())

                        # Handle messages
                        async for msg in ws:
                            try:
                                if msg.type == WSMsgType.TEXT:
                                    text = msg.data
                                    try:
                                        obj = json.loads(text)
                                    except json.JSONDecodeError:
                                        obj = text

                                    # If this is the sol_price room, update local sol price
                                    try:
                                        if isinstance(obj, dict) and obj.get("room") == "sol_price":
                                            content = obj.get("content")
                                            try:
                                                await set_sol_price(float(content))
                                            except Exception:
                                                logger.exception("Failed to parse sol_price content: %s", content)
                                            continue

                                        # If this is the pump-live-stream-tokens room, process token entries
                                        if isinstance(obj, dict) and obj.get("room") == "pump-live-stream-tokens":
                                            content = obj.get("content")
                                            if isinstance(content, list):
                                                # Fire-and-forget processing task to avoid blocking message loop
                                                try:
                                                    asyncio.create_task(_process_pump_live_tokens(content))
                                                except Exception:
                                                    logger.exception("Failed to schedule pump_live_tokens processing")
                                            else:
                                                logger.debug("pump-live-stream-tokens content not list: %s", content)
                                            continue

                                    except Exception:
                                        logger.exception("Error handling Axiom room-specific message")

                                    #logger.info("Axiom TEXT: %s", obj)
                                    
                                elif msg.type == WSMsgType.BINARY:
                                    try:
                                        text = msg.data.decode("utf-8")
                                        obj = json.loads(text)
                                    except (UnicodeDecodeError, json.JSONDecodeError):
                                        obj = msg.data
                                    #logger.info("Axiom BINARY: %s", obj)
                                    
                                elif msg.type == WSMsgType.CLOSED:
                                    logger.info("Axiom socket closed: %s %s", msg.data, msg.extra)
                                    break
                                    
                                elif msg.type == WSMsgType.ERROR:
                                    logger.error("Axiom socket error: %s", ws.exception())
                                    break
                                    
                            except Exception:
                                logger.exception("Error handling Axiom message")

                except aiohttp.WSServerHandshakeError as e:
                    status = getattr(e, "status", None)
                    logger.error("Axiom handshake failed: status=%s uri=%s", status, uri)
                    # If we get 401/403 attempt to refresh access token via API and retry
                    if status in (401, 403):
                        try:
                            refreshed = await _refresh_axiom_tokens(session, cookie_jar)
                            if refreshed:
                                logger.info("Refreshed Axiom tokens from refresh endpoint; retrying connection immediately")
                                continue
                            else:
                                logger.warning("Token refresh attempt did not return new cookies; will retry in 5s")
                        except Exception:
                            logger.exception("Error while refreshing Axiom tokens")
                    await asyncio.sleep(5)
                    
                except Exception:
                    logger.exception("Axiom connection error, retrying in 5s")
                    await asyncio.sleep(5)
                    
                finally:
                    # Cleanup ping task
                    if ping_task and not ping_task.done():
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass
        finally:
            # Cancel the periodic refresh task when outer session is closing
            if refresh_task and not refresh_task.done():
                refresh_task.cancel()
                try:
                    await refresh_task
                except asyncio.CancelledError:
                    pass



# Track subs
# active_subs: dict[str, int] = {}
# sid_counter = 1

# def next_sid() -> int:
#     global sid_counter
#     sid = sid_counter
#     sid_counter += 1
#     return sid


# async def sync_subscriptions(websocket):
#     """
#     Poll DB every 1s and sync subs with Pumpfun socket.
#     """
#     global active_subs
#     while True:
#         try:
#             db = SessionLocal()
#             try:
#                 tokens = db.query(Token.mint_address).all()
#                 current_tokens = {t[0] for t in tokens}
#             finally:
#                 db.close()

#             # Subscribe new tokens
#             new_tokens = current_tokens - set(active_subs.keys())
#             for mint in new_tokens:
#                 sid = next_sid()
#                 sub_line = f"SUB unifiedTradeEvent.processed.{mint} {sid}\r\n"
#                 await websocket.send_bytes(sub_line.encode("utf-8"))
#                 active_subs[mint] = sid
#                 print(f"[+] Subscribed {mint} (sid={sid})")

#             # Unsubscribe removed tokens
#             removed_tokens = set(active_subs.keys()) - current_tokens
#             for mint in removed_tokens:
#                 sid = active_subs[mint]
#                 unsub_line = f"UNSUB {sid}\r\n"
#                 await websocket.send_bytes(unsub_line.encode("utf-8"))
#                 del active_subs[mint]
#                 print(f"[-] Unsubscribed {mint} (sid={sid})")

#             await asyncio.sleep(10)
#         except Exception as e:
#             print(f"Error in sync_subscriptions: {e}")
#             await asyncio.sleep(10)



# async def fetch_and_relay_unified_trades(connected_clients):
#     """Connect to unified-prod endpoint for trade data"""
#     uri = "wss://unified-prod.nats.realtime.pump.fun/"
#     extra_headers = {
#         "Connection": "Upgrade",
#         "Pragma": "no-cache",
#         "Cache-Control": "no-cache",
#         "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
#         "Upgrade": "websocket",
#         "Origin": "https://pump.fun",
#         "Accept-Encoding": "gzip, deflate, br",
#         "Accept-Language": "en-US,en;q=0.9",
#         "Cookie": "_ga=GA1.1.1379899687.1757300687; intercom-id-w7scljv7=b1704389-09ba-49cb-a4b0-f0bd59d00da6; intercom-session-w7scljv7=; intercom-device-id-w7scljv7=5daab4f2-5559-42b8-97d0-c8898ab0d3e4; mp_567f3c2d509af79e6c5694b2d20439d2_mixpanel=%7B%22distinct_id%22%3A%22%24device%3A1c440d2f-677e-4948-8c6c-46e8a219718a%22%2C%22%24device_id%22%3A%221c440d2f-677e-4948-8c6c-46e8a219718a%22%2C%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D; _cfuvid=4Je2MlFk44ZteLfx3R2IaGbVgdIqN__QgprYh0JpNQk-1757379935151-0.0.1.1-604800000; _fs_dwell_passed=4572f959-b96e-46a4-8640-ba9dc76cc77b; fs_lua=1.1757401202929; fs_uid=#o-1YWTMD-na1#b6affcd0-891a-418b-b313-49f38d7ec743:22a5b1f9-6c5e-44da-be6c-1aba6fc378e2:1757401202929::1#/1788836708; __cf_bm=Xxp9b7xgB_vZqCpPcRL5fy4LzkZ80d1P_APveeuHgRs-1757401333-1.0.1.1-kfi6lSR6gn2ikvQvb4Zz2xUCtHHNLQ2Ai8G2_MFDuhUH1ax9pTu0ddAQzQQZEtji3_d6Nmm1jwy6i3tjKeJJ03nEIpHlkT.A1k.sHFmMPDw; _ga_T65NVS2TQ6=GS2.1.s1757401332$$o6$$g0$$t1757401332$$j60$$l0$$h0; cf_clearance=2uV3ZtRZ5vMDUrclzSO.W0uu10mlHNFOiN4Xuh0LJeM-1757401337-1.2.1.1-h.iZWJrpx20Yaj4zty5Hysjxh4a9wILFmOqsB3jLN8oJJOhL5Cz3ZMP2YxNnw_qRwlEz2bMVlRBE7NoPjx0anhEZF_5JFJxe7AyhPgYyOLgpVmnoESGZNOLr0NRgJz1DnYZk19xYU17YWG1if38OOAb48ekKN7NMNtJNyO2Gw2CejCwm1VG2Fhb3r7.pvoJ6Jdhr.FgPlAxwn8Vnhr.maO_MwtGtrTegW7zXVcoWMqM",
#     }

#     async with aiohttp.ClientSession(
#         timeout=aiohttp.ClientTimeout(total=None)
#     ) as session:
#         while True:
#             try:
#                 async with session.ws_connect(uri, headers=extra_headers) as websocket:
#                     #print("Connected to unified-prod WebSocket")

#                     # Variables to track connection state
#                     info_received = False
#                     connect_sent = False
#                     initial_subs_sent = False
#                     warmup_sent = False
#                     real_subs_sent = False
#                     seen_ping = False
#                     seen_pong = False
#                     ping_interval = 25000
#                     ping_timeout = 20000
#                     last_ping_time = None

#                     # Start ping task
#                     ping_task = None
#                     sync_task = None

#                     async def ping_handler():
#                         nonlocal last_ping_time
#                         while True:
#                             await asyncio.sleep(ping_interval / 1000)
#                             try:
#                                 await websocket.send_bytes(b"PING\r\n")
#                                 last_ping_time = asyncio.get_event_loop().time()
#                             except Exception as e:
#                                 break

#                     async for message in websocket:
#                         #print(f"Received message type: {message.type}")
#                         if message.type == aiohttp.WSMsgType.TEXT:
#                             data = message.data
#                             if isinstance(data, bytes):
#                                 data = data.decode("utf-8")
#                             #print(f"Received TEXT data: {data[:200]}...")

#                             # Handle INFO message
#                             if not info_received and data.startswith("INFO"):
#                                 try:
#                                     info_json = data[4:].strip()
#                                     info_data = json.loads(info_json)
#                                     #logger.info("Received INFO from unified-prod: %s", info_data)

#                                     if "ping_interval" in info_data:
#                                         ping_interval = info_data["ping_interval"]
#                                         #logger.debug("Updated unified ping_interval=%sms", ping_interval)
#                                     if "ping_timeout" in info_data:
#                                         ping_timeout = info_data["ping_timeout"]
#                                         #logger.debug("Updated unified ping_timeout=%sms", ping_timeout)

#                                     info_received = True

#                                     # Send CONNECT
#                                     connect_payload = {
#                                         "no_responders": True,
#                                         "protocol": 1,
#                                         "verbose": False,
#                                         "pedantic": False,
#                                         "user": "subscriber",
#                                         "pass": "OX745xvUbNQMuFqV",
#                                         "lang": "nats.ws",
#                                         "version": "1.30.3",
#                                         "headers": True,
#                                     }
#                                     logger.info("Sending CONNECT to unified-prod")
#                                     await websocket.send_bytes(("CONNECT " + json.dumps(connect_payload) + "\r\n").encode("utf-8"))
#                                     connect_sent = True

#                                     # Subscribe to minimal initial topics to establish connection
#                                     if not initial_subs_sent:
#                                         sub1 = b"SUB trades.unified 1\r\n"
#                                         sub2 = b"SUB pump.trades.all 2\r\n"
#                                         logger.info("Sending initial SUBs to unified-prod")
#                                         await websocket.send_bytes(sub1)
#                                         await websocket.send_bytes(sub2)
#                                         initial_subs_sent = True

#                                         # Start ping handler
#                                         ping_task = asyncio.create_task(ping_handler())
#                                         #logger.debug("Started ping handler for unified connection")

#                                         # Start tracker task (once) and capture main loop for cross-thread submissions
#                                         try:
#                                             if not globals().get("_tracker_task"):
#                                                 globals()["_main_loop"] = asyncio.get_running_loop()
#                                                 globals()["_tracker_task"] = asyncio.create_task(track_updated_tokens(5))
#                                         except Exception:
#                                             logger.exception("Failed to start tracker task")

#                                 except json.JSONDecodeError as e:
#                                     logger.warning("Failed to parse INFO from unified-prod: %s", e)

#                             # Handle PING/PONG
#                             elif data.strip() == "PING":
#                                 seen_ping = True
#                                 await websocket.send_bytes(b"PONG\r\n")
#                             elif data.strip() == "PONG":
#                                 seen_pong = True
#                                 if last_ping_time:
#                                     response_time = (asyncio.get_event_loop().time() - last_ping_time) * 1000

#                                 # After first ping/pong exchange, send warmups and start token sync
#                                 if not warmup_sent and (seen_ping or last_ping_time is not None):
#                                     warmup_lines = (
#                                         "SUB _WARMUP_UNIFIED_1758496295588 3\r\n"
#                                         "SUB _WARMUP_UNIFIED_1758496295875 4\r\n"
#                                         "SUB _WARMUP_UNIFIED_1758496296372 5\r\n"
#                                         "SUB _WARMUP_UNIFIED_1758496297223 6\r\n"
#                                         "SUB _WARMUP_UNIFIED_1758496297760 7\r\n"
#                                         "SUB _WARMUP_UNIFIED_1758496307928 8\r\n"
#                                     )
#                                     #logger.info("Sending warmup SUBs to unified-prod")
#                                     await websocket.send_bytes(warmup_lines.encode("utf-8"))
#                                     warmup_sent = True
                                    
#                                     # Start the token subscription sync task
#                                     if not sync_task:
#                                         sync_task = asyncio.create_task(sync_subscriptions(websocket))
#                                         #logger.info("Started token subscription sync task")

#                             # Handle trade messages (MSG format)
#                             elif data.startswith("MSG"):
#                                 lines = data.split("\r\n")
#                                 if len(lines) >= 2:
#                                     msg_line = lines[0]
#                                     payload = lines[1]
#                                     try:
#                                         raw = json.loads(payload)
#                                         if isinstance(raw, str):
#                                             try:
#                                                 trade_data = json.loads(raw)
#                                             except json.JSONDecodeError:
#                                                 trade_data = raw
#                                         else:
#                                             trade_data = raw

#                                         if isinstance(trade_data, dict):
#                                             mint_address = trade_data.get('mintAddress')
#                                             market_cap_str = trade_data.get('marketCap')
                                            
#                                             if mint_address and market_cap_str:
#                                                 try:
#                                                     market_cap = float(market_cap_str)
#                                                     # Limit concurrency to avoid exhausting Supabase connection limit
#                                                     async with DB_CONNECTION_SEMAPHORE:
#                                                         await asyncio.to_thread(update_token_mcap, mint_address, market_cap)
#                                                     # Check and update ATH/progress in real-time
#                                                     async with DB_CONNECTION_SEMAPHORE:
#                                                         await asyncio.to_thread(update_token_ath_if_needed, mint_address, market_cap)
#                                                     # Update dev_activity if creator performed this trade
#                                                     user_addr = trade_data.get('userAddress') or trade_data.get('user_address')
#                                                     if user_addr:
#                                                         async with DB_CONNECTION_SEMAPHORE:
#                                                             await asyncio.to_thread(update_token_dev_activity, mint_address, user_addr, trade_data)
#                                                 except ValueError:
#                                                     print(f"Invalid marketCap value: {market_cap_str}")
#                                         else:
#                                             print("Parsed trade payload is not a dict; skipping mcap/ath update")

#                                         # Relay trade data to connected clients
#                                         for client in connected_clients.copy():
#                                             try:
#                                                 await client.send_text(json.dumps({
#                                                     "type": "unified_trade",
#                                                     "data": trade_data,
#                                                     "timestamp": datetime.now().isoformat()
#                                                 }))
#                                             except:
#                                                 connected_clients.remove(client)
#                                     except json.JSONDecodeError as e:
#                                         print(f"Failed to parse trade payload: {e}")

#                         elif message.type == aiohttp.WSMsgType.BINARY:
#                             data = message.data
#                             try:
#                                 text_data = data.decode("utf-8")
#                                 preview = text_data[:500]
#                                 #print(f"Received BINARY message: {len(data)} bytes; text preview: {preview!r}")
#                             except Exception:
#                                 hex_preview = data[:256].hex()
#                                 #print(f"Received BINARY message: {len(data)} bytes; hex preview (first 256 bytes): {hex_preview}")

#                             try:
#                                 # Handle INFO in binary if it looks like text
#                                 if not info_received and isinstance(text_data, str) and text_data.startswith("INFO"):
#                                     try:
#                                         info_json = text_data[4:].strip()
#                                         info_data = json.loads(info_json)

#                                         if "ping_interval" in info_data:
#                                             ping_interval = info_data["ping_interval"]
#                                             logger.debug("Updated unified ping_interval=%sms", ping_interval)
#                                         if "ping_timeout" in info_data:
#                                             ping_timeout = info_data["ping_timeout"]
#                                             logger.debug("Updated unified ping_timeout=%sms", ping_timeout)

#                                         info_received = True

#                                         # Send CONNECT
#                                         connect_payload = {
#                                             "no_responders": True,
#                                             "protocol": 1,
#                                             "verbose": False,
#                                             "pedantic": False,
#                                             "user": "subscriber",
#                                             "pass": "OX745xvUbNQMuFqV",
#                                             "lang": "nats.ws",
#                                             "version": "1.30.3",
#                                             "headers": True,
#                                         }
#                                         await websocket.send_bytes(("CONNECT " + json.dumps(connect_payload) + "\r\n").encode("utf-8"))
#                                         connect_sent = True

#                                         # Subscribe to minimal initial topics
#                                         if not initial_subs_sent:
#                                             sub1 = b"SUB trades.unified 1\r\n"
#                                             sub2 = b"SUB pump.trades.all 2\r\n"
#                                             await websocket.send_bytes(sub1)
#                                             await websocket.send_bytes(sub2)
#                                             initial_subs_sent = True

#                                             # Start ping handler
#                                             ping_task = asyncio.create_task(ping_handler())
#                                             logger.debug("Started ping handler (binary) for unified connection")

#                                             # Start tracker task (once) and capture main loop for cross-thread submissions
#                                             try:
#                                                 if not globals().get("_tracker_task"):
#                                                     globals()["_main_loop"] = asyncio.get_running_loop()
#                                                     globals()["_tracker_task"] = asyncio.create_task(track_updated_tokens(5))
#                                                     # Start Axiom stream task (non-blocking)
#                                                     if not globals().get("_axiom_task"):
#                                                         globals()["_axiom_task"] = asyncio.create_task(fetch_and_relay_axiom())
#                                             except Exception:
#                                                 logger.exception("Failed to start tracker task (binary)")

#                                     except json.JSONDecodeError as e:
#                                         logger.warning("Failed to parse INFO (binary) from unified-prod: %s", e)

#                                 # Handle binary PING
#                                 elif text_data.strip() == "PING":
#                                     await websocket.send_bytes(b"PONG\r\n")

#                                 # Handle binary PONG
#                                 elif text_data.strip() == "PONG":
#                                     seen_pong = True
#                                     if last_ping_time:
#                                         response_time = (asyncio.get_event_loop().time() - last_ping_time) * 1000

#                                     # After first ping/pong exchange, send warmups and start token sync
#                                     if not warmup_sent and (seen_ping or last_ping_time is not None):
#                                         warmup_lines = (
#                                             "SUB _WARMUP_UNIFIED_1758496295588 3\r\n"
#                                             "SUB _WARMUP_UNIFIED_1758496295875 4\r\n"
#                                             "SUB _WARMUP_UNIFIED_1758496296372 5\r\n"
#                                             "SUB _WARMUP_UNIFIED_1758496297223 6\r\n"
#                                             "SUB _WARMUP_UNIFIED_1758496297760 7\r\n"
#                                             "SUB _WARMUP_UNIFIED_1758496307928 8\r\n"
#                                         )
#                                         logger.info("Sending warmup SUBs (binary) to unified-prod")
#                                         await websocket.send_bytes(warmup_lines.encode("utf-8"))
#                                         warmup_sent = True
                                        
#                                         # Start the token subscription sync task
#                                         if not sync_task:
#                                             sync_task = asyncio.create_task(sync_subscriptions(websocket))
#                                             logger.info("Started token subscription sync task (binary)")

#                                 # Handle binary MSG frames
#                                 elif isinstance(text_data, str) and text_data.startswith("MSG"):
#                                     try:
#                                         header_end = data.find(b"\r\n")
#                                         if header_end == -1:
#                                             raise ValueError("No header terminator found in binary MSG")

#                                         header = data[:header_end].decode("utf-8", errors="replace")
#                                         parts = header.split()
#                                         if len(parts) < 4:
#                                             raise ValueError(f"Unexpected MSG header format: {header}")

#                                         try:
#                                             size = int(parts[-1])
#                                         except Exception:
#                                             raise ValueError(f"Unable to parse size from MSG header: {header}")

#                                         payload_start = header_end + 2
#                                         payload_end = payload_start + size

#                                         if len(data) >= payload_end:
#                                             payload_bytes = data[payload_start:payload_end]

#                                             try:
#                                                 payload_text = payload_bytes.decode("utf-8")
#                                             except UnicodeDecodeError:
#                                                 payload_text = payload_bytes.decode("utf-8", errors="replace")

#                                             raw_bin = None
#                                             try:
#                                                 raw_bin = json.loads(payload_text)
#                                             except json.JSONDecodeError as e:
#                                                 # Recovery heuristics for common malformations
#                                                 pt = payload_text.strip()
#                                                 if pt.startswith('"') and pt.endswith('"'):
#                                                     try:
#                                                         inner = json.loads(pt)
#                                                         if isinstance(inner, str):
#                                                             raw_bin = json.loads(inner)
#                                                         else:
#                                                             raw_bin = inner
#                                                     except Exception:
#                                                         raw_bin = None
#                                                 else:
#                                                     first = pt.find('{')
#                                                     last = pt.rfind('}')
#                                                     if first != -1 and last != -1 and last > first:
#                                                         candidate = pt[first : last + 1]
#                                                         try:
#                                                             raw_bin = json.loads(candidate)
#                                                         except Exception:
#                                                             raw_bin = None

#                                             if raw_bin is None:
#                                                 print(f"Failed to parse binary MSG payload: Unterminated or malformed JSON")
#                                             else:
#                                                 if isinstance(raw_bin, str):
#                                                     try:
#                                                         trade_data_bin = json.loads(raw_bin)
#                                                     except json.JSONDecodeError:
#                                                         trade_data_bin = raw_bin
#                                                 else:
#                                                     trade_data_bin = raw_bin

#                                                 if isinstance(trade_data_bin, dict):
#                                                     mint_address = trade_data_bin.get('mintAddress')
#                                                     market_cap_str = trade_data_bin.get('marketCap')
#                                                     if mint_address and market_cap_str:
#                                                         try:
#                                                             market_cap = float(market_cap_str)
#                                                             async with DB_CONNECTION_SEMAPHORE:
#                                                                 await asyncio.to_thread(update_token_mcap, mint_address, market_cap)
#                                                             async with DB_CONNECTION_SEMAPHORE:
#                                                                 await asyncio.to_thread(update_token_ath_if_needed, mint_address, market_cap)
#                                                             user_addr_b = trade_data_bin.get('userAddress') or trade_data_bin.get('user_address')
#                                                             if user_addr_b:
#                                                                 async with DB_CONNECTION_SEMAPHORE:
#                                                                     await asyncio.to_thread(update_token_dev_activity, mint_address, user_addr_b, trade_data_bin)
#                                                         except ValueError:
#                                                             print(f"Invalid marketCap value (binary): {market_cap_str}")
#                                                 else:
#                                                     print("Parsed binary trade payload is not a dict; skipping mcap/ath update")

#                                                 # Relay to connected clients
#                                                 for client in connected_clients.copy():
#                                                     try:
#                                                         await client.send_text(json.dumps({
#                                                             "type": "unified_trade",
#                                                             "data": trade_data_bin,
#                                                             "timestamp": datetime.now().isoformat()
#                                                         }))
#                                                     except Exception:
#                                                         connected_clients.remove(client)
#                                     except Exception as e:
#                                         print(f"Failed to parse binary MSG payload: {e}")

#                                 elif text_data.startswith("+OK"):
#                                     print("Received +OK (binary)")

#                             except UnicodeDecodeError:
#                                 print("Binary data is not text")

#                         elif message.type == aiohttp.WSMsgType.CLOSED:
#                             print(f"Unified connection closed: code={message.data}, reason={message.extra}")
#                             break
#                         elif message.type == aiohttp.WSMsgType.ERROR:
#                             print(f"Unified WebSocket error: {websocket.exception()}")
#                             break

#                     # Cleanup
#                     if ping_task:
#                         ping_task.cancel()
#                         try:
#                             await ping_task
#                         except asyncio.CancelledError:
#                             pass
#                     if sync_task:
#                         sync_task.cancel()
#                         try:
#                             await sync_task
#                         except asyncio.CancelledError:
#                             pass
#                     print("Unified WebSocket connection ended, cleaning up")

#             except Exception as e:
#                 print(f"Unified connection error: {e}, will retry in 5 seconds")
#                 await asyncio.sleep(5)



#         # fetch_and_relay_axiom was previously nested here; moved to module level below


# def _create_minimal_token_payload(token: Token, changed_fields: list = None) -> dict:
#     """
#     Create a minimal token payload for broadcasting with only essential fields.
#     If changed_fields is provided, only include those fields plus mint_address.
#     Maintains nested structure for easy frontend merging.
#     """
#     payload = {
#         "mint_address": token.mint_address,
#     }
    
#     if changed_fields:
#         # Only include the changed fields, maintaining nested structure
#         for field in changed_fields:
#             if field == "mcap":
#                 payload["mcap"] = token.mcap
#             elif field == "ath":
#                 payload["ath"] = token.ath
#             elif field == "progress":
#                 payload["progress"] = token.progress
#             elif field == "dev_activity":
#                 payload["dev_activity"] = token.dev_activity
#             elif field == "creator_balance_sol":
#                 payload["creator_balance_sol"] = token.creator_balance_sol
#             elif field == "creator_balance_usd":
#                 payload["creator_balance_usd"] = token.creator_balance_usd
#             elif field == "price_change_5m":
#                 if "price_changes" not in payload:
#                     payload["price_changes"] = {}
#                 payload["price_changes"]["5m"] = token.price_change_5m
#             elif field == "price_change_1h":
#                 if "price_changes" not in payload:
#                     payload["price_changes"] = {}
#                 payload["price_changes"]["1h"] = token.price_change_1h
#             elif field == "price_change_6h":
#                 if "price_changes" not in payload:
#                     payload["price_changes"] = {}
#                 payload["price_changes"]["6h"] = token.price_change_6h
#             elif field == "price_change_24h":
#                 if "price_changes" not in payload:
#                     payload["price_changes"] = {}
#                 payload["price_changes"]["24h"] = token.price_change_24h
#             elif field == "traders_5m":
#                 if "traders" not in payload:
#                     payload["traders"] = {}
#                 payload["traders"]["5m"] = token.traders_5m
#             elif field == "traders_1h":
#                 if "traders" not in payload:
#                     payload["traders"] = {}
#                 payload["traders"]["1h"] = token.traders_1h
#             elif field == "traders_6h":
#                 if "traders" not in payload:
#                     payload["traders"] = {}
#                 payload["traders"]["6h"] = token.traders_6h
#             elif field == "traders_24h":
#                 if "traders" not in payload:
#                     payload["traders"] = {}
#                 payload["traders"]["24h"] = token.traders_24h
#             elif field == "volume_5m":
#                 if "volume" not in payload:
#                     payload["volume"] = {}
#                 payload["volume"]["5m"] = token.volume_5m
#             elif field == "volume_1h":
#                 if "volume" not in payload:
#                     payload["volume"] = {}
#                 payload["volume"]["1h"] = token.volume_1h
#             elif field == "volume_6h":
#                 if "volume" not in payload:
#                     payload["volume"] = {}
#                 payload["volume"]["6h"] = token.volume_6h
#             elif field == "volume_24h":
#                 if "volume" not in payload:
#                     payload["volume"] = {}
#                 payload["volume"]["24h"] = token.volume_24h
#             elif field == "txns_5m":
#                 if "txns" not in payload:
#                     payload["txns"] = {}
#                 payload["txns"]["5m"] = token.txns_5m
#             elif field == "txns_1h":
#                 if "txns" not in payload:
#                     payload["txns"] = {}
#                 payload["txns"]["1h"] = token.txns_1h
#             elif field == "txns_6h":
#                 if "txns" not in payload:
#                     payload["txns"] = {}
#                 payload["txns"]["6h"] = token.txns_6h
#             elif field == "txns_24h":
#                 if "txns" not in payload:
#                     payload["txns"] = {}
#                 payload["txns"]["24h"] = token.txns_24h
#             elif field == "candle_data":
#                 payload["candle_data"] = token.candle_data
#             elif field == "reply_count":
#                 if "activity" not in payload:
#                     payload["activity"] = {}
#                 payload["activity"]["reply_count"] = token.reply_count
#             elif field == "last_reply":
#                 if "activity" not in payload:
#                     payload["activity"] = {}
#                 payload["activity"]["last_reply"] = token.last_reply.isoformat() if token.last_reply else None
#             elif field == "last_trade_timestamp":
#                 if "activity" not in payload:
#                     payload["activity"] = {}
#                 payload["activity"]["last_trade_timestamp"] = token.last_trade_timestamp.isoformat() if token.last_trade_timestamp else None
#             elif field == "top_holders":
#                 if "holders" not in payload:
#                     payload["holders"] = {}
#                 payload["holders"]["top_holders"] = token.top_holders
#             elif field == "creator_holding_amount":
#                 if "holders" not in payload:
#                     payload["holders"] = {}
#                 payload["holders"]["creator_holding_amount"] = token.creator_holding_amount
#             elif field == "creator_holding_percentage":
#                 if "holders" not in payload:
#                     payload["holders"] = {}
#                 payload["holders"]["creator_holding_percentage"] = token.creator_holding_percentage
#             elif field == "creator_is_top_holder":
#                 if "holders" not in payload:
#                     payload["holders"] = {}
#                 payload["holders"]["creator_is_top_holder"] = token.creator_is_top_holder
#     else:
#         # Include all fields if no specific changes specified
#         payload.update({
#             "name": token.name,
#             "symbol": token.symbol,
#             "image_url": token.image_url,
#             "age": token.age.isoformat() if token.age else None,
#             "mcap": token.mcap,
#             "ath": token.ath,
#             "dev_activity": token.dev_activity,
#             "created_coin_count": token.created_coin_count,
#             "creator_balance_sol": token.creator_balance_sol,
#             "creator_balance_usd": token.creator_balance_usd,
#             "creator": token.creator,
#             "total_supply": token.total_supply,
#             "pump_swap_pool": token.pump_swap_pool,
#             "viewers": token.viewers,
#             "progress": token.progress,
#             "liquidity": token.liquidity,
#             "is_live": token.is_live,
#             "is_active": token.is_active,
#             "nsfw": token.nsfw,
#             "social_links": token.social_links,
#             "price_changes": {
#                 "5m": token.price_change_5m,
#                 "1h": token.price_change_1h,
#                 "6h": token.price_change_6h,
#                 "24h": token.price_change_24h,
#             },
#             "traders": {
#                 "5m": token.traders_5m,
#                 "1h": token.traders_1h,
#                 "6h": token.traders_6h,
#                 "24h": token.traders_24h,
#             },
#             "volume": {
#                 "5m": token.volume_5m,
#                 "1h": token.volume_1h,
#                 "6h": token.volume_6h,
#                 "24h": token.volume_24h,
#             },
#             "txns": {
#                 "5m": token.txns_5m,
#                 "1h": token.txns_1h,
#                 "6h": token.txns_6h,
#                 "24h": token.txns_24h,
#             },
#             "candle_data": token.candle_data,
#             "pool_info": {
#                 "raydium_pool": token.raydium_pool,
#                 "virtual_sol_reserves": token.virtual_sol_reserves,
#                 "real_sol_reserves": token.real_sol_reserves,
#                 "virtual_token_reserves": token.virtual_token_reserves,
#                 "real_token_reserves": token.real_token_reserves,
#                 "complete": token.complete,
#             },
#             "activity": {
#                 "reply_count": token.reply_count,
#                 "last_reply": token.last_reply.isoformat() if token.last_reply else None,
#                 "last_trade_timestamp": token.last_trade_timestamp.isoformat() if token.last_trade_timestamp else None,
#             },
#             "holders": {
#                 "top_holders": token.top_holders,
#                 "creator_holding_amount": token.creator_holding_amount,
#                 "creator_holding_percentage": token.creator_holding_percentage,
#                 "creator_is_top_holder": token.creator_is_top_holder,
#             },
#             "timestamps": {
#                 "created_at": token.created_at.isoformat(),
#                 "updated_at": token.updated_at.isoformat(),
#             },
#         })
    
#     return payload


# def _create_complete_token_payload(token: Token, event_type: str = "token_updated") -> dict:
#     """
#     Create a complete token payload for broadcasting with all fields.
#     This ensures consistent data structure across all token update events.
#     """
#     return {
#         "type": event_type,
#         "data": {
#             "id": token.id,
#             "mint_address": token.mint_address,
#             "name": token.name,
#             "symbol": token.symbol,
#             "image_url": token.image_url,
#             "age": token.age.isoformat() if token.age else None,
#             "mcap": token.mcap,
#             "ath": token.ath,
#             "dev_activity": token.dev_activity,
#             "created_coin_count": token.created_coin_count,
#             "creator_balance_sol": token.creator_balance_sol,
#             "creator_balance_usd": token.creator_balance_usd,
#             "creator": token.creator,
#             "total_supply": token.total_supply,
#             "pump_swap_pool": token.pump_swap_pool,
#             "viewers": token.viewers,
#             "progress": token.progress,
#             "liquidity": token.liquidity,
#             "is_live": token.is_live,
#             "is_active": token.is_active,
#             "nsfw": token.nsfw,
#             "social_links": token.social_links,
#             "price_changes": {
#                 "5m": token.price_change_5m,
#                 "1h": token.price_change_1h,
#                 "6h": token.price_change_6h,
#                 "24h": token.price_change_24h,
#             },
#             "traders": {
#                 "5m": token.traders_5m,
#                 "1h": token.traders_1h,
#                 "6h": token.traders_6h,
#                 "24h": token.traders_24h,
#             },
#             "volume": {
#                 "5m": token.volume_5m,
#                 "1h": token.volume_1h,
#                 "6h": token.volume_6h,
#                 "24h": token.volume_24h,
#             },
#             "txns": {
#                 "5m": token.txns_5m,
#                 "1h": token.txns_1h,
#                 "6h": token.txns_6h,
#                 "24h": token.txns_24h,
#             },
#             "candle_data": token.candle_data,
#             "pool_info": {
#                 "raydium_pool": token.raydium_pool,
#                 "virtual_sol_reserves": token.virtual_sol_reserves,
#                 "real_sol_reserves": token.real_sol_reserves,
#                 "virtual_token_reserves": token.virtual_token_reserves,
#                 "real_token_reserves": token.real_token_reserves,
#                 "complete": token.complete,
#             },
#             "activity": {
#                 "reply_count": token.reply_count,
#                 "last_reply": token.last_reply.isoformat() if token.last_reply else None,
#                 "last_trade_timestamp": token.last_trade_timestamp.isoformat() if token.last_trade_timestamp else None,
#             },
#             "holders": {
#                 "top_holders": token.top_holders,
#                 "creator_holding_amount": token.creator_holding_amount,
#                 "creator_holding_percentage": token.creator_holding_percentage,
#                 "creator_is_top_holder": token.creator_is_top_holder,
#             },
#             "timestamps": {
#                 "created_at": token.created_at.isoformat(),
#                 "updated_at": token.updated_at.isoformat(),
#             },
#         },
#     }

# async def fetch_and_relay_livestreams(connected_clients):
#     uri = "wss://prod-v2.nats.realtime.pump.fun/"
#     extra_headers = {
#         "Connection": "Upgrade",
#         "Pragma": "no-cache",
#         "Cache-Control": "no-cache",
#         "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
#         "Upgrade": "websocket",
#         "Origin": "https://pump.fun",
#         "Accept-Encoding": "gzip, deflate, br",
#         "Accept-Language": "en-US,en;q=0.9",
#         "Cookie": "GA1.1.1379899687.1757300687; intercom-id-w7scljv7=b1704389-09ba-49cb-a4b0-f0bd59d00da6; intercom-session-w7scljv7=; intercom-device-id-w7scljv7=5daab4f2-5559-42b8-97d0-c8898ab0d3e4; mp_567f3c2d509af79e6c5694b2d20439d2_mixpanel=%7B%22distinct_id%22%3A%22%24device%3A1c440d2f-677e-4948-8c6c-46e8a219718a%22%2C%22%24device_id%22%3A%221c440d2f-677e-4948-8c6c-46e8a219718a%22%2C%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D; _cfuvid=4Je2MlFk44ZteLfx3R2IaGbVgdIqN__QgprYh0JpNQk-1757379935151-0.0.1.1-604800000; _fs_dwell_passed=4572f959-b96e-46a4-8640-ba9dc76cc77b; fs_lua=1.1757401202929; fs_uid=#o-1YWTMD-na1#b6affcd0-891a-418b-b313-49f38d7ec743:22a5b1f9-6c5e-44da-be6c-1aba6fc378e2:1757401202929::1#/1788836708; __cf_bm=Xxp9b7xgB_vZqCpPcRL5fy4LzkZ80d1P_APveeuHgRs-1757401333-1.0.1.1-kfi6lSR6gn2ikvQvb4Zz2xUCtHHNLQ2Ai8G2_MFDuhUH1ax9pTu0ddAQzQQZEtji3_d6Nmm1jwy6i3tjKeJJ03nEIpHlkT.A1k.sHFmMPDw; _ga_T65NVS2TQ6=GS2.1.s1757401332$o6$g0$t1757401332$j60$l0$h0; cf_clearance=2uV3ZtRZ5vMDUrclzSO.W0uu10mlHNFOiN4Xuh0LJeM-1757401337-1.2.1.1-h.iZWJrpx20Yaj4zty5Hysjxh4a9wILFmOqsB3jLN8oJJOhL5Cz3ZMP2YxNnw_qRwlEz2bMVlRBE7NoPjx0anhEZF_5JFJxe7AyhPgYyOLgpVmnoESGZNOLr0NRgJz1DnYZk19xYU17YWG1if38OOAb48ekKN7NMNtJNyO2Gw2CejCwm1VG2Fhb3r7.pvoJ6Jdhr.FgPlAxwn8Vnhr.maO_MwtGtrTegW7zXVcoWMqM",
#     }

#     async with aiohttp.ClientSession(
#         timeout=aiohttp.ClientTimeout(total=None)
#     ) as session:
#         while True:
#             try:
#                 async with session.ws_connect(uri, headers=extra_headers) as websocket:
#                     # print("Connected to external WebSocket")

#                     # Variables to track connection state
#                     info_received = False
#                     connect_sent = False
#                     subs_sent = False
#                     ping_interval = 25000  # Default ping interval in milliseconds
#                     ping_timeout = 20000  # Default ping timeout in milliseconds
#                     last_ping_time = None

#                     # Start ping task
#                     ping_task = None

#                     async def ping_handler():
#                         nonlocal last_ping_time
#                         while True:
#                             await asyncio.sleep(
#                                 ping_interval / 1000
#                             )  # Convert to seconds
#                             try:
#                                 # print("Sending scheduled PING")
#                                 await websocket.send_bytes(b"PING\r\n")
#                                 last_ping_time = asyncio.get_event_loop().time()
#                             except Exception as e:
#                                 # print(f"Error sending ping: {e}")
#                                 break

#                     # print("Listening for messages...")

#                     async for message in websocket:
#                         if message.type == aiohttp.WSMsgType.TEXT:
#                             data = message.data
#                             if isinstance(data, bytes):
#                                 data = data.decode("utf-8")
#                             # print(f"Received message: {data}")

#                             # Step 1: Wait for INFO message
#                             if not info_received and data.startswith("INFO"):
#                                 try:
#                                     # Parse INFO message to extract connection details
#                                     info_json = data[4:].strip()  # Remove "INFO" prefix
#                                     info_data = json.loads(info_json)
#                                     logger.info("Received INFO from prod-v2: %s", info_data)

#                                     # Extract ping interval and timeout if available
#                                     if "ping_interval" in info_data:
#                                         ping_interval = info_data["ping_interval"]
#                                         logger.debug("Updated ping_interval=%sms", ping_interval)
#                                     if "ping_timeout" in info_data:
#                                         ping_timeout = info_data["ping_timeout"]
#                                         logger.debug("Updated ping_timeout=%sms", ping_timeout)

#                                     info_received = True

#                                     # Step 2: Send CONNECT after receiving INFO
#                                     connect_payload = {
#                                         "no_responders": True,
#                                         "protocol": 1,
#                                         "verbose": False,
#                                         "pedantic": False,
#                                         "user": "subscriber",
#                                         "pass": "lW5a9y20NceF6AE9",
#                                         "lang": "nats.ws",
#                                         "version": "1.30.3",
#                                         "headers": True,
#                                     }
#                                     logger.info("Sending CONNECT to prod-v2: %s", connect_payload)
#                                     await websocket.send_bytes(("CONNECT " + json.dumps(connect_payload) + "\r\n").encode("utf-8"))
#                                     connect_sent = True

#                                     # Send SUBs immediately after CONNECT (since +OK may not be sent)
#                                     if not subs_sent:
#                                         sub1 = "SUB newCoinCreated.prod 1\r\n"
#                                         sub2 = "SUB pump.fun.livestream 2\r\n"
#                                         logger.info("Sending SUBs to prod-v2: %s | %s", sub1.strip(), sub2.strip())
#                                         await websocket.send_bytes(sub1.encode("utf-8"))
#                                         await websocket.send_bytes(sub2.encode("utf-8"))
#                                         subs_sent = True

#                                         # Start ping handler after subs are sent
#                                         ping_task = asyncio.create_task(ping_handler())
#                                         logger.debug("Started ping handler for prod-v2 connection")
#                                 except json.JSONDecodeError as e:
#                                     logger.warning("Failed to parse INFO message from prod-v2: %s", e)

#                             # Handle +OK acknowledgment
#                             elif data.startswith("+OK"):
#                                 logger.debug("Received +OK from prod-v2")

#                             # Handle PING messages
#                             elif data.strip() == "PING":
#                                 logger.debug("Received PING from prod-v2, sending PONG")
#                                 await websocket.send_bytes(b"PONG\r\n")

#                             # Handle PONG messages
#                             elif data.strip() == "PONG":
#                                 logger.debug("Received PONG from prod-v2")
#                                 if last_ping_time:
#                                     response_time = (asyncio.get_event_loop().time() - last_ping_time) * 1000
#                                     logger.debug("prod-v2 ping response time: %.2fms", response_time)

#                             # Handle Socket.IO messages (if any)
#                             elif data.startswith("0"):
#                                 # Socket.IO handshake
#                                 try:
#                                     socketio_data = json.loads(data[1:])
#                                     if "pingInterval" in socketio_data:
#                                         ping_interval = socketio_data["pingInterval"]
#                                         # print(f"Socket.IO ping interval: {ping_interval}ms")
#                                     # print(f"Socket.IO handshake: {socketio_data}")
#                                 except:
#                                     # print(f"Socket.IO message: {data}")
#                                     pass

#                             elif data.startswith("42"):
#                                 # Parse Socket.IO event message: 42[event, data]
#                                 try:
#                                     json_str = data[2:]
#                                     event_data = json.loads(json_str)
#                                     if (
#                                         isinstance(event_data, list)
#                                         and len(event_data) >= 2
#                                     ):
#                                         event_name = event_data[0]
#                                         payload = event_data[1]
#                                         # print(f"Received Socket.IO event: {event_name}")
#                                         # print(f"Payload: {json.dumps(payload, indent=2)}")

#                                         # Forward raw payload for all Socket.IO events
#                                         organized_data = payload
#                                         logger.debug("Socket.IO event %s payload: %s", event_name, json.dumps(payload)[:1000])

#                                         # Relay the organized payload to all connected clients
#                                         for client in connected_clients.copy():
#                                             try:
#                                                 await client.send_text(json.dumps(organized_data))
#                                             except Exception:
#                                                 connected_clients.remove(client)
#                                 except json.JSONDecodeError as e:
#                                     # print(f"Failed to parse Socket.IO message: {e}")
#                                     pass

#                             # Handle NATS messages (MSG format)
#                             elif data.startswith("MSG"):
#                                 # print(f"Received NATS message: {data}")
#                                 # Parse MSG message: MSG <subject> <sid> <size>\r\n<payload>
#                                 lines = data.split("\r\n")
#                                 if len(lines) >= 2:
#                                     msg_line = lines[0]
#                                     payload = lines[1]
#                                     logger.debug("Received NATS MSG: %s", msg_line)
#                                     try:
#                                         msg_data = json.loads(payload)
#                                         logger.debug("Parsed MSG payload (truncated): %s", json.dumps(msg_data)[:1000])

#                                         # Relay the raw MSG payload to all connected clients
#                                         for client in connected_clients.copy():
#                                             try:
#                                                 await client.send_text(json.dumps(msg_data))
#                                             except Exception:
#                                                 connected_clients.remove(client)
#                                     except json.JSONDecodeError as e:
#                                         logger.warning("Failed to parse MSG payload: %s -- raw payload: %s", e, payload[:1000])
#                                 else:
#                                     # print("MSG message format incorrect")
#                                     pass

#                             # Handle other NATS protocol messages
#                             elif data.startswith("+OK"):
#                                 pass
#                                 # print("Received +OK acknowledgment")
#                             elif data.startswith("-ERR"):
#                                 logger.error("Received NATS -ERR from prod-v2: %s", data)

#                         elif message.type == aiohttp.WSMsgType.BINARY:
#                             data = message.data
#                             # print(f"Received binary message: {len(data)} bytes")
#                             try:
#                                 text_data = data.decode("utf-8")
#                                 # print(f"Binary as text: {text_data}")

#                                 # Handle INFO in binary
#                                 if not info_received and text_data.startswith("INFO"):
#                                     try:
#                                         # Parse INFO message to extract connection details
#                                         info_json = text_data[4:].strip()  # Remove "INFO" prefix
#                                         info_data = json.loads(info_json)
#                                         #logger.info("Received INFO (binary) from prod-v2: %s", info_data)

#                                         # Extract ping interval and timeout if available
#                                         if "ping_interval" in info_data:
#                                             ping_interval = info_data["ping_interval"]
#                                             # print(f"Updated ping interval to: {ping_interval}ms")
#                                         if "ping_timeout" in info_data:
#                                             ping_timeout = info_data["ping_timeout"]
#                                             # print(f"Updated ping timeout to: {ping_timeout}ms")

#                                         info_received = True

#                                         # Step 2: Send CONNECT after receiving INFO
#                                         connect_payload = {
#                                             "no_responders": True,
#                                             "protocol": 1,
#                                             "verbose": False,
#                                             "pedantic": False,
#                                             "user": "subscriber",
#                                             "pass": "lW5a9y20NceF6AE9",
#                                             "lang": "nats.ws",
#                                             "version": "1.30.3",
#                                             "headers": True,
#                                         }
#                                         logger.info("Sending CONNECT (binary) to prod-v2: %s", connect_payload)
#                                         await websocket.send_bytes(("CONNECT " + json.dumps(connect_payload) + "\r\n").encode("utf-8"))
#                                         connect_sent = True

#                                         # Send SUBs immediately after CONNECT (since +OK may not be sent)
#                                         if not subs_sent:
#                                             sub1 = "SUB newCoinCreated.prod 1\r\n"
#                                             sub2 = "SUB pump.fun.livestream 2\r\n"
#                                             logger.info("Sending SUBs (binary) to prod-v2: %s | %s", sub1.strip(), sub2.strip())
#                                             await websocket.send_bytes(sub1.encode("utf-8"))
#                                             await websocket.send_bytes(sub2.encode("utf-8"))
#                                             subs_sent = True

#                                             # Start ping handler after subs are sent
#                                             ping_task = asyncio.create_task(ping_handler())
#                                             logger.debug("Started ping handler (binary) for prod-v2")
#                                     except json.JSONDecodeError:
#                                         # print(f"Failed to parse INFO message: {e}")
#                                         pass

#                                 # Handle binary PING
#                                 elif text_data.strip() == "PING":
#                                     # print("Received binary PING, sending PONG")
#                                     await websocket.send_bytes(b"PONG\r\n")

#                                 # Handle +OK in binary
#                                 elif text_data.startswith("+OK"):
#                                     # print("Received +OK acknowledgment (binary)")
#                                     pass
#                             except UnicodeDecodeError:
#                                 # print(f"Binary data (non-text): {data[:100]}...")
#                                 pass  # Show first 100 bytes

#                         elif message.type == aiohttp.WSMsgType.CLOSED:
#                             # print(f"Connection closed with code: {message.data}, reason: {message.extra}")
#                             break
#                         elif message.type == aiohttp.WSMsgType.ERROR:
#                             # print(f"WebSocket error: {websocket.exception()}")
#                             break

#                     # Cleanup ping task
#                     if ping_task:
#                         ping_task.cancel()
#                         try:
#                             await ping_task
#                         except asyncio.CancelledError:
#                             pass

#                     # print("Connection loop ended")
#                 # print("WebSocket context exited")

#             except Exception as e:
#                 # print(f"Connection error: {e}")
#                 await asyncio.sleep(5)  # Retry after 5 seconds
