import asyncio
import json
import os
from datetime import datetime

import aiohttp
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from ..models import get_db
from .database_sync_service import DatabaseSyncService
from .event_broadcaster import broadcaster


load_dotenv()

# No cumulative counter: we only log the count fetched per poll

# Global sync service instance (no longer needed)
sync_service = None


def save_streamed_data_to_json(data, filename="streamed_data.json"):
    """Save the complete streamed data structure to a JSON file for inspection"""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"💾 Saved complete streamed data to {filename}")
    except Exception as e:
        print(f"❌ Error saving data to JSON: {e}")


async def poll_live_tokens():
    """Continuously poll for live tokens and sync with database"""
    global sync_service

    cookie = os.getenv("COOKIE")
    username = os.getenv("USERNAME")

    if not cookie:
        print("❌ Error: Cookie not found in .env file")
        return

    url = "https://frontend-api-v3.pump.fun/coins/currently-live?offset=0&limit=2000&sort=currently_live&order=DESC&includeNsfw=false"
    headers = {
        "Host": "frontend-api-v3.pump.fun",
        "Cookie": cookie,
        "Sec-Ch-Ua-Platform": "macOS",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
        "Content-Type": "application/json",
        "Sec-Ch-Ua-Mobile": "?0",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Origin": "https://pump.fun",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://pump.fun/",
        "Accept-Encoding": "gzip, deflate, br",
        "Priority": "u=1, i",
    }

    poll_interval = 2  # Poll every 2 seconds as requested
    last_data_hash = None

    async with aiohttp.ClientSession() as session:
        while True:
            # Create fresh database session and sync service for each polling cycle
            db = next(get_db())
            sync_service = DatabaseSyncService(
                db, max_workers=15  # Balanced concurrency for performance and stability
            )

            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        fetched_count = len(data) if isinstance(data, list) else 0
                        print(f"📥 Fetched {fetched_count if fetched_count else 'N/A'} tokens from pump.fun")

                        if isinstance(data, list) and len(data) > 2:
                            print(f"🔄 ... and {len(data) - 2} more tokens")

                        # Always process the live tokens data to ensure fresh updates
                        if isinstance(data, list) and data:
                            # Calculate data hash to detect changes for logging purposes
                            current_data_hash = json.dumps(data, sort_keys=True)
                            data_changed = current_data_hash != last_data_hash
                            
                            if data_changed:
                                print(f"🔄 Live tokens changed! Processing {len(data)} tokens")
                            else:
                                print(f"🔄 Processing {len(data)} live tokens (continuous updates)")

                            # Always sync with database to ensure fresh data
                            max_retries = 3
                            sync_successful = False
                            for attempt in range(max_retries):
                                try:
                                    sync_stats = await sync_service.sync_live_tokens(data)
                                    print(f"⚡ Database sync completed: {sync_stats}")
                                    sync_successful = True
                                    break  # Success, exit retry loop
                                except Exception as e:
                                    print(f"❌ Database sync error (attempt {attempt + 1}/{max_retries}): {e}")
                                    if attempt < max_retries - 1:
                                        print("⏳ Retrying database sync in 2 seconds...")
                                        await asyncio.sleep(2)
                                    else:
                                        print("❌ Max retries reached, skipping this sync cycle")
                                        break

                            # ATH updates are handled exclusively by DatabaseSyncService to avoid races

                            # Save sample data for inspection
                            if len(data) > 0:
                                save_streamed_data_to_json(
                                    data[:5], "latest_tokens_sample.json"
                                )

                            last_data_hash = current_data_hash
                            print(f"✅ Database synchronized with {len(data)} live tokens")
                        else:
                            print(f"✅ No valid live tokens data received")
                    elif response.status == 429:
                        print(f"⚠️ Rate limited (429)! Pausing polling for 40 seconds...")
                        await asyncio.sleep(40)
                        continue  # Skip the normal sleep interval and retry immediately
                    else:
                        print(
                            f"❌ Failed to fetch live tokens, status: {response.status}"
                        )

            except Exception as e:
                print(f"❌ Polling error: {str(e)}")
                # Clean up database session and sync service on error
                try:
                    db.close()
                except:
                    pass
                try:
                    sync_service.cleanup()
                except:
                    pass
                # Continue polling even on errors

            # Clean up database session and sync service after successful operation
            try:
                db.close()
            except:
                pass
            try:
                sync_service.cleanup()
            except:
                pass

            # Wait before next poll
            await asyncio.sleep(poll_interval)


def cleanup():
    """Cleanup resources"""
    global sync_service
    if sync_service:
        sync_service.cleanup()
