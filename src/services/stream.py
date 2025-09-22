import asyncio
import json
import logging
from datetime import datetime

import aiohttp

from src.models.database import SessionLocal
from src.models.token import Token

logger = logging.getLogger(__name__)
connected_clients = set()


def update_token_mcap(mint_address: str, market_cap: float):
    """Update the market cap of a token in the database if it exists."""
    db = SessionLocal()
    try:
        token = db.query(Token).filter(Token.mint_address == mint_address).first()
        if token:
            #print(f"Found Token")
            token.mcap = market_cap
            db.commit()
            #print(f"Updated mcap for {mint_address} to {market_cap}")
        else:
            pass
    except Exception as e:
        print(f"Error updating token mcap: {e}")
    finally:
        db.close()


def update_token_ath_if_needed(mint_address: str, market_cap: float):
    """Update ATH and progress if mcap exceeds current ATH."""
    db = SessionLocal()
    try:
        token = db.query(Token).filter(Token.mint_address == mint_address).first()
        if token and token.ath > 0.0 and market_cap > token.ath:
            token.ath = market_cap
            token.progress = (market_cap / token.ath) * 100
            db.commit()
            #print(f"Updated ATH for {mint_address} to {market_cap}, progress: {token.progress:.2f}%")
        elif token:
            # Always recalculate progress even if ATH not updated
            token.progress = (market_cap / token.ath) * 100 if token.ath > 0.0 else 0.0
            db.commit()
            #print(f"Recalculated progress for {mint_address}: {token.progress:.2f}%")
    except Exception as e:
        print(f"Error updating token ATH/progress: {e}")
    finally:
        db.close()


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

                    # fetch_creator_balance_for_mint is async; we're running inside a thread
                    # scheduling a short-lived event loop here as a best-effort fetch.
                    balance_info = asyncio.run(fetch_creator_balance_for_mint(user_address, mint_address))
                    if balance_info:
                        token.creator_balance_sol = balance_info.get("balance")
                        token.creator_balance_usd = balance_info.get("balanceUSD")
                        logger.info(
                            f"Updated creator balance for {mint_address} after dev_activity change: "
                            f"sol={token.creator_balance_sol} usd={token.creator_balance_usd} (creator={user_address})"
                        )
                except Exception as e:
                    # If asyncio.run fails inside thread or network error, just log and continue
                    logger.warning(f"Failed to fetch creator balance during dev_activity update for {mint_address}: {e}")

                db.commit()
                logger.info(f"Updated dev_activity for {mint_address}: {dev_obj}")
    except Exception as e:
        print(f"Error updating dev_activity for {mint_address}: {e}")
    finally:
        db.close()


async def fetch_and_relay_livestreams(connected_clients):
    uri = "wss://prod-v2.nats.realtime.pump.fun/"
    extra_headers = {
        "Connection": "Upgrade",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Upgrade": "websocket",
        "Origin": "https://pump.fun",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cookie": "GA1.1.1379899687.1757300687; intercom-id-w7scljv7=b1704389-09ba-49cb-a4b0-f0bd59d00da6; intercom-session-w7scljv7=; intercom-device-id-w7scljv7=5daab4f2-5559-42b8-97d0-c8898ab0d3e4; mp_567f3c2d509af79e6c5694b2d20439d2_mixpanel=%7B%22distinct_id%22%3A%22%24device%3A1c440d2f-677e-4948-8c6c-46e8a219718a%22%2C%22%24device_id%22%3A%221c440d2f-677e-4948-8c6c-46e8a219718a%22%2C%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D; _cfuvid=4Je2MlFk44ZteLfx3R2IaGbVgdIqN__QgprYh0JpNQk-1757379935151-0.0.1.1-604800000; _fs_dwell_passed=4572f959-b96e-46a4-8640-ba9dc76cc77b; fs_lua=1.1757401202929; fs_uid=#o-1YWTMD-na1#b6affcd0-891a-418b-b313-49f38d7ec743:22a5b1f9-6c5e-44da-be6c-1aba6fc378e2:1757401202929::1#/1788836708; __cf_bm=Xxp9b7xgB_vZqCpPcRL5fy4LzkZ80d1P_APveeuHgRs-1757401333-1.0.1.1-kfi6lSR6gn2ikvQvb4Zz2xUCtHHNLQ2Ai8G2_MFDuhUH1ax9pTu0ddAQzQQZEtji3_d6Nmm1jwy6i3tjKeJJ03nEIpHlkT.A1k.sHFmMPDw; _ga_T65NVS2TQ6=GS2.1.s1757401332$o6$g0$t1757401332$j60$l0$h0; cf_clearance=2uV3ZtRZ5vMDUrclzSO.W0uu10mlHNFOiN4Xuh0LJeM-1757401337-1.2.1.1-h.iZWJrpx20Yaj4zty5Hysjxh4a9wILFmOqsB3jLN8oJJOhL5Cz3ZMP2YxNnw_qRwlEz2bMVlRBE7NoPjx0anhEZF_5JFJxe7AyhPgYyOLgpVmnoESGZNOLr0NRgJz1DnYZk19xYU17YWG1if38OOAb48ekKN7NMNtJNyO2Gw2CejCwm1VG2Fhb3r7.pvoJ6Jdhr.FgPlAxwn8Vnhr.maO_MwtGtrTegW7zXVcoWMqM",
    }

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None)
    ) as session:
        while True:
            try:
                async with session.ws_connect(uri, headers=extra_headers) as websocket:
                    # print("Connected to external WebSocket")

                    # Variables to track connection state
                    info_received = False
                    connect_sent = False
                    subs_sent = False
                    ping_interval = 25000  # Default ping interval in milliseconds
                    ping_timeout = 20000  # Default ping timeout in milliseconds
                    last_ping_time = None

                    # Start ping task
                    ping_task = None

                    async def ping_handler():
                        nonlocal last_ping_time
                        while True:
                            await asyncio.sleep(
                                ping_interval / 1000
                            )  # Convert to seconds
                            try:
                                # print("Sending scheduled PING")
                                await websocket.send_bytes(b"PING\r\n")
                                last_ping_time = asyncio.get_event_loop().time()
                            except Exception as e:
                                # print(f"Error sending ping: {e}")
                                break

                    # print("Listening for messages...")

                    async for message in websocket:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            data = message.data
                            if isinstance(data, bytes):
                                data = data.decode("utf-8")
                            # print(f"Received message: {data}")

                            # Step 1: Wait for INFO message
                            if not info_received and data.startswith("INFO"):
                                try:
                                    # Parse INFO message to extract connection details
                                    info_json = data[4:].strip()  # Remove "INFO" prefix
                                    info_data = json.loads(info_json)
                                    logger.info("Received INFO from prod-v2: %s", info_data)

                                    # Extract ping interval and timeout if available
                                    if "ping_interval" in info_data:
                                        ping_interval = info_data["ping_interval"]
                                        logger.debug("Updated ping_interval=%sms", ping_interval)
                                    if "ping_timeout" in info_data:
                                        ping_timeout = info_data["ping_timeout"]
                                        logger.debug("Updated ping_timeout=%sms", ping_timeout)

                                    info_received = True

                                    # Step 2: Send CONNECT after receiving INFO
                                    connect_payload = {
                                        "no_responders": True,
                                        "protocol": 1,
                                        "verbose": False,
                                        "pedantic": False,
                                        "user": "subscriber",
                                        "pass": "lW5a9y20NceF6AE9",
                                        "lang": "nats.ws",
                                        "version": "1.30.3",
                                        "headers": True,
                                    }
                                    logger.info("Sending CONNECT to prod-v2: %s", connect_payload)
                                    await websocket.send_bytes(("CONNECT " + json.dumps(connect_payload) + "\r\n").encode("utf-8"))
                                    connect_sent = True

                                    # Send SUBs immediately after CONNECT (since +OK may not be sent)
                                    if not subs_sent:
                                        sub1 = "SUB newCoinCreated.prod 1\r\n"
                                        sub2 = "SUB pump.fun.livestream 2\r\n"
                                        logger.info("Sending SUBs to prod-v2: %s | %s", sub1.strip(), sub2.strip())
                                        await websocket.send_bytes(sub1.encode("utf-8"))
                                        await websocket.send_bytes(sub2.encode("utf-8"))
                                        subs_sent = True

                                        # Start ping handler after subs are sent
                                        ping_task = asyncio.create_task(ping_handler())
                                        logger.debug("Started ping handler for prod-v2 connection")
                                except json.JSONDecodeError as e:
                                    logger.warning("Failed to parse INFO message from prod-v2: %s", e)

                            # Handle +OK acknowledgment
                            elif data.startswith("+OK"):
                                logger.debug("Received +OK from prod-v2")

                            # Handle PING messages
                            elif data.strip() == "PING":
                                logger.debug("Received PING from prod-v2, sending PONG")
                                await websocket.send_bytes(b"PONG\r\n")

                            # Handle PONG messages
                            elif data.strip() == "PONG":
                                logger.debug("Received PONG from prod-v2")
                                if last_ping_time:
                                    response_time = (asyncio.get_event_loop().time() - last_ping_time) * 1000
                                    logger.debug("prod-v2 ping response time: %.2fms", response_time)

                            # Handle Socket.IO messages (if any)
                            elif data.startswith("0"):
                                # Socket.IO handshake
                                try:
                                    socketio_data = json.loads(data[1:])
                                    if "pingInterval" in socketio_data:
                                        ping_interval = socketio_data["pingInterval"]
                                        # print(f"Socket.IO ping interval: {ping_interval}ms")
                                    # print(f"Socket.IO handshake: {socketio_data}")
                                except:
                                    # print(f"Socket.IO message: {data}")
                                    pass

                            elif data.startswith("42"):
                                # Parse Socket.IO event message: 42[event, data]
                                try:
                                    json_str = data[2:]
                                    event_data = json.loads(json_str)
                                    if (
                                        isinstance(event_data, list)
                                        and len(event_data) >= 2
                                    ):
                                        event_name = event_data[0]
                                        payload = event_data[1]
                                        # print(f"Received Socket.IO event: {event_name}")
                                        # print(f"Payload: {json.dumps(payload, indent=2)}")

                                        # Forward raw payload for all Socket.IO events
                                        organized_data = payload
                                        logger.debug("Socket.IO event %s payload: %s", event_name, json.dumps(payload)[:1000])

                                        # Relay the organized payload to all connected clients
                                        for client in connected_clients.copy():
                                            try:
                                                await client.send_text(json.dumps(organized_data))
                                            except Exception:
                                                connected_clients.remove(client)
                                except json.JSONDecodeError as e:
                                    # print(f"Failed to parse Socket.IO message: {e}")
                                    pass

                            # Handle NATS messages (MSG format)
                            elif data.startswith("MSG"):
                                # print(f"Received NATS message: {data}")
                                # Parse MSG message: MSG <subject> <sid> <size>\r\n<payload>
                                lines = data.split("\r\n")
                                if len(lines) >= 2:
                                    msg_line = lines[0]
                                    payload = lines[1]
                                    logger.debug("Received NATS MSG: %s", msg_line)
                                    try:
                                        msg_data = json.loads(payload)
                                        logger.debug("Parsed MSG payload (truncated): %s", json.dumps(msg_data)[:1000])

                                        # Relay the raw MSG payload to all connected clients
                                        for client in connected_clients.copy():
                                            try:
                                                await client.send_text(json.dumps(msg_data))
                                            except Exception:
                                                connected_clients.remove(client)
                                    except json.JSONDecodeError as e:
                                        logger.warning("Failed to parse MSG payload: %s -- raw payload: %s", e, payload[:1000])
                                else:
                                    # print("MSG message format incorrect")
                                    pass

                            # Handle other NATS protocol messages
                            elif data.startswith("+OK"):
                                pass
                                # print("Received +OK acknowledgment")
                            elif data.startswith("-ERR"):
                                logger.error("Received NATS -ERR from prod-v2: %s", data)

                        elif message.type == aiohttp.WSMsgType.BINARY:
                            data = message.data
                            # print(f"Received binary message: {len(data)} bytes")
                            try:
                                text_data = data.decode("utf-8")
                                # print(f"Binary as text: {text_data}")

                                # Handle INFO in binary
                                if not info_received and text_data.startswith("INFO"):
                                    try:
                                        # Parse INFO message to extract connection details
                                        info_json = text_data[4:].strip()  # Remove "INFO" prefix
                                        info_data = json.loads(info_json)
                                        #logger.info("Received INFO (binary) from prod-v2: %s", info_data)

                                        # Extract ping interval and timeout if available
                                        if "ping_interval" in info_data:
                                            ping_interval = info_data["ping_interval"]
                                            # print(f"Updated ping interval to: {ping_interval}ms")
                                        if "ping_timeout" in info_data:
                                            ping_timeout = info_data["ping_timeout"]
                                            # print(f"Updated ping timeout to: {ping_timeout}ms")

                                        info_received = True

                                        # Step 2: Send CONNECT after receiving INFO
                                        connect_payload = {
                                            "no_responders": True,
                                            "protocol": 1,
                                            "verbose": False,
                                            "pedantic": False,
                                            "user": "subscriber",
                                            "pass": "lW5a9y20NceF6AE9",
                                            "lang": "nats.ws",
                                            "version": "1.30.3",
                                            "headers": True,
                                        }
                                        logger.info("Sending CONNECT (binary) to prod-v2: %s", connect_payload)
                                        await websocket.send_bytes(("CONNECT " + json.dumps(connect_payload) + "\r\n").encode("utf-8"))
                                        connect_sent = True

                                        # Send SUBs immediately after CONNECT (since +OK may not be sent)
                                        if not subs_sent:
                                            sub1 = "SUB newCoinCreated.prod 1\r\n"
                                            sub2 = "SUB pump.fun.livestream 2\r\n"
                                            logger.info("Sending SUBs (binary) to prod-v2: %s | %s", sub1.strip(), sub2.strip())
                                            await websocket.send_bytes(sub1.encode("utf-8"))
                                            await websocket.send_bytes(sub2.encode("utf-8"))
                                            subs_sent = True

                                            # Start ping handler after subs are sent
                                            ping_task = asyncio.create_task(ping_handler())
                                            logger.debug("Started ping handler (binary) for prod-v2")
                                    except json.JSONDecodeError:
                                        # print(f"Failed to parse INFO message: {e}")
                                        pass

                                # Handle binary PING
                                elif text_data.strip() == "PING":
                                    # print("Received binary PING, sending PONG")
                                    await websocket.send_bytes(b"PONG\r\n")

                                # Handle +OK in binary
                                elif text_data.startswith("+OK"):
                                    # print("Received +OK acknowledgment (binary)")
                                    pass
                            except UnicodeDecodeError:
                                # print(f"Binary data (non-text): {data[:100]}...")
                                pass  # Show first 100 bytes

                        elif message.type == aiohttp.WSMsgType.CLOSED:
                            # print(f"Connection closed with code: {message.data}, reason: {message.extra}")
                            break
                        elif message.type == aiohttp.WSMsgType.ERROR:
                            # print(f"WebSocket error: {websocket.exception()}")
                            break

                    # Cleanup ping task
                    if ping_task:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

                    # print("Connection loop ended")
                # print("WebSocket context exited")

            except Exception as e:
                # print(f"Connection error: {e}")
                await asyncio.sleep(5)  # Retry after 5 seconds


async def fetch_and_relay_unified_trades(connected_clients):
    """Connect to unified-prod endpoint for trade data"""
    uri = "wss://unified-prod.nats.realtime.pump.fun/"
    extra_headers = {
        "Connection": "Upgrade",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Upgrade": "websocket",
        "Origin": "https://pump.fun",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cookie": "GA1.1.1379899687.1757300687; intercom-id-w7scljv7=b1704389-09ba-49cb-a4b0-f0bd59d00da6; intercom-session-w7scljv7=; intercom-device-id-w7scljv7=5daab4f2-5559-42b8-97d0-c8898ab0d3e4; mp_567f3c2d509af79e6c5694b2d20439d2_mixpanel=%7B%22distinct_id%22%3A%22%24device%3A1c440d2f-677e-4948-8c6c-46e8a219718a%22%2C%22%24device_id%22%3A%221c440d2f-677e-4948-8c6c-46e8a219718a%22%2C%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D; _cfuvid=4Je2MlFk44ZteLfx3R2IaGbVgdIqN__QgprYh0JpNQk-1757379935151-0.0.1.1-604800000; _fs_dwell_passed=4572f959-b96e-46a4-8640-ba9dc76cc77b; fs_lua=1.1757401202929; fs_uid=#o-1YWTMD-na1#b6affcd0-891a-418b-b313-49f38d7ec743:22a5b1f9-6c5e-44da-be6c-1aba6fc378e2:1757401202929::1#/1788836708; __cf_bm=Xxp9b7xgB_vZqCpPcRL5fy4LzkZ80d1P_APveeuHgRs-1757401333-1.0.1.1-kfi6lSR6gn2ikvQvb4Zz2xUCtHHNLQ2Ai8G2_MFDuhUH1ax9pTu0ddAQzQQZEtji3_d6Nmm1jwy6i3tjKeJJ03nEIpHlkT.A1k.sHFmMPDw; _ga_T65NVS2TQ6=GS2.1.s1757401332$o6$g0$t1757401332$j60$l0$h0; cf_clearance=2uV3ZtRZ5vMDUrclzSO.W0uu10mlHNFOiN4Xuh0LJeM-1757401337-1.2.1.1-h.iZWJrpx20Yaj4zty5Hysjxh4a9wILFmOqsB3jLN8oJJOhL5Cz3ZMP2YxNnw_qRwlEz2bMVlRBE7NoPjx0anhEZF_5JFJxe7AyhPgYyOLgpVmnoESGZNOLr0NRgJz1DnYZk19xYU17YWG1if38OOAb48ekKN7NMNtJNyO2Gw2CejCwm1VG2Fhb3r7.pvoJ6Jdhr.FgPlAxwn8Vnhr.maO_MwtGtrTegW7zXVcoWMqM",
    }

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None)
    ) as session:
        while True:
            # print("Attempting to connect to unified-prod WebSocket")
            try:
                async with session.ws_connect(uri, headers=extra_headers) as websocket:
                    # print("Connected to unified-prod WebSocket")

                    # Variables to track connection state
                    info_received = False
                    connect_sent = False
                    subs_sent = False
                    # Track whether we've sent the optional warmup subscriptions
                    warmup_sent = False
                    # Track whether we've sent the real subscriptions that should follow warmups
                    real_subs_sent = False
                    # Track whether we've seen a PING/PONG from the server
                    seen_ping = False
                    seen_pong = False
                    ping_interval = 25000
                    ping_timeout = 20000
                    last_ping_time = None

                    # Start ping task
                    ping_task = None

                    async def ping_handler():
                        nonlocal last_ping_time
                        while True:
                            await asyncio.sleep(ping_interval / 1000)
                            try:
                                # print("Sending PING to unified-prod")
                                await websocket.send_bytes(b"PING\r\n")
                                last_ping_time = asyncio.get_event_loop().time()
                            except Exception as e:
                                # print(f"Error sending ping to unified: {e}")
                                break

                    async for message in websocket:
                        #print(f"Received message type: {message.type}")
                        if message.type == aiohttp.WSMsgType.TEXT:
                            data = message.data
                            if isinstance(data, bytes):
                                data = data.decode("utf-8")
                            # print(f"Received TEXT data: {data[:200]}...")

                            # Handle INFO message
                            if not info_received and data.startswith("INFO"):
                                try:
                                    info_json = data[4:].strip()
                                    info_data = json.loads(info_json)
                                    logger.info("Received INFO from unified-prod: %s", info_data)
                                    # print(f"Unified INFO parsed: {info_data}")

                                    if "ping_interval" in info_data:
                                        ping_interval = info_data["ping_interval"]
                                        logger.debug("Updated unified ping_interval=%sms", ping_interval)
                                        # print(f"Updated ping_interval to {ping_interval}ms")
                                    if "ping_timeout" in info_data:
                                        ping_timeout = info_data["ping_timeout"]
                                        logger.debug("Updated unified ping_timeout=%sms", ping_timeout)
                                        # print(f"Updated ping_timeout to {ping_timeout}ms")

                                    info_received = True

                                    # Send CONNECT
                                    connect_payload = {
                                        "no_responders": True,
                                        "protocol": 1,
                                        "verbose": False,
                                        "pedantic": False,
                                        "user": "subscriber",
                                        "pass": "OX745xvUbNQMuFqV",
                                        "lang": "nats.ws",
                                        "version": "1.30.3",
                                        "headers": True,
                                    }
                                    logger.info("Sending CONNECT to unified-prod: %s", connect_payload)
                                    # print(f"Sending CONNECT to unified-prod")
                                    await websocket.send_bytes(("CONNECT " + json.dumps(connect_payload) + "\r\n").encode("utf-8"))
                                    connect_sent = True

                                    # Subscribe to unified trade topics
                                    if not subs_sent:
                                        # Subscribe to trade-related topics on unified endpoint
                                        sub1 = b"SUB trades.unified 1\r\n"
                                        sub2 = b"SUB pump.trades.all 2\r\n"
                                        logger.info("Sending SUBs to unified-prod: %s | %s", sub1.decode().strip(), sub2.decode().strip())
                                        # print(f"Sending initial SUBs: {sub1.decode().strip()} and {sub2.decode().strip()}")
                                        await websocket.send_bytes(sub1)
                                        await websocket.send_bytes(sub2)
                                        subs_sent = True

                                        # Start ping handler
                                        ping_task = asyncio.create_task(ping_handler())
                                        logger.debug("Started ping handler for unified connection")
                                        # print("Started ping handler for unified connection")

                                except json.JSONDecodeError as e:
                                    logger.warning("Failed to parse INFO from unified-prod: %s", e)
                                    # print(f"Failed to parse INFO: {e}")

                            # Handle PING/PONG
                            elif data.strip() == "PING":
                                # Note that server sent a PING; reply with PONG
                                seen_ping = True
                                # print("Received PING from server, sending PONG")
                                await websocket.send_bytes(b"PONG\r\n")
                            elif data.strip() == "PONG":
                                # Server replied to our PING (or sent a PONG)
                                seen_pong = True
                                if last_ping_time:
                                    response_time = (asyncio.get_event_loop().time() - last_ping_time) * 1000
                                    # print(f"Unified ping response: {response_time:.2f}ms")

                                # After the first interchange of ping and pong, send the warmup SUBs once.
                                # These warmup SUBs emulate client subscriptions seen by the server
                                # and help prime the unified endpoint so downstream routing behaves
                                # as expected during initial connection.
                                # Sent as a single message containing multiple SUB lines.
                                # (This is a lightweight, non-destructive addition.)
                                # After the first interchange of ping and pong, send the warmup SUBs once
                                if not warmup_sent and (seen_ping or last_ping_time is not None):
                                    warmup_lines = (
                                        "SUB _WARMUP_UNIFIED_1758496295588 1\r\n"
                                        "SUB _WARMUP_UNIFIED_1758496295875 2\r\n"
                                        "SUB _WARMUP_UNIFIED_1758496296372 3\r\n"
                                        "SUB _WARMUP_UNIFIED_1758496297223 4\r\n"
                                        "SUB _WARMUP_UNIFIED_1758496297760 5\r\n"
                                        "SUB _WARMUP_UNIFIED_1758496307928 6\r\n"
                                    )
                                    logger.info("Sending warmup SUBs to unified-prod: %s", warmup_lines.replace('\r\n',' | '))
                                    # print(f"Sending warmup SUBs to unified-prod")
                                    await websocket.send_bytes(warmup_lines.encode("utf-8"))
                                    warmup_sent = True
                                    # After warmups are sent, immediately send the real subscriptions
                                    # requested by the user. These must be sent as a single
                                    # message and only after the warmup SUBs have been dispatched.
                                    if not real_subs_sent:
                                        real_subs = (
                                            "SUB unifiedTradeEvent.processed.* 7\r\n"
                                            "SUB unifiedTradeEvent.processed 8\r\n"
                                            "SUB unifiedTradeEvent.processed.* 9\r\n"
                                            "SUB unifiedTradeEvent.processed 10\r\n"
                                            "SUB unifiedTradeEvent.processed. 24\r\n"
                                            "SUB unifiedTradeEvent.processed. 25\r\n"
                                        )
                                        logger.info("Sending real SUBs to unified-prod: %s", real_subs.replace('\r\n',' | '))
                                        # print(f"Sending real SUBs to unified-prod")
                                        await websocket.send_bytes(real_subs.encode("utf-8"))
                                        real_subs_sent = True

                            # Handle trade messages (MSG format)
                            elif data.startswith("MSG"):
                                # print(f"Received unified MSG: {data[:100]}...")
                                lines = data.split("\r\n")
                                if len(lines) >= 2:
                                    msg_line = lines[0]
                                    payload = lines[1]
                                    try:
                                        # Payload may be a JSON object or a JSON-encoded string (quoted JSON).
                                        raw = json.loads(payload)
                                        if isinstance(raw, str):
                                            # Double-encoded JSON: parse inner JSON string
                                            try:
                                                trade_data = json.loads(raw)
                                            except json.JSONDecodeError:
                                                # Fallback: keep the raw string
                                                trade_data = raw
                                        else:
                                            trade_data = raw

                                        # print(f"Parsed unified trade: {trade_data}")

                                        # Update token mcap in database if token exists
                                        # Ensure trade_data is a dict before using .get()
                                        if isinstance(trade_data, dict):
                                            mint_address = trade_data.get('mintAddress')
                                            # print(f"Trade mint address: {mint_address}")
                                            market_cap_str = trade_data.get('marketCap')
                                            # print(f"Trade marketCap: {market_cap_str}")
                                        else:
                                             print("Parsed trade payload is not a dict; skipping mcap/ath update")
                                        if mint_address and market_cap_str:
                                            try:
                                                market_cap = float(market_cap_str)
                                                await asyncio.to_thread(update_token_mcap, mint_address, market_cap)
                                                # Check and update ATH/progress in real-time
                                                await asyncio.to_thread(update_token_ath_if_needed, mint_address, market_cap)
                                                # Update dev_activity if creator performed this trade
                                                user_addr = trade_data.get('userAddress') or trade_data.get('user_address')
                                                if user_addr:
                                                    await asyncio.to_thread(update_token_dev_activity, mint_address, user_addr, trade_data)
                                            except ValueError:
                                                print(f"Invalid marketCap value: {market_cap_str}")

                                        # Relay trade data to connected clients
                                        for client in connected_clients.copy():
                                            try:
                                                await client.send_text(json.dumps({
                                                    "type": "unified_trade",
                                                    "data": trade_data,
                                                    "timestamp": datetime.now().isoformat()
                                                }))
                                            except:
                                                connected_clients.remove(client)
                                    except json.JSONDecodeError as e:
                                        print(f"Failed to parse trade payload: {e}")

                        elif message.type == aiohttp.WSMsgType.BINARY:
                            # print(f"Received BINARY message: {len(message.data)} bytes")
                            data = message.data
                            try:
                                text_data = data.decode("utf-8")
                                #print(f"Binary as text: {text_data}")

                                # Handle INFO in binary
                                if not info_received and text_data.startswith("INFO"):
                                    try:
                                        info_json = text_data[4:].strip()
                                        info_data = json.loads(info_json)
                                        #logger.info("Received INFO (binary) from unified-prod: %s", info_data)
                                        # print(f"Unified INFO (binary) parsed: {info_data}")

                                        if "ping_interval" in info_data:
                                            ping_interval = info_data["ping_interval"]
                                            logger.debug("Updated unified ping_interval=%sms", ping_interval)
                                            # print(f"Updated ping_interval to {ping_interval}ms")
                                        if "ping_timeout" in info_data:
                                            ping_timeout = info_data["ping_timeout"]
                                            logger.debug("Updated unified ping_timeout=%sms", ping_timeout)
                                            # print(f"Updated ping_timeout to {ping_timeout}ms")

                                        info_received = True

                                        # Send CONNECT
                                        connect_payload = {
                                            "no_responders": True,
                                            "protocol": 1,
                                            "verbose": False,
                                            "pedantic": False,
                                            "user": "subscriber",
                                            "pass": "OX745xvUbNQMuFqV",
                                            "lang": "nats.ws",
                                            "version": "1.30.3",
                                            "headers": True,
                                        }
                                        #logger.info("Sending CONNECT (binary) to unified-prod: %s", connect_payload)
                                        # print(f"Sending CONNECT (binary) to unified-prod")
                                        await websocket.send_bytes(("CONNECT " + json.dumps(connect_payload) + "\r\n").encode("utf-8"))
                                        connect_sent = True

                                        # Subscribe to unified trade topics
                                        if not subs_sent:
                                            # Subscribe to trade-related topics on unified endpoint
                                            sub1 = b"SUB trades.unified 1\r\n"
                                            sub2 = b"SUB pump.trades.all 2\r\n"
                                            #logger.info("Sending SUBs (binary) to unified-prod: %s | %s", sub1.decode().strip(), sub2.decode().strip())
                                            # print(f"Sending initial SUBs (binary): {sub1.decode().strip()} and {sub2.decode().strip()}")
                                            await websocket.send_bytes(sub1)
                                            await websocket.send_bytes(sub2)
                                            subs_sent = True

                                            # Start ping handler
                                            ping_task = asyncio.create_task(ping_handler())
                                            logger.debug("Started ping handler (binary) for unified connection")
                                            # print("Started ping handler (binary) for unified connection")

                                    except json.JSONDecodeError as e:
                                        logger.warning("Failed to parse INFO (binary) from unified-prod: %s", e)
                                        # print(f"Failed to parse INFO (binary): {e}")

                                # Handle binary PING
                                elif text_data.strip() == "PING":
                                    # print("Received PING (binary) from server, sending PONG")
                                    await websocket.send_bytes(b"PONG\r\n")

                                # Handle binary PONG
                                elif text_data.strip() == "PONG":
                                    # print("Received PONG (binary) from server")
                                    seen_pong = True
                                    if last_ping_time:
                                        response_time = (asyncio.get_event_loop().time() - last_ping_time) * 1000
                                        # print(f"Unified ping response (binary): {response_time:.2f}ms")

                                    # After the first interchange of ping and pong, send the warmup SUBs once.
                                    if not warmup_sent and (seen_ping or last_ping_time is not None):
                                        warmup_lines = (
                                            "SUB _WARMUP_UNIFIED_1758496295588 1\r\n"
                                            "SUB _WARMUP_UNIFIED_1758496295875 2\r\n"
                                            "SUB _WARMUP_UNIFIED_1758496296372 3\r\n"
                                            "SUB _WARMUP_UNIFIED_1758496297223 4\r\n"
                                            "SUB _WARMUP_UNIFIED_1758496297760 5\r\n"
                                            "SUB _WARMUP_UNIFIED_1758496307928 6\r\n"
                                        )
                                        logger.info("Sending warmup SUBs (binary) to unified-prod: %s", warmup_lines.replace('\r\n',' | '))
                                        # print(f"Sending warmup SUBs (binary) to unified-prod")
                                        await websocket.send_bytes(warmup_lines.encode("utf-8"))
                                        warmup_sent = True
                                        # After warmups are sent, immediately send the real subscriptions
                                        if not real_subs_sent:
                                            real_subs = (
                                                "SUB unifiedTradeEvent.processed.* 7\r\n"
                                                "SUB unifiedTradeEvent.processed 8\r\n"
                                                "SUB unifiedTradeEvent.processed.* 9\r\n"
                                                "SUB unifiedTradeEvent.processed 10\r\n"
                                                "SUB unifiedTradeEvent.processed. 24\r\n"
                                                "SUB unifiedTradeEvent.processed. 25\r\n"
                                            )
                                            logger.info("Sending real SUBs (binary) to unified-prod: %s", real_subs.replace('\r\n',' | '))
                                            # print(f"Sending real SUBs (binary) to unified-prod")
                                            await websocket.send_bytes(real_subs.encode("utf-8"))
                                            real_subs_sent = True

                                # Detect NATS MSG frames inside binary messages and process them
                                elif text_data.startswith("MSG"):
                                    # Parse MSG using raw bytes and size field to avoid issues with \r\n splitting
                                    try:
                                        # data is the original bytes blob for this message
                                        header_end = data.find(b"\r\n")
                                        if header_end == -1:
                                            raise ValueError("No header terminator found in binary MSG")

                                        header = data[:header_end].decode("utf-8", errors="replace")
                                        # Header format: MSG <subject> <sid> [<reply>] <#bytes>
                                        parts = header.split()
                                        if len(parts) < 4:
                                            raise ValueError(f"Unexpected MSG header format: {header}")

                                        # size is last token
                                        try:
                                            size = int(parts[-1])
                                        except Exception:
                                            raise ValueError(f"Unable to parse size from MSG header: {header}")

                                        payload_start = header_end + 2
                                        payload_end = payload_start + size

                                        if len(data) < payload_end:
                                            # Incomplete frame — skip processing this websocket message
                                            pass
                                        else:
                                            payload_bytes = data[payload_start:payload_end]

                                            # Try to decode and parse JSON robustly
                                            try:
                                                payload_text = payload_bytes.decode("utf-8")
                                            except UnicodeDecodeError:
                                                payload_text = payload_bytes.decode("utf-8", errors="replace")

                                            raw_bin = None
                                            try:
                                                raw_bin = json.loads(payload_text)
                                            except json.JSONDecodeError as e:
                                                # Recovery heuristics for common malformations
                                                pt = payload_text.strip()
                                                # If payload is a JSON string containing JSON, try unwrapping
                                                if pt.startswith('"') and pt.endswith('"'):
                                                    try:
                                                        inner = json.loads(pt)
                                                        if isinstance(inner, str):
                                                            raw_bin = json.loads(inner)
                                                        else:
                                                            raw_bin = inner
                                                    except Exception:
                                                        raw_bin = None
                                                else:
                                                    # Try to extract first {...} block
                                                    first = pt.find('{')
                                                    last = pt.rfind('}')
                                                    if first != -1 and last != -1 and last > first:
                                                        candidate = pt[first : last + 1]
                                                        try:
                                                            raw_bin = json.loads(candidate)
                                                        except Exception:
                                                            raw_bin = None

                                            if raw_bin is None:
                                                print(f"Failed to parse binary MSG payload: Unterminated or malformed JSON")
                                            else:
                                                if isinstance(raw_bin, str):
                                                    try:
                                                        trade_data_bin = json.loads(raw_bin)
                                                    except json.JSONDecodeError:
                                                        trade_data_bin = raw_bin
                                                else:
                                                    trade_data_bin = raw_bin

                                                # same processing as TEXT branch: update mcap/ath and relay
                                                if isinstance(trade_data_bin, dict):
                                                    mint_address = trade_data_bin.get('mintAddress')
                                                    market_cap_str = trade_data_bin.get('marketCap')
                                                    if mint_address and market_cap_str:
                                                        try:
                                                            market_cap = float(market_cap_str)
                                                            await asyncio.to_thread(update_token_mcap, mint_address, market_cap)
                                                            await asyncio.to_thread(update_token_ath_if_needed, mint_address, market_cap)
                                                            # Update dev_activity if creator performed this trade
                                                            user_addr_b = trade_data_bin.get('userAddress') or trade_data_bin.get('user_address')
                                                            if user_addr_b:
                                                                await asyncio.to_thread(update_token_dev_activity, mint_address, user_addr_b, trade_data_bin)
                                                        except ValueError:
                                                            print(f"Invalid marketCap value (binary): {market_cap_str}")
                                                else:
                                                    print("Parsed binary trade payload is not a dict; skipping mcap/ath update")

                                                # Relay to connected clients
                                                for client in connected_clients.copy():
                                                    try:
                                                        await client.send_text(json.dumps({
                                                            "type": "unified_trade",
                                                            "data": trade_data_bin,
                                                            "timestamp": datetime.now().isoformat()
                                                        }))
                                                    except Exception:
                                                        connected_clients.remove(client)
                                    except Exception as e:
                                        print(f"Failed to parse binary MSG payload: {e}")

                                # Handle +OK in binary
                                elif text_data.startswith("+OK"):
                                    print("Received +OK (binary)")

                            except UnicodeDecodeError:
                                print("Binary data is not text")

                        elif message.type == aiohttp.WSMsgType.CLOSED:
                            print(f"Unified connection closed: code={message.data}, reason={message.extra}")
                            break
                        elif message.type == aiohttp.WSMsgType.ERROR:
                            print(f"Unified WebSocket error: {websocket.exception()}")
                            break

                    # Cleanup
                    if ping_task:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass
                    print("Unified WebSocket connection ended, cleaning up")

            except Exception as e:
                print(f"Unified connection error: {e}, will retry in 5 seconds")
                await asyncio.sleep(5)