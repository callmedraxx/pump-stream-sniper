import asyncio
import json
from datetime import datetime

import aiohttp

connected_clients = set()


def organize_token_data(raw_data, msg_line):
    """Organize and clean token data for frontend display"""

    # Parse MSG line: MSG <subject> <sid> <size>
    msg_parts = msg_line.split()
    subject = msg_parts[1] if len(msg_parts) > 1 else "unknown"
    sid = msg_parts[2] if len(msg_parts) > 2 else "0"

    # Format timestamps
    created_timestamp = raw_data.get("created_timestamp")
    if created_timestamp:
        created_datetime = datetime.fromtimestamp(created_timestamp / 1000)
        created_formatted = created_datetime.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        created_formatted = None

    last_trade_timestamp = raw_data.get("last_trade_timestamp")
    if last_trade_timestamp:
        last_trade_datetime = datetime.fromtimestamp(last_trade_timestamp / 1000)
        last_trade_formatted = last_trade_datetime.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        last_trade_formatted = None

    # Calculate bonding curve progress
    virtual_sol_reserves = raw_data.get("virtual_sol_reserves", 0)
    real_sol_reserves = raw_data.get("real_sol_reserves", 0)
    total_sol = virtual_sol_reserves + real_sol_reserves
    progress_percentage = (real_sol_reserves / total_sol * 100) if total_sol > 0 else 0

    # Format large numbers
    def format_number(num):
        if num is None:
            return None
        if num >= 1e9:
            return ".2f"
        elif num >= 1e6:
            return ".2f"
        elif num >= 1e3:
            return ".2f"
        else:
            return str(num)

    # Organize social links
    social_links = {
        "twitter": raw_data.get("twitter"),
        "telegram": raw_data.get("telegram"),
        "website": raw_data.get("website"),
    }
    # Remove null values
    social_links = {k: v for k, v in social_links.items() if v is not None}

    # Organize the data
    organized = {
        "message_info": {
            "subject": subject,
            "sid": sid,
            "timestamp": datetime.now().isoformat(),
            "event_type": "new_coin_created",
        },
        "token_info": {
            "mint": raw_data.get("mint"),
            "name": raw_data.get("name"),
            "symbol": raw_data.get("symbol"),
            "description": raw_data.get("description"),
            "image_uri": raw_data.get("image_uri"),
            "metadata_uri": raw_data.get("metadata_uri"),
            "video_uri": raw_data.get("video_uri"),
            "banner_uri": raw_data.get("banner_uri"),
        },
        "creator_info": {
            "creator": raw_data.get("creator"),
            "created_timestamp": created_timestamp,
            "created_formatted": created_formatted,
        },
        "market_data": {
            "market_cap": raw_data.get("market_cap"),
            "usd_market_cap": raw_data.get("usd_market_cap"),
            "ath_market_cap": raw_data.get("ath_market_cap"),
            "ath_market_cap_timestamp": raw_data.get("ath_market_cap_timestamp"),
            "last_trade_timestamp": last_trade_timestamp,
            "last_trade_formatted": last_trade_formatted,
            "king_of_the_hill_timestamp": raw_data.get("king_of_the_hill_timestamp"),
        },
        "bonding_curve": {
            "bonding_curve": raw_data.get("bonding_curve"),
            "associated_bonding_curve": raw_data.get("associated_bonding_curve"),
            "virtual_sol_reserves": virtual_sol_reserves,
            "virtual_token_reserves": raw_data.get("virtual_token_reserves"),
            "real_sol_reserves": real_sol_reserves,
            "real_token_reserves": raw_data.get("real_token_reserves"),
            "progress_percentage": round(progress_percentage, 2),
            "complete": raw_data.get("complete", False),
        },
        "supply_info": {
            "total_supply": raw_data.get("total_supply"),
            "total_supply_formatted": format_number(raw_data.get("total_supply")),
        },
        "social_links": social_links,
        "status_flags": {
            "show_name": raw_data.get("show_name", True),
            "hidden": raw_data.get("hidden"),
            "nsfw": raw_data.get("nsfw", False),
            "is_banned": raw_data.get("is_banned", False),
            "is_currently_live": raw_data.get("is_currently_live", False),
            "initialized": raw_data.get("initialized", True),
            "hide_banner": raw_data.get("hide_banner", False),
        },
        "additional_info": {
            "market_id": raw_data.get("market_id"),
            "inverted": raw_data.get("inverted"),
            "raydium_pool": raw_data.get("raydium_pool"),
            "pump_swap_pool": raw_data.get("pump_swap_pool"),
            "program": raw_data.get("program"),
            "livestream_ban_expiry": raw_data.get("livestream_ban_expiry"),
            "livestream_downrank_score": raw_data.get("livestream_downrank_score"),
            "last_reply": raw_data.get("last_reply"),
            "reply_count": raw_data.get("reply_count", 0),
            "updated_at": raw_data.get("updated_at"),
        },
    }

    return organized


def organize_trade_data(raw_data, event_name):
    """Organize and clean trade data for frontend display"""

    # Format timestamps
    timestamp = raw_data.get("timestamp")
    if timestamp:
        trade_datetime = datetime.fromtimestamp(timestamp)
        trade_formatted = trade_datetime.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        trade_formatted = None

    created_timestamp = raw_data.get("created_timestamp")
    if created_timestamp:
        created_datetime = datetime.fromtimestamp(created_timestamp / 1000)
        created_formatted = created_datetime.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        created_formatted = None

    last_trade_timestamp = raw_data.get("last_trade_timestamp")
    if last_trade_timestamp:
        last_trade_datetime = datetime.fromtimestamp(last_trade_timestamp / 1000)
        last_trade_formatted = last_trade_datetime.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        last_trade_formatted = None

    # Format amounts
    sol_amount = raw_data.get("sol_amount", 0)
    token_amount = raw_data.get("token_amount", 0)

    # Convert lamports to SOL (1 SOL = 1e9 lamports)
    sol_amount_sol = sol_amount / 1e9 if sol_amount else 0

    # Format large token amounts
    def format_token_amount(amount):
        if amount >= 1e12:
            return ".2f"
        elif amount >= 1e9:
            return ".2f"
        elif amount >= 1e6:
            return ".2f"
        elif amount >= 1e3:
            return ".2f"
        else:
            return str(amount)

    # Calculate price per token
    price_per_token = sol_amount_sol / token_amount if token_amount > 0 else 0

    # Calculate bonding curve progress
    virtual_sol_reserves = raw_data.get("virtual_sol_reserves", 0)
    real_sol_reserves = sol_amount_sol  # Add this trade's SOL to real reserves
    total_sol = virtual_sol_reserves + real_sol_reserves
    progress_percentage = (real_sol_reserves / total_sol * 100) if total_sol > 0 else 0

    # Organize social links
    social_links = {
        "twitter": raw_data.get("twitter"),
        "telegram": raw_data.get("telegram"),
        "website": raw_data.get("website"),
    }
    # Remove null values
    social_links = {k: v for k, v in social_links.items() if v is not None}

    # Organize the trade data
    organized = {
        "message_info": {
            "event_type": event_name,
            "timestamp": datetime.now().isoformat(),
            "signature": raw_data.get("signature"),
            "slot": raw_data.get("slot"),
            "tx_index": raw_data.get("tx_index"),
        },
        "trade_info": {
            "is_buy": raw_data.get("is_buy", True),
            "sol_amount": sol_amount,
            "sol_amount_sol": round(sol_amount_sol, 6),
            "token_amount": token_amount,
            "token_amount_formatted": format_token_amount(token_amount),
            "price_per_token": round(price_per_token, 10),
            "user": raw_data.get("user"),
            "timestamp": timestamp,
            "trade_formatted": trade_formatted,
        },
        "token_info": {
            "mint": raw_data.get("mint"),
            "name": raw_data.get("name"),
            "symbol": raw_data.get("symbol"),
            "description": raw_data.get("description"),
            "image_uri": raw_data.get("image_uri"),
            "metadata_uri": raw_data.get("metadata_uri"),
            "video_uri": raw_data.get("video_uri"),
        },
        "creator_info": {
            "creator": raw_data.get("creator"),
            "creator_username": raw_data.get("creator_username"),
            "creator_profile_image": raw_data.get("creator_profile_image"),
            "created_timestamp": created_timestamp,
            "created_formatted": created_formatted,
        },
        "market_data": {
            "market_cap": raw_data.get("market_cap"),
            "usd_market_cap": raw_data.get("usd_market_cap"),
            "last_trade_timestamp": last_trade_timestamp,
            "last_trade_formatted": last_trade_formatted,
            "king_of_the_hill_timestamp": raw_data.get("king_of_the_hill_timestamp"),
        },
        "bonding_curve": {
            "bonding_curve": raw_data.get("bonding_curve"),
            "associated_bonding_curve": raw_data.get("associated_bonding_curve"),
            "virtual_sol_reserves": virtual_sol_reserves,
            "virtual_token_reserves": raw_data.get("virtual_token_reserves"),
            "progress_percentage": round(progress_percentage, 2),
            "complete": raw_data.get("complete", False),
        },
        "supply_info": {
            "total_supply": raw_data.get("total_supply"),
            "total_supply_formatted": format_token_amount(
                raw_data.get("total_supply", 0)
            ),
        },
        "social_links": social_links,
        "status_flags": {
            "show_name": raw_data.get("show_name", True),
            "nsfw": raw_data.get("nsfw", False),
            "is_currently_live": raw_data.get("is_currently_live", False),
            "market_id": raw_data.get("market_id"),
            "inverted": raw_data.get("inverted"),
            "raydium_pool": raw_data.get("raydium_pool"),
        },
        "activity_info": {
            "reply_count": raw_data.get("reply_count", 0),
            "last_reply": raw_data.get("last_reply"),
        },
    }

    return organized


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
                                # print("Received INFO message")
                                try:
                                    # Parse INFO message to extract connection details
                                    info_json = data[4:].strip()  # Remove "INFO" prefix
                                    info_data = json.loads(info_json)
                                    # print(f"INFO data: {info_data}")

                                    # Extract ping interval and timeout if available
                                    if "ping_interval" in info_data:
                                        ping_interval = info_data["ping_interval"]
                                        # print(f"Updated ping interval to: {ping_interval}ms")
                                    if "ping_timeout" in info_data:
                                        ping_timeout = info_data["ping_timeout"]
                                        # print(f"Updated ping timeout to: {ping_timeout}ms")

                                    info_received = True

                                    # Step 2: Send CONNECT after receiving INFO
                                    # print("Sending CONNECT")
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
                                    await websocket.send_bytes(
                                        (
                                            "CONNECT "
                                            + json.dumps(connect_payload)
                                            + "\r\n"
                                        ).encode("utf-8")
                                    )
                                    # print("Sent CONNECT")
                                    connect_sent = True

                                    # Send SUBs immediately after CONNECT (since +OK may not be sent)
                                    if not subs_sent:
                                        # print("Sending SUB commands")
                                        await websocket.send_bytes(
                                            ("SUB newCoinCreated.prod 1\r\n").encode(
                                                "utf-8"
                                            )
                                        )
                                        await websocket.send_bytes(
                                            ("SUB pump.fun.livestream 2\r\n").encode(
                                                "utf-8"
                                            )
                                        )
                                        # print("Sent all SUB commands")
                                        subs_sent = True

                                        # Start ping handler after subs are sent
                                        ping_task = asyncio.create_task(ping_handler())
                                        # print("Started ping handler")
                                except json.JSONDecodeError as e:
                                    pass
                                    # print(f"Failed to parse INFO message: {e}")

                            # Handle +OK acknowledgment
                            elif data.startswith("+OK"):
                                pass
                                # print("Received +OK acknowledgment")

                            # Handle PING messages
                            elif data.strip() == "PING":
                                # print("Received PING, sending PONG")
                                await websocket.send_bytes(b"PONG\r\n")

                            # Handle PONG messages
                            elif data.strip() == "PONG":
                                # print("Received PONG")
                                if last_ping_time:
                                    response_time = (
                                        asyncio.get_event_loop().time() - last_ping_time
                                    ) * 1000
                                    # print(f"Ping response time: {response_time:.2f}ms")

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

                                        # Organize trade data if it's a tradeCreated event
                                        if event_name == "tradeCreated":
                                            organized_data = organize_trade_data(
                                                payload, event_name
                                            )
                                        else:
                                            # For other events, use the raw payload
                                            organized_data = payload

                                        # Relay the organized payload to all connected clients
                                        for client in connected_clients.copy():
                                            try:
                                                await client.send_text(
                                                    json.dumps(organized_data)
                                                )
                                            except:
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
                                    try:
                                        msg_data = json.loads(payload)
                                        # print(f"Parsed MSG payload: {msg_data}")

                                        # Organize and clean the token data
                                        organized_data = organize_token_data(
                                            msg_data, msg_line
                                        )

                                        # Relay the organized payload to all connected clients
                                        for client in connected_clients.copy():
                                            try:
                                                await client.send_text(
                                                    json.dumps(organized_data)
                                                )
                                            except:
                                                connected_clients.remove(client)
                                    except json.JSONDecodeError as e:
                                        # print(f"Failed to parse MSG payload: {e}")
                                        pass
                                else:
                                    # print("MSG message format incorrect")
                                    pass

                            # Handle other NATS protocol messages
                            elif data.startswith("+OK"):
                                pass
                                # print("Received +OK acknowledgment")
                            elif data.startswith("-ERR"):
                                pass
                                # print(f"Received error: {data}")

                        elif message.type == aiohttp.WSMsgType.BINARY:
                            data = message.data
                            # print(f"Received binary message: {len(data)} bytes")
                            try:
                                text_data = data.decode("utf-8")
                                # print(f"Binary as text: {text_data}")

                                # Handle INFO in binary
                                if not info_received and text_data.startswith("INFO"):
                                    # print("Received INFO message (binary)")
                                    try:
                                        # Parse INFO message to extract connection details
                                        info_json = text_data[
                                            4:
                                        ].strip()  # Remove "INFO" prefix
                                        info_data = json.loads(info_json)
                                        # print(f"INFO data: {info_data}")

                                        # Extract ping interval and timeout if available
                                        if "ping_interval" in info_data:
                                            ping_interval = info_data["ping_interval"]
                                            # print(f"Updated ping interval to: {ping_interval}ms")
                                        if "ping_timeout" in info_data:
                                            ping_timeout = info_data["ping_timeout"]
                                            # print(f"Updated ping timeout to: {ping_timeout}ms")

                                        info_received = True

                                        # Step 2: Send CONNECT after receiving INFO
                                        # print("Sending CONNECT")
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
                                        await websocket.send_bytes(
                                            (
                                                "CONNECT "
                                                + json.dumps(connect_payload)
                                                + "\r\n"
                                            ).encode("utf-8")
                                        )
                                        # print("Sent CONNECT")
                                        connect_sent = True

                                        # Send SUBs immediately after CONNECT (since +OK may not be sent)
                                        if not subs_sent:
                                            # print("Sending SUB commands")
                                            await websocket.send_bytes(
                                                (
                                                    "SUB newCoinCreated.prod 1\r\n"
                                                ).encode("utf-8")
                                            )
                                            await websocket.send_bytes(
                                                (
                                                    "SUB pump.fun.livestream 2\r\n"
                                                ).encode("utf-8")
                                            )
                                            # print("Sent all SUB commands")
                                            subs_sent = True

                                            # Start ping handler after subs are sent
                                            ping_task = asyncio.create_task(
                                                ping_handler()
                                            )
                                            # print("Started ping handler")
                                    except json.JSONDecodeError as e:
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
