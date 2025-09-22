import asyncio
from contextlib import asynccontextmanager
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from src.services.stream import fetch_and_relay_livestreams, fetch_and_relay_unified_trades, connected_clients
from src.routes.stream import router as websocket_router
from src.routes.live import router as live_router
from src.routes.vibe import router as vibe_router
from src.services.fetch_live import poll_live_tokens
from src.services.fetch_ath import start_background_loop, stop_background_loop
from src.models import get_db, Token
from src.models.database import create_tables
from fastapi import FastAPI
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

    # Create database tables if they don't exist
    print("🗄️  Ensuring database tables exist...")
    create_tables()

    print("📊 Starting live tokens polling...")
    live_tokens_task = asyncio.create_task(poll_live_tokens())

    # Start WebSocket connections to pump.fun
    print("🔗 Starting pump.fun WebSocket connections...")
    livestream_task = asyncio.create_task(fetch_and_relay_livestreams(connected_clients))
    unified_task = asyncio.create_task(fetch_and_relay_unified_trades(connected_clients))

    # Start ATH background loop (fetch ATH for complete==True every 60s)
    try:
        ath_task = start_background_loop(interval_seconds=60, batch_size=150, delay_between_batches=0.5)
        print("⚡ Started ATH background loop")
    except Exception as e:
        print(f"⚠️ Failed to start ATH background loop: {e}")
    # Start snapshot background task so SSE can serve last snapshot even if no clients were connected
    try:
        from src.services.sync_snapshot_service import run_forever as run_snapshot_service
        asyncio.create_task(run_snapshot_service())
        print("🗂️  Started sync snapshot service")
    except Exception:
        print("⚠️  Failed to start sync snapshot service")

    yield
    # Shutdown
    print("🛑 Shutting down background tasks...")
    if live_tokens_task:
        live_tokens_task.cancel()
        try:
            await live_tokens_task
        except asyncio.CancelledError:
            pass

    # Stop WebSocket connections
    if 'livestream_task' in locals():
        livestream_task.cancel()
        try:
            await livestream_task
        except asyncio.CancelledError:
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

app = FastAPI(lifespan=lifespan)

app.include_router(websocket_router)
app.include_router(live_router)
app.include_router(vibe_router)

@app.get("/")
async def get():
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
        <head>
            <title>Pump.fun Backend Dashboard</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
                .container { max-width: 1200px; margin: 0 auto; }
                .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; }
                .section { background: white; padding: 20px; margin: 20px 0; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                .status { padding: 10px; margin: 10px 0; border-radius: 5px; }
                .online { background-color: #d4edda; border: 1px solid #c3e6cb; color: #155724; }
                .token { background-color: #f8f9fa; border: 1px solid #dee2e6; padding: 15px; margin: 10px 0; border-radius: 5px; }
                .token h3 { margin-top: 0; color: #495057; }
                .progress { width: 100%; height: 20px; background-color: #e9ecef; border-radius: 10px; overflow: hidden; margin: 10px 0; }
                .progress-bar { height: 100%; background: linear-gradient(90deg, #28a745, #20c997); transition: width 0.3s; border-radius: 10px; }
                .endpoint { background-color: #e7f3ff; border: 1px solid #b3d7ff; padding: 15px; border-radius: 5px; margin: 10px 0; }
                .endpoint h4 { margin-top: 0; color: #0056b3; }
                code { background-color: #f8f9fa; padding: 2px 6px; border-radius: 3px; font-family: 'Courier New', monospace; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🎯 Pump.fun Backend Dashboard</h1>
                    <p>Real-time monitoring for livestreams and live tokens</p>
                </div>

                <div class="section">
                    <h2>📊 Live Tokens Monitor</h2>
                    <div id="status" class="status">Connecting...</div>
                    <div id="tokenCount">Waiting for data...</div>
                    <div id="tokens"></div>
                </div>

                <div class="section">
                    <h2>🔌 Available Endpoints</h2>
                    <div class="endpoint">
                        <h4>📡 WebSocket Livestream</h4>
                        <p><code>ws://localhost:8000/ws</code></p>
                        <p>Real-time trade and token creation events</p>
                    </div>
                    <div class="endpoint">
                        <h4>📊 Live Tokens SSE</h4>
                        <p><code>http://localhost:8000/live-tokens</code></p>
                        <p>Server-Sent Events for live token updates</p>
                    </div>
                </div>

                <div class="section">
                    <h2>💻 Frontend Integration</h2>
                    <h3>Connect to Live Tokens:</h3>
                    <pre><code>const eventSource = new EventSource('/live-tokens');
eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    if (data.event === 'live_tokens_update') {
        console.log('Live tokens:', data.data);
    }
};</code></pre>

                    <h3>Connect to WebSocket:</h3>
                    <pre><code>const ws = new WebSocket('ws://localhost:8000/ws');
ws.onmessage = function(event) {
    console.log('Livestream event:', event.data);
};</code></pre>
                </div>
            </div>

            <script>
                const statusDiv = document.getElementById('status');
                const tokenCountDiv = document.getElementById('tokenCount');
                const tokensDiv = document.getElementById('tokens');

                const eventSource = new EventSource('/live-tokens');

                eventSource.onopen = function() {
                    statusDiv.textContent = '🟢 Connected to live tokens stream';
                    statusDiv.className = 'status online';
                };

                eventSource.onmessage = function(event) {
                    try {
                        const data = JSON.parse(event.data);
                        if (data.event === 'live_tokens_update') {
                            updateTokens(data);
                        } else if (data.event === 'heartbeat') {
                            statusDiv.textContent = `🟢 Connected (Last heartbeat: ${new Date(data.timestamp).toLocaleTimeString()})`;
                        }
                    } catch (e) {
                        console.error('Error parsing SSE data:', e);
                    }
                };

                eventSource.onerror = function() {
                    statusDiv.textContent = '🔴 Connection lost - retrying...';
                    statusDiv.className = 'status';
                    statusDiv.style.backgroundColor = '#f8d7da';
                    statusDiv.style.borderColor = '#f5c6cb';
                    statusDiv.style.color = '#721c24';
                };

                function updateTokens(data) {
                    tokenCountDiv.textContent = `📊 Monitoring ${data.token_count} live tokens (Updated: ${new Date(data.timestamp).toLocaleString()})`;

                    tokensDiv.innerHTML = '';
                    data.data.forEach((token, index) => {
                        const tokenDiv = document.createElement('div');
                        tokenDiv.className = 'token';

                        const progressPercent = token.market_data.progress_percentage;
                        const progressColor = progressPercent > 80 ? '#28a745' : progressPercent > 50 ? '#ffc107' : '#dc3545';

                        tokenDiv.innerHTML = `
                            <h3>${token.token_info.symbol || 'Unknown'} - ${token.token_info.name || 'Unnamed Token'}</h3>
                            <p><strong>Mint:</strong> ${token.token_info.mint}</p>
                            <p><strong>Market Cap:</strong> $${token.market_data.usd_market_cap?.toLocaleString() || 'N/A'}</p>
                            <p><strong>Bonding Curve Progress:</strong></p>
                            <div class="progress">
                                <div class="progress-bar" style="width: ${progressPercent}%; background-color: ${progressColor};"></div>
                            </div>
                            <p>${progressPercent.toFixed(1)}% complete</p>
                            <p><strong>Creator:</strong> ${token.creator_info.creator?.slice(0, 8)}...${token.creator_info.creator?.slice(-8)}</p>
                            <p><strong>Created:</strong> ${token.creator_info.created_formatted || 'Unknown'}</p>
                            ${token.social_links.twitter ? `<p><strong>Twitter:</strong> <a href="${token.social_links.twitter}" target="_blank">${token.social_links.twitter}</a></p>` : ''}
                            ${token.social_links.website ? `<p><strong>Website:</strong> <a href="${token.social_links.website}" target="_blank">${token.social_links.website}</a></p>` : ''}
                        `;

                        tokensDiv.appendChild(tokenDiv);
                    });
                }}
            </script>
        </body>
    </html>
    """)

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    try:
        db = next(get_db())
        live_tokens_count = db.query(Token).filter(Token.is_live == True).count()
        last_update = db.query(Token.updated_at).order_by(Token.updated_at.desc()).first()
        last_update_time = last_update[0].isoformat() if last_update else None
    except Exception:
        live_tokens_count = 0
        last_update_time = None

    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "pumpfun-backend",
        "version": "1.0.0",
        "environment": os.getenv("ENVIRONMENT", "development"),
        "live_tokens_count": live_tokens_count,
        "last_update": last_update_time
    }