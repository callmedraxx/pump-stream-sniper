# Gunicorn configuration for production
import multiprocessing
import os

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50

# Timeout
timeout = 30
keepalive = 2

# Logging
loglevel = os.getenv("LOG_LEVEL", "info")
accesslog = "-"
errorlog = "-"

# Process naming
proc_name = "pumpfun_backend"

# Server mechanics
preload_app = True
pidfile = "/tmp/gunicorn.pid"
user = "app"
group = "app"
tmp_upload_dir = None

# SSL (if needed)
keyfile = None
certfile = None

# Application
wsgi_module = "main:app"
pythonpath = "/app"
