FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    redis-tools \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# (No PM2 needed for production runtime - use gunicorn + uvicorn workers)

# Copy project files
COPY . .

# Create non-root user and set permissions
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app \
    && chmod -R 777 /app
USER app

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application with gunicorn (uvicorn worker) in production, uvicorn in development
CMD if [ "$ENVIRONMENT" = "production" ]; then \
        exec gunicorn main:app -k uvicorn.workers.UvicornWorker --workers 1 --bind 0.0.0.0:8000 --timeout 120; \
    else \
        exec uvicorn main:app --host 0.0.0.0 --port 8000 --reload; \
    fi
