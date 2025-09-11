# Pump.fun Backend

A FastAPI-based backend service for streaming real-time pump.fun token data with WebSocket and SSE support.

## Features

- 🚀 **Real-time Data Streaming**: WebSocket and Server-Sent Events (SSE)
- 📊 **Complete Token Data**: Organized structure with raw data preservation
- 🐳 **Docker Support**: Production-ready with Gunicorn and multiple workers
- 🗄️ **Database Integration**: PostgreSQL with Alembic migrations
- 🔄 **Redis Caching**: High-performance data management
- 📈 **Production Ready**: Health checks, logging, and monitoring

## Quick Start

### Development

```bash
# Clone the repository
git clone <repository-url>
cd pumpfun_ss_backend

# Copy environment file
cp .env.example .env

# Start development environment
docker-compose up --build
```

### Production

```bash
# Set production environment
echo "ENVIRONMENT=production" >> .env

# Start production environment
docker-compose -f docker-compose.prod.yml up --build -d
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ENVIRONMENT` | Environment mode (`development` or `production`) | `development` |
| `APP_PORT` | Application port | `8000` |
| `LOG_LEVEL` | Logging level | `info` |
| `POSTGRES_DB` | PostgreSQL database name | `pumpfun_db` |
| `POSTGRES_USER` | PostgreSQL username | `pumpfun_user` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `pumpfun_password` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `REDIS_PORT` | Redis port | `6379` |
| `COOKIE` | pump.fun authentication cookie | Required |
| `USERNAME` | pump.fun username | Required |

## API Endpoints

### WebSocket
- `ws://localhost:8000/ws/live-tokens` - Real-time token updates

### SSE (Server-Sent Events)
- `GET /live-tokens` - Stream live token data

### REST API
- `GET /` - Web interface
- `GET /health` - Health check endpoint

## Docker Configuration

### Development Mode
- Uses `uvicorn` with `--reload` for hot reloading
- Single worker process
- Debug logging

### Production Mode
- Uses `gunicorn` with multiple workers
- Optimized for performance
- Production logging
- Health checks enabled

### Worker Configuration
- **Workers**: `(CPU cores × 2) + 1`
- **Worker Class**: `uvicorn.workers.UvicornWorker`
- **Max Requests**: 1000 per worker (with jitter)
- **Timeout**: 30 seconds

## Database Setup

### Initialize Database
```bash
# Run database migrations
docker-compose exec app alembic upgrade head

# Create initial data (if needed)
docker-compose exec app python -c "from main import init_db; init_db()"
```

### Alembic Commands
```bash
# Create new migration
docker-compose exec app alembic revision --autogenerate -m "migration message"

# Apply migrations
docker-compose exec app alembic upgrade head

# Downgrade
docker-compose exec app alembic downgrade -1
```

## Monitoring

### Health Checks
- Application: `http://localhost:8000/health`
- Database: PostgreSQL health check built-in
- Redis: Redis ping check built-in

### Logs
```bash
# View application logs
docker-compose logs -f app

# View all service logs
docker-compose logs -f
```

## Scaling

### Horizontal Scaling
```bash
# Scale application instances
docker-compose up -d --scale app=3
```

### Load Balancing
Use a reverse proxy (nginx, traefik) in front of multiple app instances for load balancing.

## Troubleshooting

### Common Issues

1. **Port already in use**
   ```bash
   # Change port in .env
   APP_PORT=8001
   ```

2. **Database connection issues**
   ```bash
   # Check database logs
   docker-compose logs db
   ```

3. **Redis connection issues**
   ```bash
   # Check redis logs
   docker-compose logs redis
   ```

### Performance Tuning

- **Increase workers** in `gunicorn.conf.py`
- **Adjust database connection pool** in SQLAlchemy config
- **Enable Redis clustering** for high availability

## Development

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run with uvicorn
uvicorn main:app --reload

# Run with gunicorn (production simulation)
gunicorn -c gunicorn.conf.py main:app
```

### Testing
```bash
# Run tests
pytest

# Run with coverage
pytest --cov=.
```

## Deployment

### Docker Hub
```bash
# Build and push
docker build -t your-registry/pumpfun-backend:latest .
docker push your-registry/pumpfun-backend:latest
```

### Kubernetes
Use the provided `docker-compose.yml` as a reference for Kubernetes deployment with:
- ConfigMaps for environment variables
- Secrets for sensitive data
- PersistentVolumeClaims for database storage
- HorizontalPodAutoscaler for scaling

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
# pump-stream-sniper
