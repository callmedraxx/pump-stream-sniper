module.exports = {
  apps: [{
    name: 'pumpfun-backend-prod',
    script: 'gunicorn',
    args: '-c gunicorn.conf.py main:app',
    interpreter: 'python',
    cwd: '/app',
    instances: 1,
    exec_mode: 'fork',
    watch: false,
    max_memory_restart: '2G',
    env: {
      ENVIRONMENT: 'production',
      LOG_LEVEL: 'info',
      PYTHONPATH: '/app',
      PYTHONUNBUFFERED: '1',
      PYTHONDONTWRITEBYTECODE: '1'
    },
    error_file: '/app/logs/err.log',
    out_file: '/app/logs/out.log',
    log_file: '/app/logs/combined.log',
    time: true,
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    merge_logs: true,
    max_restarts: 10,
    min_uptime: '30s',
    restart_delay: 5000,
    autorestart: true,
    kill_timeout: 30000,
    wait_ready: true,
    listen_timeout: 10000,
    // Ignore files that shouldn't trigger restarts
    ignore_watch: [
      "logs",
      "*.log",
      "*.json",
      "__pycache__",
      ".git",
      ".pytest_cache",
      "*.pyc"
    ],
    // Environment specific settings
    env_production: {
      ENVIRONMENT: 'production',
      LOG_LEVEL: 'warning',
      PYTHONPATH: '/app',
      PYTHONUNBUFFERED: '1',
      PYTHONDONTWRITEBYTECODE: '1'
    },
    env_development: {
      ENVIRONMENT: 'development',
      LOG_LEVEL: 'debug',
      PYTHONPATH: '/app',
      PYTHONUNBUFFERED: '1'
    }
  }],

  // Docker Compose integration
  deploy: {
    production: {
      user: 'app',
      host: 'localhost',
      ref: 'origin/main',
      repo: 'git@github.com:callmedraxx/pump-stream-sniper.git',
      path: '/app',
      'pre-setup': 'docker-compose -f docker-compose.prod.yml down',
      'post-setup': 'docker-compose -f docker-compose.prod.yml build',
      'pre-deploy': 'docker-compose -f docker-compose.prod.yml stop app',
      'post-deploy': 'docker-compose -f docker-compose.prod.yml up -d && docker-compose -f docker-compose.prod.yml exec -T app pm2 reload ecosystem.config.js --env production',
      'pre-deploy-local': 'docker-compose -f docker-compose.prod.yml pull',
      'post-deploy-local': 'docker-compose -f docker-compose.prod.yml up -d'
    }
  }
};
