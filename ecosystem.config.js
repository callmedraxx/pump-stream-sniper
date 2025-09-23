module.exports = {
  apps: [{
    name: 'pump-stream-sniper-docker',
  // Run docker compose via a shell so `pm2 start` simply executes the compose command
  script: 'sh',
  args: '-c "docker compose up"',
    cwd: '/root/pump-stream-sniper',
    instances: 1,
    exec_mode: 'fork',
    watch: false,
    autorestart: true,
    max_restarts: 10,
    min_uptime: '10s',
    restart_delay: 5000,
    kill_timeout: 30000,
    wait_ready: false,
    listen_timeout: 10000,
    env: {
      COMPOSE_PARALLEL_LIMIT: '1',
      COMPOSE_HTTP_TIMEOUT: '120'
    },
    error_file: '/root/pump-stream-sniper/logs/docker-err.log',
    out_file: '/root/pump-stream-sniper/logs/docker-out.log',
    log_file: '/root/pump-stream-sniper/logs/docker-combined.log',
    time: true,
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    merge_logs: true,
    // Ignore files that shouldn't trigger restarts
    ignore_watch: [
      "logs",
      "*.log",
      "*.json",
      "__pycache__",
      ".git",
      ".pytest_cache",
      "*.pyc",
      "docker-compose*.yml",
      "Dockerfile"
    ],
    // Environment specific settings
    env_production: {
      COMPOSE_PARALLEL_LIMIT: '1',
      COMPOSE_HTTP_TIMEOUT: '120',
      NODE_ENV: 'production'
    },
    env_development: {
      COMPOSE_PARALLEL_LIMIT: '1',
      COMPOSE_HTTP_TIMEOUT: '60',
      NODE_ENV: 'development'
    }
  }],

  // Helper scripts for easier management
  scripts: {
    start: "pm2 start ecosystem.config.js --env production",
    stop: "pm2 stop pump-stream-sniper-docker && docker compose -f docker-compose.prod.yml down",
    restart: "pm2 restart pump-stream-sniper-docker",
    build: "docker compose -f docker-compose.prod.yml build",
    logs: "pm2 logs pump-stream-sniper-docker",
    status: "pm2 status"
  }
};
