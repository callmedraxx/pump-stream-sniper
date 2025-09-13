#!/bin/bash

# Production startup script for Docker deployment
echo "🚀 Starting Pump Stream Sniper in production mode..."

# Set environment
export ENVIRONMENT=production

# Check if PM2 is available
if ! command -v pm2 &> /dev/null; then
    echo "❌ PM2 not found, installing..."
    npm install -g pm2
fi

# Create logs directory if it doesn't exist
mkdir -p /app/logs

# Start the application with PM2
echo "📊 Starting application with PM2..."
pm2-runtime start ecosystem.config.js --env production

echo "✅ Application started successfully!"