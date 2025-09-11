# Makefile for Python project formatting and development

.PHONY: format format-check lint install-dev clean

# Install development dependencies
install-dev:
	pip install -r requirements.txt

# Format code with Black and isort
format:
	@echo "🔧 Formatting Python code..."
	black src/
	isort src/
	@echo "✅ Code formatted successfully!"

# Check if code is properly formatted
format-check:
	@echo "🔍 Checking code formatting..."
	black --check src/
	isort --check-only src/
	@echo "✅ Code formatting is correct!"

# Run linting
lint:
	@echo "🔍 Running linter..."
	flake8 src/
	@echo "✅ Linting completed!"

# Format and lint all at once
check-all: format-check lint
	@echo "🎉 All checks passed!"

# Test streaming endpoints
test-streaming:
	@echo "� Testing streaming endpoints..."
	python test_streaming.py
	@echo "✅ Streaming tests completed!"

# Run all tests
test: test-streaming
	@echo "🎉 All tests passed!"

# Run the development server
run:
	@echo "🚀 Starting development server..."
	python main.py

# Run server in background for testing
run-bg:
	@echo "🚀 Starting server in background..."
	nohup python main.py > server.log 2>&1 &

# Stop background server
stop-bg:
	@echo "🛑 Stopping background server..."
	pkill -f "python main.py" || true
	@echo "✅ Server stopped!"

docker-down:
	@echo "🛑 Stopping Docker containers..."
	docker-compose down

docker-logs:
	@echo "📋 Showing Docker logs..."
	docker-compose logs -f

docker-clean:
	@echo "🧹 Cleaning Docker containers and volumes..."
	docker-compose down -v
	docker system prune -f

docker-restart: docker-down docker-up
	@echo "🔄 Restarting Docker containers..."

# Full development setup
setup: install-dev format lint
	@echo "🚀 Development environment ready!"

# Help
help:
	@echo "Available commands:"
	@echo "  make install-dev    - Install development dependencies"
	@echo "  make format         - Format code with Black and isort"
	@echo "  make format-check   - Check if code is properly formatted"
	@echo "  make lint           - Run flake8 linter"
	@echo "  make check-all      - Run all checks (format + lint)"
	@echo "  make clean          - Clean Python cache files"
	@echo "  make setup          - Full development setup"
	@echo ""
	@echo "Docker commands:"
	@echo "  make docker-build   - Build Docker containers"
	@echo "  make docker-up      - Start Docker containers"
	@echo "  make docker-down    - Stop Docker containers"
	@echo "  make docker-logs    - Show Docker logs"
	@echo "  make docker-clean   - Clean Docker containers and volumes"
	@echo "  make docker-restart - Restart Docker containers"
	@echo "  make help           - Show this help message"
