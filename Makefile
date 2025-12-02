.PHONY: help setup build run stop logs restart clean check-docker check-env

# Default target
help:
	@echo "🔍 Natural Language Log Explorer"
	@echo ""
	@echo "Commands:"
	@echo "  make setup   - Check dependencies, build, and run the app"
	@echo "  make build   - Build Docker image"
	@echo "  make run     - Start the app"
	@echo "  make stop    - Stop the app"
	@echo "  make logs    - View container logs"
	@echo "  make restart - Restart the app"
	@echo "  make clean   - Remove containers and images"

# Full setup: check deps, build, run, open browser
setup: check-docker check-env build
	@echo ""
	@echo "🚀 Starting the app..."
	docker-compose up -d
	@echo ""
	@echo "✅ App is running at http://localhost:8501"
	@sleep 2
	@open http://localhost:8501 2>/dev/null || xdg-open http://localhost:8501 2>/dev/null || echo "Open http://localhost:8501 in your browser"

# Check if Docker is installed and running
check-docker:
	@command -v docker >/dev/null 2>&1 || { echo "❌ Docker not found. Install from https://www.docker.com/products/docker-desktop"; exit 1; }
	@docker info >/dev/null 2>&1 || { echo "❌ Docker is not running. Please start Docker Desktop."; exit 1; }
	@echo "✅ Docker is running"

# Check if .env file exists
check-env:
	@if [ ! -f .env ]; then \
		echo "⚠️  No .env file found. Creating from .env.example..."; \
		cp .env.example .env; \
		echo ""; \
		echo "📝 Edit .env with your API keys:"; \
		echo "   ANTHROPIC_API_KEY=your_key"; \
		echo "   DD_API_KEY=your_key"; \
		echo "   DD_APP_KEY=your_key"; \
		echo "   DD_SITE=us5.datadoghq.com"; \
		echo ""; \
		echo "Then run: make setup"; \
		exit 1; \
	fi
	@echo "✅ .env file exists"

# Build Docker image
build:
	@echo ""
	@echo "🔨 Building Docker image..."
	docker-compose build

# Run the app (attached)
run:
	docker-compose up

# Run the app (detached) and open browser
start: check-docker
	docker-compose up -d
	@echo "✅ App running at http://localhost:8501"
	@open http://localhost:8501 2>/dev/null || xdg-open http://localhost:8501 2>/dev/null || true

# Stop the app
stop:
	docker-compose down
	@echo "✅ App stopped"

# View logs
logs:
	docker-compose logs -f

# Restart the app
restart: stop start

# Clean up everything
clean:
	docker-compose down --rmi local --volumes
	@echo "✅ Cleaned up containers and images"
