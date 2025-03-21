
# Load environment variables from .env file
include .env
export

.PHONY: up down rebuild logs logs-api prune aider-llama aider-deepseek shell-api shell-aider health


# Start only the API service
up:
	docker-compose up -d
	@echo "Services started. API available at http://localhost:8000"

# Stop all containers
down:
	docker-compose down

# Rebuild and restart
rebuild:
	docker-compose down
	docker-compose build --no-cache
	docker-compose up -d
	@echo "Services rebuilt and restarted"


aider-deepseek:
	aider --model $(DEEPSEEK_MODEL)


# View logs
logs:
	docker-compose logs -f

logs-api:
	docker-compose logs -f fastapi_backend

# Remove orphaned containers and networks
prune:
	docker-compose down -v --remove-orphans
	docker network prune -f
	docker system prune -f
	@echo "Removed orphaned containers and networks"


# Shell into containers
shell-api:
	docker-compose exec fastapi_backend bash || docker-compose exec fastapi_backend sh


# Health check the API
health:
	curl -f http://localhost:8000/health || echo "API is not responding"



# Default help message
help:
	@echo "Available commands:"
	@echo "  make up            - Start services in detached mode"
	@echo "  make down          - Stop all services"
	@echo "  make rebuild       - Rebuild and restart services"
	@echo "  make logs          - View logs for all services"
	@echo "  make logs-api      - View logs for API service"
	@echo "  make shell-api     - Open shell in API container"
	@echo "  make health        - Check API health"
	@echo "  make prune         - Remove orphaned containers and networks"
	@echo "  make aider-deepseek - Start aider with deepseek model"