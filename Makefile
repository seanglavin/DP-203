
# Load environment variables from .env file
include .env
export

.PHONY: up down rebuild logs logs-api prune aider-llama aider-deepseek shell-api shell-aider health

# Create a local uv virtual environment if it doesn't exist
venv:
	uv venv --python python3.12
	uv pip install --all

# Sync dependencies from lockfile locally
sync:
	uv pip install --requirements uv.lock

uv-clean:
	rm -rf .venv uv.lock


# Start only the API service
up:
	docker-compose up -d

# Stop all containers
down:
	docker-compose down -v --remove-orphans

# Rebuild and restart
rebuild:
	docker-compose down -v --remove-orphans
	docker system prune -f
	docker-compose build --no-cache


aider-deepseek:
	aider --model $(DEEPSEEK_MODEL)


# View logs
logs:
	docker logs --tail=100 fastapi_backend

logs-api:
	docker-compose logs -f fastapi_backend

# Remove orphaned containers and networks
prune:
	docker-compose down -v --remove-orphans
	docker system prune -f
	docker network prune -f


# Shell into containers
shell-api:
	docker-compose exec fastapi_backend sh -c "bash || sh"


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