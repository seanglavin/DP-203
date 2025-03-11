
# Load environment variables from .env file
include .env
export

.PHONY: up down rebuild logs clean aider-llama aider-deepseek


# Start only the API service
up:
	docker-compose up -d

# Stop all containers
down:
	docker-compose down

# Rebuild and restart
rebuild:
	docker-compose down
	docker-compose up -d --build

# Start aider with llama model
aider-llama: up
	docker-compose exec aider aider --model $(OLLAMA_MODEL)

# Start aider with deepseek model
aider-deepseek: up
	docker-compose exec aider aider --model $(DEEPSEEK_MODEL)

# View logs
logs:
	docker-compose logs -f

# Clean up Docker resources
clean:
	docker-compose down -v
	docker system prune -f

