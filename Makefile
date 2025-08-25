# Makefile for Docker Compose operations

# Default target
all: build

# Bring down all services
down:
	docker compose down

# Build and start all services in detached mode
up:
	docker compose up --build -d

# Clean up: stop and remove containers, networks, and volumes
clean:
	docker-compose down -v --remove-orphans

.PHONY: all down up clean
