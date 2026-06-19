SHELL        := /bin/bash
COMPOSE_FILE := deployments/docker-compose.yml
COMPOSE      := docker compose -f $(COMPOSE_FILE)

.PHONY: up down logs build test tidy seed lint

## ── Docker Compose ────────────────────────────────────────────────────────────

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

## ── Local Go ──────────────────────────────────────────────────────────────────

build:
	go build ./...

test:
	go test -race ./...

tidy:
	go mod tidy

lint:
	golangci-lint run ./...

## ── Dev helpers ───────────────────────────────────────────────────────────────

# Send a sample POST to the ingestion-api (requires the stack to be running)
seed:
	curl -i -X POST http://localhost:8080/api/v1/records \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: seed-$(shell date +%s)" \
		-d '{"type":"order.created","amount":4200}'

# Tail a single service: make service=ingestion-api tail
tail:
	$(COMPOSE) logs -f $(service)
