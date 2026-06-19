SHELL        := /bin/bash
COMPOSE_FILE := deployments/docker-compose.yml
COMPOSE      := podman compose -f $(COMPOSE_FILE)
GO           := podman run --rm -v "$(CURDIR):/app" -w /app golang:1.26-alpine

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
	$(GO) go build ./...

test:
	$(GO) go test -race ./...

tidy:
	$(GO) go mod tidy

lint:
	podman run --rm -v "$(CURDIR):/app" -w /app golangci/golangci-lint:latest golangci-lint run ./...

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
