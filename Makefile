SHELL        := /bin/bash
COMPOSE_FILE := deployments/docker-compose.yml
COMPOSE      := podman compose -f $(COMPOSE_FILE)
GO           := podman run --rm -v "$(CURDIR):/app" -w /app golang:1.26-alpine

.PHONY: up down logs build test tidy seed seed-pix seed-boleto seed-transfer lint swag

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

# Regenerate docs/swagger.json, docs/swagger.yaml, docs/docs.go from swaggo annotations.
swag:
	$(GO) sh -c "go run github.com/swaggo/swag/cmd/swag init -g cmd/ingestion-api/main.go -o docs --parseDependency"

## ── Dev helpers ───────────────────────────────────────────────────────────────

# Send sample POSTs to the ingestion-api (requires the stack to be running).
# `seed` defaults to the PIX sample; seed-boleto/seed-transfer cover the other methods.
seed: seed-pix

seed-pix:
	curl -i -X POST http://localhost:8080/api/v1/payments \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: seed-pix-$(shell date +%s)" \
		-d '{"eventId":"evt_seed_pix_1","provider":{"name":"MERCADO_PAGO","providerPaymentId":"987654321"},"payment":{"paymentId":"pay_123","amount":100.50,"currency":"BRL","method":"PIX"},"pix":{"endToEndId":"E123456789ABCDEF","txid":"ORDER123"},"occurredAt":"2026-06-19T18:30:00Z"}'

seed-boleto:
	curl -i -X POST http://localhost:8080/api/v1/payments \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: seed-boleto-$(shell date +%s)" \
		-d '{"eventId":"evt_seed_boleto_1","provider":{"name":"BOLETO_BANCARIO","providerPaymentId":"555000111"},"payment":{"paymentId":"pay_456","amount":250.00,"currency":"BRL","method":"BOLETO"},"boleto":{"barcode":"34191790010104351004791020150008291070026000","dueDate":"2026-07-01","payerDocument":"12345678900"},"occurredAt":"2026-06-19T18:30:00Z"}'

seed-transfer:
	curl -i -X POST http://localhost:8080/api/v1/payments \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: seed-transfer-$(shell date +%s)" \
		-d '{"eventId":"evt_seed_transfer_1","provider":{"name":"INTERNAL","providerPaymentId":"internal-1"},"payment":{"paymentId":"pay_789","amount":75.00,"currency":"USD","method":"TRANSFER","payerId":"018f7f9e-6e8b-7c3a-8f2a-000000000001","recipientId":"018f7f9e-6e8b-7c3a-8f2a-000000000002"},"occurredAt":"2026-06-19T18:30:00Z"}'

# Tail a single service: make service=ingestion-api tail
tail:
	$(COMPOSE) logs -f $(service)

## ── Kubernetes (Track 4) ──────────────────────────────────────────────────────

.PHONY: k8s-apply k8s-delete k8s-status

k8s-apply:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/secret.yaml
	kubectl apply -f k8s/ingestion-api/
	kubectl apply -f k8s/consumer-worker/

k8s-delete:
	kubectl delete -f k8s/ --recursive

k8s-status:
	kubectl get all -n transaction-outbox
	kubectl get scaledobject -n transaction-outbox
