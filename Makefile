SHELL        := /bin/bash
COMPOSE_FILE := docker-compose.yml
COMPOSE      := podman compose -f $(COMPOSE_FILE)
GO           := podman run --rm -v "$(CURDIR):/app" -w /app golang:1.26-alpine

.PHONY: up down logs build test tidy seed seed-pix seed-boleto seed-transfer lint swag test-unit test-integration coverage

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

test-unit:
	$(GO) go test -race -coverprofile=coverage.out ./internal/...

# testcontainers-go needs to launch sibling Postgres/RabbitMQ containers from
# inside this golang:1.26-alpine container — mount the Podman machine's own
# socket (Docker-API-compatible) so testcontainers can talk to it, disable
# Ryuk (its reaper sidecar doesn't get along with Podman), and run with
# --network host so this container can reach the sibling containers'
# 127.0.0.1:<mapped-port> addresses (they're mapped on the Podman VM's host
# network, not on this container's own isolated network namespace).
# -race needs cgo, which golang:1.26-alpine lacks a C toolchain for by
# default — install one before running.
test-integration:
	podman run --rm \
		--network host \
		-v "$(CURDIR):/app" -w /app \
		-v /run/podman/podman.sock:/var/run/docker.sock \
		-e DOCKER_HOST=unix:///var/run/docker.sock \
		-e TESTCONTAINERS_RYUK_DISABLED=true \
		golang:1.26-alpine sh -c "apk add --no-cache gcc musl-dev >/dev/null && \
			go test -tags=integration -race -timeout=300s \
				-coverprofile=coverage.out \
				-coverpkg=./internal/... \
				./tests/integration/..."

coverage:
	$(GO) sh -c "go tool cover -html=coverage.out -o coverage.html && go tool cover -func=coverage.out | grep total"

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

.PHONY: k8s-apply k8s-delete k8s-status k8s-lint k8s-template

# helmcharts/transaction-outbox is a Helm chart: one ingestion-api
# Deployment/Service/HPA, plus one consumer-worker Deployment + KEDA
# ScaledObject pair per entry in values.yaml's paymentMethods list (rendered
# via {{ range }}, not hand-duplicated). Assumes `helm` and `kubectl` are on
# PATH and pointed at the target cluster.
CHART   := helmcharts/transaction-outbox
RELEASE := transaction-outbox
NS      := transaction-outbox

k8s-apply:
	helm upgrade --install $(RELEASE) $(CHART) --namespace $(NS) --create-namespace

k8s-delete:
	helm uninstall $(RELEASE) --namespace $(NS)

k8s-status:
	helm status $(RELEASE) --namespace $(NS)
	kubectl get all -n $(NS)
	kubectl get scaledobject -n $(NS)

k8s-lint:
	helm lint $(CHART)

k8s-template:
	helm template $(RELEASE) $(CHART) --namespace $(NS)
