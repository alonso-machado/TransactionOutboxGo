SHELL        := /bin/bash
COMPOSE_FILE := docker-compose.yml
COMPOSE      := podman compose -f $(COMPOSE_FILE)
GO           := podman run --rm -v "$(CURDIR):/app" -w /app golang:1.26-alpine

.PHONY: up down logs build test tidy seed seed-pix seed-boleto seed-transfer seed-card lint swag test-unit test-integration coverage coverage-all observability-up migrate migrate-down replay-dead drain-dlq purge-loadtest-dlq

## ── Docker Compose ────────────────────────────────────────────────────────────

up: migrate
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v

# Phase 5 Track 1: golang-migrate as a container, not the app, applies
# migrations/*.up.sql against the compose Postgres before `up` brings up the
# app services. Connects on localhost using compose's published Postgres
# port, so Postgres must already be up — `make up`'s dependency on this
# target starts it first via the one-shot `migrate` compose service.
POSTGRES_USER     ?= outbox
POSTGRES_PASSWORD ?= outbox
POSTGRES_PORT     ?= 5432
POSTGRES_DB       ?= outbox
MIGRATE_DSN := postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable

migrate:
	$(COMPOSE) up --build -d postgres
	podman run --rm -v "$(CURDIR)/migrations:/migrations" --network host \
		migrate/migrate -path=/migrations -database "$(MIGRATE_DSN)" up

migrate-down:
	podman run --rm -v "$(CURDIR)/migrations:/migrations" --network host \
		migrate/migrate -path=/migrations -database "$(MIGRATE_DSN)" down 1

# Phase 5 Track 2.C — DLQ replay convenience targets. METHOD="" (default)
# replays across every method for replay-dead; drain-dlq requires METHOD.
# --network host + localhost DSNs so this reaches the compose Postgres/
# RabbitMQ published ports, same pattern as test-integration.
LIMIT ?= 100
ADMIN_DATABASE_URL := postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable
ADMIN_RABBITMQ_URL  := amqp://$${RABBITMQ_USER:-guest}:$${RABBITMQ_PASSWORD:-guest}@localhost:$${RABBITMQ_AMQP_PORT:-5672}/

replay-dead:
	podman run --rm --network host -v "$(CURDIR):/app" -w /app \
		-e DATABASE_URL="$(ADMIN_DATABASE_URL)" -e RABBITMQ_URL="$(ADMIN_RABBITMQ_URL)" \
		golang:1.26-alpine go run ./cmd/outbox-admin replay-dead --method "$(METHOD)" --limit "$(LIMIT)"

drain-dlq:
	podman run --rm --network host -v "$(CURDIR):/app" -w /app \
		-e DATABASE_URL="$(ADMIN_DATABASE_URL)" -e RABBITMQ_URL="$(ADMIN_RABBITMQ_URL)" \
		golang:1.26-alpine go run ./cmd/outbox-admin drain-dlq --method "$(METHOD)"

# Removes only providerName="LOADTEST" messages from a DLQ (set by every
# loadtest/*.js script) — any other message is left in place untouched.
# Safe to run against a DLQ that has a mix of real and loadtest messages,
# e.g. after load-testing in a UAT environment.
purge-loadtest-dlq:
	podman run --rm --network host -v "$(CURDIR):/app" -w /app \
		-e DATABASE_URL="$(ADMIN_DATABASE_URL)" -e RABBITMQ_URL="$(ADMIN_RABBITMQ_URL)" \
		golang:1.26-alpine go run ./cmd/outbox-admin purge-loadtest-dlq --method "$(METHOD)"

# Minimal subset for working on dashboards without the full stack: the app
# services (so there's something to scrape) plus Prometheus/Grafana/the
# postgres exporter. Grafana: http://localhost:3000 (admin/admin by default,
# see GRAFANA_ADMIN_USER/PASSWORD in .env.example).
observability-up:
	$(COMPOSE) up --build -d postgres rabbitmq ingestion-api consumer-pix prometheus grafana postgres_exporter

logs:
	$(COMPOSE) logs -f

## ── Local Go ──────────────────────────────────────────────────────────────────

build:
	$(GO) go build ./...

test:
	$(GO) sh -c "apk add --no-cache gcc musl-dev >/dev/null && go test -race ./..."

test-unit:
	$(GO) sh -c "apk add --no-cache gcc musl-dev >/dev/null && go test -race -coverprofile=unit.cov -coverpkg=./internal/... ./internal/..."

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
				-coverprofile=integration.cov \
				-coverpkg=./internal/... \
				./tests/integration/..."

coverage:
	$(GO) sh -c "go tool cover -html=unit.cov -o unit-coverage.html && go tool cover -func=unit.cov | grep total"

# Merges the unit (test-unit) and integration (test-integration) coverage
# profiles into one number for the whole internal/ tree — TestContainers
# carries most of adapter/persistence, infrastructure/database, and
# infrastructure/rabbitmq, which a unit-only profile can't reach, so reporting
# either profile alone understates real coverage. gocovmerge is fetched ad hoc
# via `go run pkg@version`, so it's never added to go.mod.
coverage-all: test-unit test-integration
	$(GO) sh -c "go run github.com/wadey/gocovmerge@latest unit.cov integration.cov > merged.cov && \
		go tool cover -html=merged.cov -o merged-coverage.html && \
		go tool cover -func=merged.cov | tail -1"

tidy:
	$(GO) go mod tidy

lint:
	podman run --rm -v "$(CURDIR):/app" -w /app golangci/golangci-lint:latest golangci-lint run ./...

# Regenerate docs/swagger.json, docs/swagger.yaml, docs/docs.go from swaggo annotations.
swag:
	$(GO) sh -c "go run github.com/swaggo/swag/cmd/swag init -g cmd/ingestion-api/main.go -o docs --parseDependency"

## ── Dev helpers ───────────────────────────────────────────────────────────────

# Send sample POSTs to the ingestion-api (requires the stack to be running).
# `seed` defaults to the PIX sample; seed-boleto/seed-transfer/seed-card cover the other methods.
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

# Sends a CARTAO_CREDITO sample; cardNumber here is a well-known test PAN
# (never a real one) and is masked to its last 4 digits by the handler
# before it's stored or published — see CardDetailsDTO/maskPAN.
seed-card:
	curl -i -X POST http://localhost:8080/api/v1/payments \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: seed-card-$(shell date +%s)" \
		-d '{"eventId":"evt_seed_card_1","provider":{"name":"ACQUIRER","providerPaymentId":"card-001"},"payment":{"paymentId":"pay_card_1","amount":199.90,"currency":"BRL","method":"CARTAO_CREDITO"},"cartao_credito":{"cardNumber":"4111111111111111","cardType":"CREDIT","cardIssuer":"VISA"},"occurredAt":"2026-06-19T18:30:00Z"}'

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

## ── k6 load tests (Track 6) ─────────────────────────────────────────────────────

.PHONY: loadtest-up loadtest loadtest-report loadtest-autoscale k6-ext-build loadtest-consumer

# Minimal-footprint subset of the SAME docker-compose.yml for running k6 on
# a small machine: one Postgres + one RabbitMQ + ingestion-api + a single
# consumer-worker bound to PIX only — no Jaeger, no other 4 consumers.
# `up <services>` only starts what's named here plus their depends_on
# (jaeger is deliberately not a dependency of either service — see
# docker-compose.yml's comments — so it's correctly excluded). Use the full
# `make up` instead when you need every method actually consumed.
# Tear down with the regular `make down` — same compose project either way.
loadtest-up:
	$(COMPOSE) up --build -d postgres rabbitmq ingestion-api consumer-pix

# 6.1 — two-phase latency baseline (P95/P99). VUS defaults to 100 (the
# original two-phase design) but override it down for a small machine, e.g.
# `make loadtest VUS=20` on a 4-core box — see loadtest/README.md.
loadtest:
	podman run --rm -i -e TARGET_URL=$(TARGET_URL) -e VUS=$(VUS) -v "$(CURDIR)/loadtest:/lt" -w /lt \
		grafana/k6 run k6-baseline.js

# Same as `loadtest`, but archives the full default summary to JSON —
# named -baseline so it sits side by side with loadtest-consumer's
# -consumer one for comparison (6.1 vs 6.3 numbers, run to run).
loadtest-report:
	podman run --rm -i -e TARGET_URL=$(TARGET_URL) -e VUS=$(VUS) -v "$(CURDIR)/loadtest:/lt" -w /lt \
		grafana/k6 run --summary-export=summary-baseline.json k6-baseline.js

# 6.2 — floods one method's queue (METHOD=PIX default) to trigger KEDA
# scale-up, then stops so it scales back to 0. Do NOT pin consumers for this
# one — needs the real KEDA config (min 0 / max 10). Kubernetes-only.
loadtest-autoscale:
	podman run --rm -i -e TARGET_URL=$(TARGET_URL) -e METHOD=$(METHOD) \
		-v "$(CURDIR)/loadtest:/lt" -w /lt grafana/k6 run k6-autoscale.js

# Builds the custom k6 binary (xk6-amqp only) that 6.3 needs — xk6-sql is
# deliberately not included; it only supports k6 v2 while xk6-amqp only
# supports v1, and the two can't currently coexist in one binary (see
# build/k6/Dockerfile's comment for the full story).
k6-ext-build:
	podman build -t k6-ext -f build/k6/Dockerfile .

# 6.3 — publishes N messages at a fixed 100 VUs straight onto one or more
# per-method queues (bypassing ingestion-api), mixed with
# DUP_FRACTION/SCHEMA_FRACTION duplicate/bad-schema-version messages by
# default so one run exercises the consumer's whole outcome taxonomy.
# METHODS (comma-separated, e.g. PIX,TRANSFER) splits N evenly via
# round-robin — useful with `podman compose up -d consumer-pix-extra` to
# compare 2 consumers on one method against 1 on another (see loadtest/README.md's
# "Multiple methods at once" section). METHOD (singular) still works as a
# one-method shorthand. Reports k6's own publish throughput;
# consumer-worker's behavior (ack/duplicate/unknown_schema_version/...) is
# read from its own /metrics, not from this command — see
# loadtest/README.md's "Checking consumer behavior". --network host so the
# default RABBITMQ_URL (localhost:5672) reaches the compose-published port.
loadtest-consumer:
	podman run --rm -i --network host -e RABBITMQ_URL=$(RABBITMQ_URL) \
		-e METHODS=$(METHODS) -e METHOD=$(METHOD) -e N=$(N) -e DUP_FRACTION=$(DUP_FRACTION) -e SCHEMA_FRACTION=$(SCHEMA_FRACTION) \
		-v "$(CURDIR)/loadtest:/lt" -w /lt k6-ext run --summary-export=summary-consumer.json k6-consumer.js
