SHELL        := /bin/bash
COMPOSE_FILE := docker-compose.yml
COMPOSE      := podman compose -f $(COMPOSE_FILE)
GO           := podman run --rm -v "$(CURDIR):/app" -w /app golang:1.26-alpine

.PHONY: up down logs build test tidy seed seed-order seed-order-sports seed-webhook-confirm notification-retry-cron lint swag test-unit test-integration coverage coverage-all observability-up migrate migrate-down replay-dead drain-dlq purge-loadtest-dlq backstage-build backstage-up

## ── Docker Compose ────────────────────────────────────────────────────────────

up: migrate
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v

# golang-migrate as a container, not the app, applies migrations/<set>/*.up.sql
# against the compose Postgres before `up` brings up the app services.
# Connects on localhost using compose's published Postgres port, so Postgres
# must already be up — `make up`'s dependency on this target starts it first.
#
# Two sets, one per logical database (the outbox/events split): outbox/ ->
# the `outbox` DB, events/ -> the `events` DB. The compose postgres init
# script (observability/postgres/init-events.sql) creates `events` only on a
# fresh volume, so migrate also CREATE-DATABASEs it if missing — keeps this
# target idempotent against a pre-existing single-DB volume.
POSTGRES_USER     ?= outbox
POSTGRES_PASSWORD ?= outbox
POSTGRES_PORT     ?= 5432
POSTGRES_DB       ?= outbox
EVENTS_DB         ?= events
MIGRATE_DSN_OUTBOX := postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable
MIGRATE_DSN_EVENTS := postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(EVENTS_DB)?sslmode=disable

# Roll back one step in this set (default outbox); override with DB=events.
MIGRATE_DOWN_PATH ?= migrations/outbox
MIGRATE_DOWN_DSN  ?= $(MIGRATE_DSN_OUTBOX)

migrate:
	$(COMPOSE) up --build -d postgres
	$(COMPOSE) exec -T postgres sh -c \
		"psql -U $(POSTGRES_USER) -tc \"SELECT 1 FROM pg_database WHERE datname='$(EVENTS_DB)'\" | grep -q 1 || createdb -U $(POSTGRES_USER) $(EVENTS_DB)"
	podman run --rm -v "$(CURDIR)/migrations:/migrations" --network host \
		migrate/migrate -path=/migrations/outbox -database "$(MIGRATE_DSN_OUTBOX)" up
	podman run --rm -v "$(CURDIR)/migrations:/migrations" --network host \
		migrate/migrate -path=/migrations/events -database "$(MIGRATE_DSN_EVENTS)" up

migrate-down:
	podman run --rm -v "$(CURDIR)/migrations:/migrations" --network host \
		migrate/migrate -path=/$(MIGRATE_DOWN_PATH) -database "$(MIGRATE_DOWN_DSN)" down 1

# DLQ replay convenience targets. EVENT_TYPE="" (default) replays across
# every event type for replay-dead; drain-dlq/purge-loadtest-dlq require
# STREAM, EVENT_TYPE, EVENT_SUBTYPE. --network host + localhost DSNs so this
# reaches the compose Postgres/RabbitMQ published ports, same pattern as
# test-integration.
LIMIT       ?= 100
OUTBOX      ?= order
STREAM      ?= order
EVENT_TYPE    ?=
EVENT_SUBTYPE ?=
ADMIN_DATABASE_URL := postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable
ADMIN_RABBITMQ_URL  := amqp://$${RABBITMQ_USER:-guest}:$${RABBITMQ_PASSWORD:-guest}@localhost:$${RABBITMQ_AMQP_PORT:-5672}/

replay-dead:
	podman run --rm --network host -v "$(CURDIR):/app" -w /app \
		-e DATABASE_URL="$(ADMIN_DATABASE_URL)" -e RABBITMQ_URL="$(ADMIN_RABBITMQ_URL)" \
		golang:1.26-alpine go run ./cmd/outbox-admin replay-dead --outbox "$(OUTBOX)" --event-type "$(EVENT_TYPE)" --limit "$(LIMIT)"

drain-dlq:
	podman run --rm --network host -v "$(CURDIR):/app" -w /app \
		-e DATABASE_URL="$(ADMIN_DATABASE_URL)" -e RABBITMQ_URL="$(ADMIN_RABBITMQ_URL)" \
		golang:1.26-alpine go run ./cmd/outbox-admin drain-dlq --stream "$(STREAM)" --event-type "$(EVENT_TYPE)" --event-subtype "$(EVENT_SUBTYPE)"

# Removes only messages a loadtest run marked (see cmd/outbox-admin's
# loadtestMarker) from a DLQ — any other message is left in place untouched.
# Safe to run against a DLQ that has a mix of real and loadtest messages,
# e.g. after load-testing in a UAT environment.
purge-loadtest-dlq:
	podman run --rm --network host -v "$(CURDIR):/app" -w /app \
		-e DATABASE_URL="$(ADMIN_DATABASE_URL)" -e RABBITMQ_URL="$(ADMIN_RABBITMQ_URL)" \
		golang:1.26-alpine go run ./cmd/outbox-admin purge-loadtest-dlq --stream "$(STREAM)" --event-type "$(EVENT_TYPE)" --event-subtype "$(EVENT_SUBTYPE)"

# Minimal subset for working on dashboards without the full stack: the app
# services (so there's something to scrape) plus Prometheus/Grafana/the
# postgres exporter. Grafana: http://localhost:3000 (admin/admin by default,
# see GRAFANA_ADMIN_USER/PASSWORD in .env.example).
observability-up:
	$(COMPOSE) up --build -d postgres rabbitmq ingestion-api order-consumer-worker-concert-rock prometheus grafana postgres_exporter

logs:
	$(COMPOSE) logs -f

# Build/run the Backstage developer portal on its own (backstage/Dockerfile
# is a heavier multi-stage Node build than the rest of this repo's `make
# up`, so these are split out rather than only reachable via the full
# stack). http://localhost:7007 once up — see catalog-info.yaml.
backstage-build:
	$(COMPOSE) build backstage

backstage-up:
	$(COMPOSE) up --build -d backstage

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

# Regenerate each API service's swagger.json/swagger.yaml/docs.go from swaggo
# annotations, into its own docs/<service> package — one per service with a
# real HTTP surface (ingestion-api, tickets-api), not a single shared docs/
# (two `-g` entrypoints would otherwise both generate `package docs` into the
# same directory and collide). Both cmd/*/main.go import the SAME
# internal/adapter/http package, and swag's --parseDependency walks that
# whole package's annotations regardless of which handlers a given main.go
# actually wires into its router — without filtering, ingestion-api's spec
# would also pick up tickets-api's order-status/checkin/ticket-holder
# routes (and vice versa; confirmed empirically — swag's --exclude flag
# does NOT prune routes discovered via --parseDependency in this shared-
# package setup, even though its --help text implies it should). --tags
# filters by the swaggo @Tags annotation instead, which IS respected —
# each handler's @Tags value is unique per service (order_status_handler.go
# was retagged order-status, distinct from order_handler.go's orders, so
# the two don't collide).
swag:
	$(GO) sh -c "go run github.com/swaggo/swag/cmd/swag init -g cmd/ingestion-api/main.go -o docs/ingestion-api --parseDependency --tags orders,webhooks,health"
	$(GO) sh -c "go run github.com/swaggo/swag/cmd/swag init -g cmd/tickets-api/main.go -o docs/tickets-api --parseDependency --tags order-status,checkin,tickets,health"

## ── Dev helpers ───────────────────────────────────────────────────────────────

# Send sample POSTs to the ingestion-api (requires the stack to be running).
# `seed` defaults to the CONCERT/ROCK sample; seed-order-sports covers the
# other shard running locally by default (order-consumer-worker-sports-football).
seed: seed-order

seed-order:
	curl -i -X POST http://localhost:8080/api/v1/orders \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: seed-order-$(shell date +%s)" \
		-d '{"sourceOrderId":"order-seed-concert-rock-1","eventType":"CONCERT","eventSubtype":"ROCK","eventId":"evt_concert_1","eventName":"Rock in Rio","venue":{"id":"venue-1","name":"Estadio Nacional","city":"Sao Paulo"},"tickets":[{"id":"TKT-1","section":"A","row":"10","seat":"5","price":150.00,"currency":"BRL"}],"customer":{"name":"Jane Doe","email":"jane@example.com","document":"12345678900"}}'

seed-order-sports:
	curl -i -X POST http://localhost:8080/api/v1/orders \
		-H "Content-Type: application/json" \
		-H "Idempotency-Key: seed-order-sports-$(shell date +%s)" \
		-d '{"sourceOrderId":"order-seed-sports-football-1","eventType":"SPORTS","eventSubtype":"FOOTBALL","eventId":"evt_sports_1","eventName":"Championship Final","venue":{"id":"venue-2","name":"Arena Sul","city":"Porto Alegre"},"tickets":[{"id":"TKT-2","section":"B","row":"3","seat":"12","price":80.00,"currency":"BRL"}],"customer":{"name":"John Roe","email":"john@example.com","document":"98765432100"}}'

# Simulates the fake gateway's webhook confirming payment for an order placed
# via seed-order — look up the order's Charge.ProviderRef first (e.g.
# `SELECT provider_ref FROM charges WHERE order_id = '<uuid>'` against the
# events DB) and pass it as PROVIDER_REF.
PROVIDER_REF ?=
seed-webhook-confirm:
	curl -i -X POST http://localhost:8080/api/v1/webhooks/payments/fake \
		-H "Content-Type: application/json" \
		-d '{"provider_ref":"$(PROVIDER_REF)","event_id":"seed-webhook-$(shell date +%s)","outcome":"CONFIRMED","event_type":"CONCERT","event_subtype":"ROCK"}'

# One-shot retry pass over ticket_notifications (mirrors the
# notification-retry-cron Kubernetes CronJob locally — see
# docker-compose.yml's comment on why this can't just loop in the container).
notification-retry-cron:
	$(COMPOSE) --profile tools run --rm notification-retry-cron

# Tail a single service: make service=ingestion-api tail
tail:
	$(COMPOSE) logs -f $(service)

## ── Kubernetes (Track 4) ──────────────────────────────────────────────────────

.PHONY: k8s-apply k8s-delete k8s-status k8s-lint k8s-template

# helmcharts/transaction-outbox is a Helm chart: one ingestion-api
# Deployment/Service/HPA, plus one order-consumer-worker and one fulfillment-consumer-worker
# Deployment + KEDA ScaledObject pair per (event_type, event_subtype) entry
# in values.yaml (rendered via {{ range }}, not hand-duplicated). Assumes
# `helm` and `kubectl` are on PATH and pointed at the target cluster.
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
# order-consumer-worker shard (CONCERT/ROCK) — no Tempo, no other shards. `up
# <services>` only starts what's named here plus their depends_on. Use the
# full `make up` instead when you need every shard actually consumed.
# Tear down with the regular `make down` — same compose project either way.
loadtest-up:
	$(COMPOSE) up --build -d postgres rabbitmq ingestion-api order-consumer-worker-concert-rock

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

# 6.2 — floods one shard's order queue (EVENT_TYPE=CONCERT/EVENT_SUBTYPE=ROCK
# default) to trigger KEDA scale-up, then stops so it scales back to 0. Do
# NOT pin consumers for this one — needs the real KEDA config (min 0 / max
# 10). Kubernetes-only.
loadtest-autoscale:
	podman run --rm -i -e TARGET_URL=$(TARGET_URL) -e EVENT_TYPE=$(EVENT_TYPE) -e EVENT_SUBTYPE=$(EVENT_SUBTYPE) \
		-v "$(CURDIR)/loadtest:/lt" -w /lt grafana/k6 run k6-autoscale.js

# Builds the custom k6 binary (xk6-amqp only) that 6.3 needs — xk6-sql is
# deliberately not included; it only supports k6 v2 while xk6-amqp only
# supports v1, and the two can't currently coexist in one binary (see
# build/k6/Dockerfile's comment for the full story).
k6-ext-build:
	podman build -t k6-ext -f build/k6/Dockerfile .

# 6.3 — publishes N messages at a fixed 100 VUs straight onto one or more
# shards' order queues (bypassing ingestion-api), mixed with
# DUP_FRACTION/SCHEMA_FRACTION duplicate/bad-schema-version messages by
# default so one run exercises order-consumer-worker's whole outcome
# taxonomy. SHARDS (comma-separated "eventType:eventSubtype" pairs, e.g.
# CONCERT:ROCK,SPORTS:FOOTBALL) splits N evenly via round-robin — useful for
# comparing two shards' consumer/DB throughput side by side (see
# loadtest/README.md's "Multiple shards at once" section). Reports k6's own
# publish throughput; order-consumer-worker's behavior
# (ack/duplicate/unknown_schema_version/...) is read from its own /metrics,
# not from this command — see loadtest/README.md's "Checking consumer
# behavior". --network host so the default RABBITMQ_URL (localhost:5672)
# reaches the compose-published port.
loadtest-consumer:
	podman run --rm -i --network host -e RABBITMQ_URL=$(RABBITMQ_URL) \
		-e SHARDS=$(SHARDS) -e N=$(N) -e DUP_FRACTION=$(DUP_FRACTION) -e SCHEMA_FRACTION=$(SCHEMA_FRACTION) \
		-v "$(CURDIR)/loadtest:/lt" -w /lt k6-ext run --summary-export=summary-consumer.json k6-consumer.js
