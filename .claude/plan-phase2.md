# Plan Phase 2 — Observability, Testing, Docs & Kubernetes

> **When to execute:** after Phase 1 coding is complete and the stack runs
> end-to-end via Docker Compose.  
> **Prerequisite:** `.claude/plan.md` fully implemented and `make up` passes the
> Phase 1 verification checklist.

---

## Overview

Four independent tracks, recommended execution order:

| # | Track | Why this order |
|---|---|---|
| 1 | **OpenTelemetry** | Instrument the code first — tests and K8s will benefit from traces |
| 2 | **OpenAPI / Swagger** | Annotate handlers (already fully written) and serve UI |
| 3 | **TestContainers** | Integration tests run against real infra; OTel traces help debug failures |
| 4 | **Kubernetes + KEDA** | Deploy last, once the app is observable and tested |

---

## Track 1 — OpenTelemetry (Traces · Metrics · Logs)

### Goal
Structured observability across the full pipeline: every HTTP request, every
outbox dispatch cycle, and every consumer message gets a trace with correlated
structured logs and counter/gauge metrics.

### Decisions
- **SDK:** `go.opentelemetry.io/otel` (official Go SDK, no vendor lock-in).
- **Propagation:** W3C TraceContext (`traceparent` header) — standard, works with
  every modern collector.
- **Exporter (local dev):** OTLP → **Jaeger all-in-one** (add to
  `docker-compose.yml`). Jaeger UI on `:16686`.
- **Exporter (production path):** OTLP → any collector (Grafana, Datadog, etc.) —
  just swap the endpoint env var.
- **Logging:** Go's stdlib `slog` (available since Go 1.21) with a custom
  `slog.Handler` that injects `trace_id` and `span_id` into every log line.
  **No third-party logging library.**
- **Metrics:** `go.opentelemetry.io/otel/metric` with a Prometheus exporter so
  Prometheus/Grafana can scrape them. Expose `/metrics` on a separate port (`:9090`
  by default).

### Instrumentation points

| Component | What to instrument |
|---|---|
| `adapter/http` | `otelgin` middleware — auto-spans every HTTP request with method, path, status |
| `usecase/ingest` | Span: `ingest.payment` — attributes: `idempotency_key`, `http_method`, dedup hit/miss |
| `usecase/outbox` (`DispatchOutbox`) | Span per poll cycle: `outbox.dispatch` — attributes: `batch_size`, `published_count`, `retrying_count`, `dead_letter_count`; counter metric `outbox.published_total`, gauge `outbox.pending_count` |
| `adapter/persistence` (GORM) | `otelgorm` plugin — auto-spans every DB query |
| `adapter/messaging` (publisher) | Span: `rabbitmq.publish` — inject TraceContext into message headers |
| `adapter/messaging` (consumer) | Span: `rabbitmq.consume` — extract TraceContext from message headers; attributes: `message_id`, `redelivered`, dedup hit/miss |
| `usecase/consume` (`ProcessMessage`) | Child span: `process.message` — attributes: `payment_id`, outcome |

### New dependencies
```
go.opentelemetry.io/otel
go.opentelemetry.io/otel/trace
go.opentelemetry.io/otel/metric
go.opentelemetry.io/otel/sdk
go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp
go.opentelemetry.io/otel/exporters/prometheus
go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin
go.opentelemetry.io/contrib/instrumentation/gorm.io/gorm/otelgorm
```

### Files to add / modify
- `internal/infrastructure/telemetry/` — new package: `Setup(ctx, cfg)` returns
  a shutdown function; initialises TracerProvider + MeterProvider + slog handler.
  Called from both `cmd/ingestion-api/main.go` and `cmd/consumer-worker/main.go`.
- `internal/adapter/http/middleware.go` — add `otelgin.Middleware(serviceName)`.
- `internal/adapter/messaging/publisher.go` — inject W3C headers before publish.
- `internal/adapter/messaging/consumer.go` — extract W3C headers on receive.
- `internal/adapter/persistence/db.go` — register `otelgorm` plugin on GORM DB.
- `internal/infrastructure/config/` — add `OtelEndpoint`, `MetricsPort` fields.
- `deployments/docker-compose.yml` — add `jaeger` service (all-in-one image).
- `.env.example` — add `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`,
  `METRICS_PORT`.

### Docker Compose addition
```yaml
jaeger:
  image: jaegertracing/all-in-one:latest
  ports:
    - "16686:16686"   # UI
    - "4318:4318"     # OTLP HTTP receiver
  networks:
    - outbox
```

### Verification
1. `make up` → open `http://localhost:16686` → Jaeger UI.
2. `make seed` → find trace in Jaeger spanning HTTP → DB write → outbox dispatch
   → RabbitMQ publish → consumer → DB persist.
3. `curl http://localhost:9090/metrics` → see `outbox_published_total` counter.
4. Logs in `docker compose logs ingestion-api` show `trace_id` field on every line.

---

## Track 2 — OpenAPI / Swagger

### Goal
Interactive API documentation served at `/swagger/index.html` during development,
generated from Go annotations on the Gin handlers. No manual YAML maintenance.

### Decisions
- **Generator:** `github.com/swaggo/swag` CLI + `github.com/swaggo/gin-swagger`
  middleware. Industry standard for Gin.
- **Scope:** development only. The swagger route is **disabled in production** via
  a build tag (`//go:build swagger`) or an env flag `SWAGGER_ENABLED=true`.
- **Format:** OpenAPI 3.0 spec generated to `docs/swagger.json` +
  `docs/swagger.yaml` — committed to the repo so it can be consumed by API
  gateways without running the server.

### What to annotate

| Handler | Annotations |
|---|---|
| `POST /api/v1/payments` | Request body schema, `201` response with `{paymentId, idempotencyKey, status}`, `400` for invalid body |
| `PUT /api/v1/payments` | Same as POST |
| `PATCH /api/v1/payments` | Same as POST |
| `GET /healthz` | `200 {"status":"ok"}` |

**Shared schemas to define:**
- `PaymentRequest` — `payerId` (UUID), `recipientId` (UUID), `amount` (int64,
  minor units), `currency` (string, ISO 4217, optional — defaults to `USD`)
- `PaymentResponse` — `paymentId` (string, UUID), `idempotencyKey` (string),
  `status` (string: `"accepted"` | `"duplicate"`)
- `ErrorResponse` — `error` (string), `detail` (string)

### Files to add / modify
- `internal/adapter/http/handlers.go` — add `// @Summary`, `// @Param`,
  `// @Success`, `// @Failure`, `// @Router` annotations.
- `internal/adapter/http/router.go` — register swagger route
  (`ginSwagger.WrapHandler`) under build tag or config guard.
- `docs/` — generated by `swag init`; commit `swagger.json` + `swagger.yaml`.
- `Makefile` — add `swag` target: `swag init -g cmd/ingestion-api/main.go -o docs`.

### New dependencies
```
github.com/swaggo/swag          (CLI tool — dev dependency)
github.com/swaggo/gin-swagger
github.com/swaggo/files
```

### Makefile target
```makefile
swag:
	swag init -g cmd/ingestion-api/main.go -o docs --parseDependency
```

### Verification
1. `make swag && make up`.
2. Open `http://localhost:8080/swagger/index.html`.
3. Execute `POST /api/v1/payments` from the UI → verify `201` with `paymentId`.
4. Execute the same request again → verify `status: "duplicate"` in the response.

---

## Track 3 — TestContainers (Integration & E2E)

### Goal
>75% code coverage on the main execution paths using **real infrastructure
containers** (Postgres 17, RabbitMQ 4.3). Mocks are used only for things that
cannot be containerised (e.g. time, UUID generation).

### Decisions
- **Library:** `github.com/testcontainers/testcontainers-go` + modules
  `testcontainers-go/modules/postgres` and `testcontainers-go/modules/rabbitmq`.
- **Test layout:** `tests/integration/` for shared container setup;
  integration test files follow `_integration_test.go` naming and use
  `//go:build integration` build tag so `go test ./...` skips them by default.
- **Run command:** `go test -tags=integration -race -coverprofile=coverage.out ./...`
- **Container lifecycle:** one shared Postgres + RabbitMQ per test suite (not per
  test), started via `TestMain`. Truncate tables between tests for isolation —
  do not restart containers.
- **No repository mocks:** the outbox repository, the payments repository, and
  the publisher are all tested against real containers. Only pure-logic
  helpers (hash computation, key derivation) use unit tests.

### Main paths to cover (>75% target)

| # | Path | What to assert |
|---|---|---|
| 1 | **Happy path E2E** | `POST /api/v1/payments` → outbox row created (`NEW`) → `DispatchOutbox` publishes → consumer inserts `payments` row → outbox status `PUBLISHED` |
| 2 | **Idempotent duplicate at ingest** | Same business fields + no `Idempotency-Key` sent twice → second request returns `status: "duplicate"`, only one outbox row |
| 3 | **Idempotency-Key header dedup** | Two different bodies with same `Idempotency-Key` → second insert is no-op, same row |
| 4 | **Distinct keys no dedup** | Same body with different `Idempotency-Key` values → two separate outbox rows |
| 5 | **DispatchOutbox: publish confirm** | Mock RabbitMQ broker drop (stop container) → outbox stays `NEW`; restart → eventually `PUBLISHED` |
| 6 | **DispatchOutbox: max retries → dead letter** | Simulate N consecutive publish errors → row transitions `NEW → RETRYING → DEAD_LETTER` after `MAX_RETRIES` |
| 7 | **Consumer dedup via UNIQUE constraint** | Deliver same message twice (requeue) → only one `payments` row (`ON CONFLICT (source_message_id) DO NOTHING`) |
| 8 | **Consumer poison message → DLQ** | Deliver a malformed/unprocessable message `RABBITMQ_MAX_REDELIVERIES` times → message lands in `payments.dlq`, consumer ACKs and continues |
| 9 | **Outbox pruning** | Insert `PUBLISHED` rows older than `OUTBOX_PRUNE_AFTER_HOURS` → prune ticker removes them; recent rows stay |
| 10 | **Concurrent ingestion** | 50 concurrent POSTs with unique bodies → 50 outbox rows, all eventually `PUBLISHED` and persisted exactly once |

### Test package structure
```
tests/
└── integration/
    ├── suite_test.go          # TestMain: start Postgres + RabbitMQ containers,
    │                          #   run AutoMigrate, declare topology, run m.Run(), teardown
    ├── ingest_test.go         # paths 1–4
    ├── dispatch_outbox_test.go # paths 5–6
    ├── consumer_test.go       # paths 7–8
    ├── prune_test.go          # path 9
    └── concurrent_test.go     # path 10
```

### New dependencies
```
github.com/testcontainers/testcontainers-go
github.com/testcontainers/testcontainers-go/modules/postgres
github.com/testcontainers/testcontainers-go/modules/rabbitmq
github.com/stretchr/testify
```

### Makefile targets
Go is not installed on the host — these run inside `golang:1.26-alpine` via
Podman, same convention as `make build`/`make lint`:
```makefile
test-unit:
	podman run --rm -v "$(CURDIR):/app" -w /app golang:1.26-alpine \
		go test -race -coverprofile=coverage.out ./internal/...

test-integration:
	podman run --rm -v "$(CURDIR):/app" -w /app golang:1.26-alpine \
		go test -tags=integration -race -timeout=300s \
			-coverprofile=coverage.out \
			-coverpkg=./internal/... \
			./tests/integration/...

coverage:
	podman run --rm -v "$(CURDIR):/app" -w /app golang:1.26-alpine sh -c \
		"go tool cover -html=coverage.out -o coverage.html && go tool cover -func=coverage.out | grep total"
```

### Coverage measurement
Run `make test-integration coverage`. The `total:` line must show ≥ 75%.
If it falls short, identify uncovered branches with `go tool cover -html` and
add targeted tests before marking this track complete.

### Verification
1. `make test-integration` → all tests pass.
2. `make coverage` → `total: (statements) ≥ 75.0%`.
3. `coverage.html` shows green on all paths listed in the table above.

---

## Track 4 — Kubernetes + HPA + KEDA

### Goal
Production-ready K8s manifests for both services. `ingestion-api` scales on
CPU/memory (standard HPA). `consumer-worker` scales on **RabbitMQ queue depth**
via KEDA — from 0 replicas when idle to N replicas under load.

### Decisions
- **Manifest format:** plain YAML under `k8s/` — no Helm yet (can be added later).
- **Namespace:** `transaction-outbox` (all resources in one namespace).
- **Image:** the same multi-stage `build/Dockerfile`; images tagged by git SHA in CI.
- **Config:** K8s `ConfigMap` for non-sensitive values, `Secret` for passwords.
- **KEDA version:** 2.x (`ScaledObject` API v1alpha1).
- **KEDA trigger:** `rabbitmq` trigger using the management HTTP API (no AMQP
  credentials in KEDA — uses `http://rabbitmq:15672/api/...` endpoint).
- **HPA on ingestion-api:** standard CPU target 70%, min 2 / max 10 replicas.
- **KEDA on consumer-worker:** queue depth trigger, min 0 / max 20 replicas,
  1 replica per 100 messages in the queue (`targetQueueLength: 100`).

### File structure
```
k8s/
├── namespace.yaml
├── configmap.yaml
├── secret.yaml                    # base64 placeholders — replace in CI/CD
├── ingestion-api/
│   ├── deployment.yaml
│   ├── service.yaml               # ClusterIP + optional LoadBalancer/Ingress
│   └── hpa.yaml                   # HorizontalPodAutoscaler (CPU 70%)
├── consumer-worker/
│   ├── deployment.yaml
│   └── scaledobject.yaml          # KEDA ScaledObject (RabbitMQ queue trigger)
└── rabbitmq/
    └── NOTE.md                    # note: use RabbitMQ Cluster Operator in prod,
                                   #   not this manifest
```

### Key manifest details

**`consumer-worker/scaledobject.yaml`**
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: consumer-worker
  namespace: transaction-outbox
spec:
  scaleTargetRef:
    name: consumer-worker
  minReplicaCount: 0          # scale to zero when idle
  maxReplicaCount: 20
  pollingInterval: 15         # seconds between queue checks
  cooldownPeriod: 60          # seconds to wait before scaling down
  triggers:
    - type: rabbitmq
      metadata:
        protocol: http
        queueName: payments.queue
        mode: QueueLength
        value: "100"          # 1 replica per 100 messages
        hostFromEnv: RABBITMQ_MANAGEMENT_URL
```

**`ingestion-api/hpa.yaml`**
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ingestion-api
  namespace: transaction-outbox
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ingestion-api
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
```

**Resource requests/limits (both services)**
```yaml
resources:
  requests:
    cpu: "100m"
    memory: "128Mi"
  limits:
    cpu: "500m"
    memory: "256Mi"
```

**Liveness / readiness probes (ingestion-api)**
```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 15

readinessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

**Readiness probe (consumer-worker)**  
Since the consumer has no HTTP server, readiness is checked by verifying the
process is alive (exec probe against a lightweight health file written by the
worker on startup, or use `startupProbe` + `livenessProbe` on PID 1).

### KEDA prerequisites
Add to setup notes (not implemented in this plan):
- `kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.x.x/keda-2.x.x.yaml`
- RabbitMQ management plugin must be enabled (`rabbitmq:4.3-management` already does this).
- `RABBITMQ_MANAGEMENT_URL` secret: `http://<user>:<pass>@rabbitmq:15672`

### Env var injection strategy
- `ConfigMap` → `envFrom` in Deployment (non-sensitive: ports, queue names,
  polling intervals, OTel endpoint).
- `Secret` → individual `env.valueFrom.secretKeyRef` entries (passwords, DSN).

### Makefile targets
```makefile
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
```

### Verification
1. `make k8s-apply` → all pods `Running`.
2. `kubectl get hpa -n transaction-outbox` → ingestion-api HPA shows current/target
   CPU.
3. `kubectl get scaledobject -n transaction-outbox` → `READY=True`.
4. Generate load: `for i in {1..500}; do make seed; done` → observe
   `kubectl get pods -n transaction-outbox -w` — consumer-worker replicas increase.
5. Stop load → after cooldown period, consumer-worker scales back to 0.

---

## Things to keep in mind across all tracks

1. **OTel before tests** — having traces in integration tests makes debugging
   test failures much easier; instrument first.
2. **Swagger disabled in prod** — gate the swagger route behind `SWAGGER_ENABLED`
   env or a build tag; the generated `docs/` JSON is safe to commit.
3. **TestContainers and CI** — containers need Docker available in CI (GitHub
   Actions `ubuntu-latest` has Docker). Use `testcontainers/ryuk` for container
   cleanup (enabled by default).
4. **KEDA scale-to-zero** — `minReplicaCount: 0` means the consumer can be
   completely idle with no running pods. Make sure the `DispatchOutbox` prune
   ticker in `ingestion-api` doesn't depend on the consumer being alive.
5. **`/healthz` must exist before K8s** — the liveness/readiness probes in K8s
   depend on this endpoint. It's implemented in Phase 1.
6. **K8s secrets in git** — `secret.yaml` should contain **placeholder** base64
   values only. Real secrets are injected by CI/CD (e.g. GitHub Actions secrets,
   Vault, Sealed Secrets). Add a comment to that effect in the file.
7. **Coverage target is 75% on `./internal/...`** — the `cmd/` packages are
   mostly wiring and are excluded from the coverage measurement (they're covered
   indirectly by the E2E path tests).
