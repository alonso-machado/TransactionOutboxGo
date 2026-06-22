# CLAUDE.md — Guide for Claude Code

This file gives Claude Code context about the project so it can assist
effectively without re-deriving the same conclusions each session.

## What this project is

A Go monorepo implementing the **Transactional Outbox** pattern for a
Payments domain:

- **`ingestion-api`** — accepts `POST`/`PUT`/`PATCH` REST writes for
  `/api/v1/payments`, stores them durably in Postgres (outbox table only —
  it never writes to the `payments` table directly), pre-generates the
  Payment UUID and embeds the full payment data in the outbox payload,
  relays to RabbitMQ via a background goroutine, returns `201 Created`.
- **`consumer-worker`** — consumes from RabbitMQ and is the **only writer**
  of the `payments` table; dedupes via a `UNIQUE` constraint on the
  `payments.source_message_id` column (no separate inbox table).

Full design (Phase 1 — core system): [`.claude/plan.md`](.claude/plan.md)
Phase 2 plan (OTel · Swagger · TestContainers · K8s+KEDA): [`.claude/plan-phase2.md`](.claude/plan-phase2.md)
Phase 3 plan (per-method queues · card payments · CI/CD · Pulumi/AWS · k6): [`.claude/plan-phase3.md`](.claude/plan-phase3.md)
Phase 4 plan (leaky-bucket rate limiter · canary deploys · Grafana · TimescaleDB): [`.claude/plan-phase4.md`](.claude/plan-phase4.md)
Phase 5 plan (versioned migrations · retry backoff · DLQ replay · LISTEN/NOTIFY · connection pooling · Loki/Tempo/alerting · External Secrets · PCI/DR): [`.claude/plan-phase5.md`](.claude/plan-phase5.md)
User-facing docs: [`README.md`](README.md)

---

## Architecture: Clean Architecture

**Dependency rule:** outer layers depend on inner layers, **never the reverse**.

```
infrastructure  (Gin · GORM · amqp091 · Postgres · config · main / DI)
  └── adapter   (http handlers/DTOs · GORM repos · RabbitMQ pub/consumer)
        └── usecase   (IngestPayment · DispatchOutbox · ProcessMessage)
              └── domain   (entities + port interfaces) ← ZERO external imports
```

**Critical convention:** `internal/domain/` must **never** import Gin, GORM,
`amqp091-go`, or any other third-party framework. Violations break the
dependency rule. Frameworks are wired in at `cmd/*/main.go` via domain port
interfaces.

---

## Where things live

| What | Path |
|---|---|
| Entities (`OutboxMessage`, `Payment`) | `internal/domain/` |
| Port interfaces (`OutboxRepository`, `PaymentRepository`, `Publisher`, `UnitOfWork`) | `internal/domain/` |
| `IngestPayment` use-case | `internal/usecase/ingest/` |
| `DispatchOutbox` use-case — Transactional Outbox core (poll → dedup → publish → mark) | `internal/usecase/outbox/` |
| `ProcessMessage` use-case (parses payload, persists via `PaymentRepository`; dedup is the `payments.source_message_id` UNIQUE constraint) | `internal/usecase/consume/` |
| Gin router, handlers, DTOs, middleware | `internal/adapter/http/` |
| GORM DB models + repository implementations + UnitOfWork | `internal/adapter/persistence/` |
| RabbitMQ publisher + consumer implementations | `internal/adapter/messaging/` |
| `Config` struct (envconfig) | `internal/infrastructure/config/` |
| DB connection bootstrap + AutoMigrate | `internal/infrastructure/database/` |
| RabbitMQ connection + topology declaration | `internal/infrastructure/rabbitmq/` |
| Composition root / DI (ingestion-api) | `cmd/ingestion-api/main.go` |
| Composition root / DI (consumer-worker) | `cmd/consumer-worker/main.go` |
| Docker Compose (local dev) | `docker-compose.yml` |
| Multi-stage Dockerfile (ARG SERVICE) | `Dockerfile` |
| Helm chart (one Deployment/Rollout + ScaledObject per payment method, templated; `canary.enabled` switches Deployment+HPA ↔ Argo Rollout+AnalysisTemplate) | `helmcharts/transaction-outbox/` |
| Pulumi (AWS: EKS, RDS, Amazon MQ, ECR, KEDA, Argo Rollouts, AWS Load Balancer Controller — installs the Helm chart) | `infra/pulumi/` |
| Rate limiter (leaky-bucket IP throttle, ingestion-api only) | `internal/adapter/http/ratelimit/` |
| Prometheus/Grafana provisioning (dashboards, datasource, postgres-exporter queries) | `observability/` |
| GitHub Actions CI/CD (one workflow per microservice — see below) | `.github/workflows/` |
| PII redaction (`Redact`/`RedactJSON`, masks `cardNumber`/`payerDocument`/etc.) | `internal/domain/pii/` |
| Integration tests (TestContainers: Postgres + RabbitMQ) | `tests/integration/` |

---

## Key conventions

- **GORM structs** live only in `adapter/persistence`, not in `domain`. Domain
  entities are plain Go structs. Repositories map between them.
- **Port interfaces** are defined in `domain` and implemented in `adapter`.
  `usecase` depends on the interface type, never on the concrete adapter.
- **UnitOfWork** (`domain/uow.go`) abstracts DB transactions so `usecase` can
  compose multiple repo operations atomically without importing GORM.
- **Idempotency key** formula:
  `sha256(http_method + sha256(provider.name:eventId) + Idempotency-Key-header?)`
  — computed from the provider's own event identity (`provider.name` +
  `eventId`), never from the server-generated Payment UUID, or every request
  would be unique and dedup would never trigger. A webhook redelivery carries
  the same `eventId`, making it the natural dedup boundary. The
  `Idempotency-Key` header is only folded in when the client sends it. Same
  key travels as the outbox `UNIQUE` constraint and the RabbitMQ `MessageId`.
- **Payment wire format** mirrors a payment-provider webhook (e.g. Mercado
  Pago PIX): `{eventId, provider{name,providerPaymentId}, payment{paymentId,
  amount,currency,method,payerId?,recipientId?}, <method-lowercased>{...},
  occurredAt}`. The method-specific sub-object (e.g. `"pix"`) is a **top-level
  sibling key named after `payment.method` lowercased** — the handler
  extracts it generically via a raw `map[string]json.RawMessage`, so adding a
  new method (`CARD`, `BOLETO`, ...) never requires a DTO change. It's stored
  opaquely as `Payment.MethodDetails` (`[]byte` JSONB).
- **Amount conversion at the boundary:** the wire format's `amount` is a
  decimal float (currency units, e.g. `100.50`). `internal/adapter/http/handler.go`
  converts it to `int64` minor units (`round(amount * 100)`) immediately —
  domain and persistence code never see a float.
- **`payerId`/`recipientId` are optional**, nested under `payment` in the
  wire format, and stored as `*uuid.UUID` (nullable) in the `Payment` domain
  entity and `payments` table — **except for `TRANSFER`**, see below.
- **Five first-class methods, validated in `internal/adapter/http/dto.go`
  (`ValidateMethod`)**:
  - `PIX` — requires the `pix{endToEndId, txid}` sibling object.
  - `BOLETO` — requires the `boleto{barcode, dueDate, payerDocument}` sibling object.
  - `TRANSFER` — an **internally-originated** method (no external payment
    provider drives it); requires both `payment.payerId` and
    `payment.recipientId` to be present instead of a sibling object.
  - `CARTAO_CREDITO` / `CARTAO_DEBITO` — require the `cartao_credito`/
    `cartao_debito` sibling object (`CardDetailsDTO`: `cardNumber`, `cardType`,
    `cardIssuer`). `cardType` must match the method (`CREDIT`/`DEBIT`) and
    `cardIssuer` must be one of `VISA`/`MASTERCARD`/`AMERICAN`. The handler
    masks `cardNumber` to its last 4 digits (`internal/adapter/http/card.go`,
    `maskPAN`) **before** it ever reaches `ingest.Execute` — the full PAN is
    never persisted, published to RabbitMQ, or logged. `cardNumber` is also a
    key in `pii.Redact`'s sensitive-key set as a second layer of defense.
  - **Any method not in `rmq.Methods` is rejected with `400`** — Phase 3's
    per-method RabbitMQ queues mean an unrecognized method has no bound
    queue, so it would be a silent black hole if published (topic exchange,
    no matching binding, message dropped). Adding a 6th method is a 2-line
    change: add it to `rmq.Methods` (so a queue/DLQ gets declared) and,
    optionally, a new `case` in `ValidateMethod` for first-class validation
    — the polymorphic `MethodDetails` design means the schema itself never
    needs to change.
- **Outbox status state machine:** `NEW` → `PUBLISHED`, `NEW` → `RETRYING`,
  `RETRYING` → `PUBLISHED`, `RETRYING` → `DEAD_LETTER` (after max retries).
- **`DispatchOutbox`** (`usecase/outbox`) is the Transactional Outbox core: it runs
  as a goroutine started from `cmd/ingestion-api/main.go`, sharing the DB pool and
  RabbitMQ connection with the HTTP server. Use `context.Context` for graceful
  shutdown. Use `FOR UPDATE SKIP LOCKED` so multiple replicas never double-publish.
- **Publisher confirms** must be enabled on the `DispatchOutbox` AMQP channel. Never
  mark a row `PUBLISHED` before the confirm ACK arrives.
- **Manual ack + prefetch** on the consumer. Only call `msg.Ack()` after the DB
  transaction commits successfully.

---

## Module

```
github.com/alonsomachado/transaction-outbox-go
```

Go version: **1.26** (`go 1.26`, toolchain `go1.26.4`)

---

## Build / run commands

Go is **not installed on the host machine** — all Go tooling runs inside Podman
containers via `make` targets. Never run `go` directly on the host.

```bash
# Build, test, lint — all run inside golang:1.26-alpine via Podman
make build    # go build ./...
make test     # go test -race ./...
make tidy     # go mod tidy
make lint     # golangci-lint run ./...  (golangci/golangci-lint:latest image)

# Podman Compose — starts Postgres + RabbitMQ + both services
make up       # podman compose up --build -d
make logs     # tail logs from all services
make down     # podman compose down -v (removes volumes)
make seed     # curl a sample POST to the ingestion-api
```

## Linting rules

**Always run `make lint` after any code change.** The linter (`golangci-lint`
running inside Podman) must report zero issues before a change is considered
done. Key rules enforced:

- **`errcheck`** — every error return must be checked. For `Close()` calls
  inside `defer` where the error is unactionable, use `defer func() { _ = x.Close() }()`.
  For `Close()` calls in `main` before the server starts, log the error.
- All default golangci-lint linters apply — do not add `.golangci.yml` overrides
  to silence findings; fix the code instead.

---

## CI/CD (GitHub Actions)

**One workflow per microservice, not one shared pipeline** —
[`.github/workflows/ingestion-api.yml`](.github/workflows/ingestion-api.yml) and
[`consumer-worker.yml`](.github/workflows/consumer-worker.yml). Each is
triggered only by changes to its own `cmd/<service>/**` path (plus shared
`internal/**`/`go.mod`/`go.sum`/`Dockerfile`), so a change scoped to one
service never triggers, gates, or redeploys the other. Both follow the same
gate order:

```
build → lint (golangci-lint + actionlint + helm lint, GATE) → unit-tests (GATE) → upload (ECR) → deploy (pulumi up)
                                                                        └── integration-tests (optional, flag-gated, never blocks)
```

`lint` runs **three** checks: `golangci-lint` over the Go code, `actionlint`
over the workflow YAML itself (catches a broken pipeline the same way
golangci-lint catches broken Go), and `helm lint` over
`helmcharts/transaction-outbox` (catches a broken K8s manifest/values schema
before the Track 4 `deploy` job ever tries to install it). `integration-tests` (the
TestContainers suite) is a safety measure only — off by default, triggered
via `workflow_dispatch` or a `ci:integration` PR label, and never wired into
anything `upload`/`deploy` depends on. See
[`.github/workflows/README.md`](.github/workflows/README.md) for the full
rationale.

---

## What NOT to do

- Do **not** import framework packages (`gin`, `gorm.io`, `amqp091`) in
  `internal/domain/` or `internal/usecase/`.
- Do **not** put GORM struct tags on domain entities.
- Do **not** ACK a RabbitMQ message before the DB transaction commits.
- Do **not** mark an outbox row `PUBLISHED` before receiving a publisher confirm.
- Do **not** add a third binary — `DispatchOutbox` is a goroutine inside
  `ingestion-api`, not a separate process.
- Do **not** use `AutoMigrate` in tests — use a test transaction rollback or a
  dedicated test schema.
- Do **not** run `git commit` or `git push` in this repo, ever, even if asked
  in a way that sounds like a general go-ahead (e.g. "vou comitar e subir
  tudo"). The user commits and pushes everything themselves. Stage/diff/log
  read-only git commands are fine; leave the working tree's changes
  uncommitted and tell the user what's ready for them to commit.
