# CLAUDE.md — Guide for Claude Code

This file gives Claude Code context about the project so it can assist
effectively without re-deriving the same conclusions each session.

## What this project is

A Go monorepo implementing the **Transactional Outbox + Inbox** pattern:

- **`ingestion-api`** — accepts `POST`/`PUT`/`PATCH` REST writes, stores them
  durably in Postgres (outbox table), relays to RabbitMQ via a background
  goroutine, returns `202 Accepted`.
- **`consumer-worker`** — consumes from RabbitMQ, dedupes via an inbox table,
  persists the business entity exactly once.

Full design (Phase 1 — core system): [`.claude/plan.md`](.claude/plan.md)
Phase 2 plan (OTel · Swagger · TestContainers · K8s+KEDA): [`.claude/plan-phase2.md`](.claude/plan-phase2.md)
User-facing docs: [`README.md`](README.md)

---

## Architecture: Clean Architecture

**Dependency rule:** outer layers depend on inner layers, **never the reverse**.

```
infrastructure  (Gin · GORM · amqp091 · Postgres · config · main / DI)
  └── adapter   (http handlers/DTOs · GORM repos · RabbitMQ pub/consumer)
        └── usecase   (IngestRecord · DispatchOutbox · ProcessMessage)
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
| Entities (`OutboxMessage`, `InboxMessage`, `Record`) | `internal/domain/` |
| Port interfaces (`OutboxRepo`, `InboxRepo`, `RecordRepo`, `Publisher`, `UnitOfWork`) | `internal/domain/` |
| `IngestRecord` use-case | `internal/usecase/ingest/` |
| `DispatchOutbox` use-case — Transactional Outbox core (poll → dedup → publish → mark) | `internal/usecase/outbox/` |
| `ProcessMessage` use-case (inbox dedup + persist) | `internal/usecase/consume/` |
| Gin router, handlers, DTOs, middleware | `internal/adapter/http/` |
| GORM DB models + repository implementations + UnitOfWork | `internal/adapter/persistence/` |
| RabbitMQ publisher + consumer implementations | `internal/adapter/messaging/` |
| `Config` struct (envconfig) | `internal/infrastructure/config/` |
| DB connection bootstrap + AutoMigrate | `internal/infrastructure/database/` |
| RabbitMQ connection + topology declaration | `internal/infrastructure/rabbitmq/` |
| Composition root / DI (ingestion-api) | `cmd/ingestion-api/main.go` |
| Composition root / DI (consumer-worker) | `cmd/consumer-worker/main.go` |
| Docker Compose (local dev) | `deployments/docker-compose.yml` |
| Multi-stage Dockerfile (ARG SERVICE) | `build/Dockerfile` |

---

## Key conventions

- **GORM structs** live only in `adapter/persistence`, not in `domain`. Domain
  entities are plain Go structs. Repositories map between them.
- **Port interfaces** are defined in `domain` and implemented in `adapter`.
  `usecase` depends on the interface type, never on the concrete adapter.
- **UnitOfWork** (`domain/uow.go`) abstracts DB transactions so `usecase` can
  compose multiple repo operations atomically without importing GORM.
- **Idempotency key** formula:
  `sha256(http_method + sha256(payload) + Idempotency-Key-header?)` — the header
  is only included when the client sends it. Same key travels as the outbox
  `UNIQUE` constraint, the RabbitMQ `MessageId`, and the inbox primary key.
- **`DispatchOutbox`** (`usecase/outbox`) is the Transactional Outbox core: it runs
  as a goroutine started from `cmd/ingestion-api/main.go`, sharing the DB pool and
  RabbitMQ connection with the HTTP server. Use `context.Context` for graceful
  shutdown. Use `FOR UPDATE SKIP LOCKED` so multiple replicas never double-publish.
- **Publisher confirms** must be enabled on the `DispatchOutbox` AMQP channel. Never
  mark a row `published` before the confirm ACK arrives.
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

## What NOT to do

- Do **not** import framework packages (`gin`, `gorm.io`, `amqp091`) in
  `internal/domain/` or `internal/usecase/`.
- Do **not** put GORM struct tags on domain entities.
- Do **not** ACK a RabbitMQ message before the DB transaction commits.
- Do **not** mark an outbox row `published` before receiving a publisher confirm.
- Do **not** add a third binary — `DispatchOutbox` is a goroutine inside
  `ingestion-api`, not a separate process.
- Do **not** use `AutoMigrate` in tests — use a test transaction rollback or a
  dedicated test schema.
