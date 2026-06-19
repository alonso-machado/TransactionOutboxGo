# Plan: Transactional Outbox System (Go Monorepo)

## Context

Greenfield Go monorepo (currently only an initial commit + `.idea/`). Goal: an
ingestion pipeline where REST writes (POST/PUT/PATCH) are accepted, reliably
queued through RabbitMQ, and persisted by a worker — with **no message loss** and
**idempotent** processing.

The original brief had the Ingestion API publishing *directly* to RabbitMQ. That
is **not** the Transactional Outbox pattern and is lossy (if RabbitMQ is down when
the request arrives, the message is gone after the client gets a 2xx). Per the
decision, we implement the **full pattern**:

- **Outbox at the API** — the request is committed to Postgres in a transaction
  and ACKed to the client; the **DispatchOutbox** use case publishes to RabbitMQ.
- **Inbox at the Consumer** — dedupe table guarantees each logical message is
  applied exactly once.

This gives at-least-once delivery end-to-end + effectively-once persistence.

**Two binaries only** (per scope): the `DispatchOutbox` goroutine is **not** a
separate process — it runs inside the ingestion-api process. So the ingestion-api
binary both serves HTTP and dispatches the outbox to RabbitMQ. The consumer-worker
is the second binary. (`SKIP LOCKED` is still used so multiple ingestion-api
replicas don't double-publish.)

### Decisions locked in
- **Durability:** Outbox (API) + Inbox (Consumer).
- **Idempotency key:** `sha256(http_method + payload_hash + Idempotency-Key)` where
  the `Idempotency-Key` header is folded into the hash **only when the client sends it**.
  - No header → key = `sha256(method + payload)` → byte-identical requests are deduped.
  - With header → key includes it → two distinct requests carrying different keys never collide.
- **Stack:** framework-oriented — **Gin** (HTTP) + **GORM** (ORM).

### How the dedup key behaves
This combines content-hashing with optional client control: identical retries (no
header, same body) collapse to one message; genuinely distinct calls can be kept
separate by sending different `Idempotency-Key` values. The same key is carried as
the RabbitMQ `MessageId` and used by the consumer's inbox table, so dedup is
consistent end-to-end.

### Versions (verified June 2026)
- **Go 1.26.4** (latest stable, released 2026-06-02)
- **RabbitMQ 4.3.2** (`rabbitmq:4.3-management`) — use **quorum queues** (4.x default reliability path)
- **Postgres 17**

---

## Architecture

```
                       ┌──────────── ingestion-api (1 process) ────────────┐
Client ─POST/PUT/PATCH─▶ Gin HTTP handler                                    │
                       │   │ tx: INSERT outbox_messages (idempotency_key UNIQUE)
                       │   ▼                                                  │
                       │ Postgres ── outbox_messages (status=pending)        │
                       │   ▲              │ poll FOR UPDATE SKIP LOCKED       │
                       │   │              ▼                                   │
                       │   │  DispatchOutbox goroutine─publish(confirm)─┐      │
                       │   │  (dedup via SKIP LOCKED, mark published)  │      │
                       └───┼────────────────────────────────────── │ ────────┘
                           │                                        ▼
                           │                                    RabbitMQ (quorum queue + DLX)
                           │                                        │ consume (manual ack, prefetch)
                           │  tx: INSERT business row + inbox row   ▼
                           └──────────────────────────────── consumer-worker (process 2)
```

### Two binaries (single Go module, `/cmd`)
1. **ingestion-api** — one process that runs **both**:
   - Gin HTTP server: computes idempotency key, writes to outbox in a tx, returns `202 Accepted`.
   - **`DispatchOutbox` goroutine** (started in the same `main`, package `usecase/outbox`): the core of the Transactional Outbox pattern — polls pending outbox rows with `FOR UPDATE SKIP LOCKED` (dedup at source), publishes with **publisher confirms**, marks published; retry/backoff → `failed`; runs the prune ticker. Shares the same DB pool and RabbitMQ connection.
2. **consumer-worker** — RabbitMQ consumer. Manual ack + prefetch; dedupes via inbox table; persists business data + inbox row in one tx; routes poison messages to DLQ.

---

## Clean Architecture

Code is organized in concentric layers; the **dependency rule** points inward
only (outer layers depend on inner, never the reverse). Domain knows nothing
about Gin, GORM, or RabbitMQ — those are details injected at the composition root
(`cmd/*/main.go`) via interfaces (ports) defined in the domain.

```
        ┌──────────────────────── infrastructure (frameworks & drivers) ────────────────────────┐
        │  Gin · GORM · amqp091 · Postgres · config · main (composition root / DI)               │
        │   ┌──────────────────── adapter (interface adapters) ────────────────────┐             │
        │   │  http handlers/DTOs · gorm repositories · rabbitmq pub/consumer       │             │
        │   │   ┌──────────────── usecase (application rules) ────────────────┐     │             │
        │   │   │  IngestRecord · DispatchOutbox · ProcessMessage            │     │             │
        │   │   │   ┌──────────── domain (entities + ports) ───────────┐      │     │             │
        │   │   │   │  Entities: OutboxMessage, InboxMessage, Record    │      │     │             │
        │   │   │   │  Ports: OutboxRepo, InboxRepo, RecordRepo,        │      │     │             │
        │   │   │   │         Publisher, UnitOfWork                     │      │     │             │
        │   │   │   └──────────────────────────────────────────────────┘      │     │             │
        │   │   └─────────────────────────────────────────────────────────────┘     │             │
        │   └───────────────────────────────────────────────────────────────────────┘             │
        └─────────────────────────────────────────────────────────────────────────────────────────┘
```

Mapping of the pattern onto the layers:
- **domain** — `OutboxMessage`, `InboxMessage`, `Record` entities + repository/publisher **interfaces** (ports). Pure Go, no imports of frameworks.
- **usecase** — the three application flows: `IngestRecord` (hash → enqueue outbox), `DispatchOutbox` (the Transactional Outbox dispatcher: poll → dedup via SKIP LOCKED → publish → mark), `ProcessMessage` (inbox dedup → persist). Depend only on domain ports.
- **adapter** — concrete implementations: Gin controllers + DTOs, GORM repositories, RabbitMQ publisher/consumer. Implement the domain ports.
- **infrastructure** — config, DB/RabbitMQ connection bootstrap, and `main` wiring everything together (dependency injection).

## Proposed structure

```
TransactionOutboxGo/
├── .claude/
│   └── plan.md                      # this plan, copied verbatim into the repo
├── CLAUDE.md                        # guidance for Claude Code in this repo
├── README.md                        # tech summary, mermaid design, components, how to run
├── cmd/                             # composition roots (frameworks & drivers)
│   ├── ingestion-api/main.go        # wires HTTP server + DispatchOutbox goroutine
│   └── consumer-worker/main.go      # wires RabbitMQ consumer
├── internal/
│   ├── domain/                      # entities + ports (no external deps)
│   │   ├── outbox.go                # OutboxMessage + OutboxRepository, Publisher ports
│   │   ├── inbox.go                 # InboxMessage + InboxRepository port
│   │   ├── record.go                # Record + RecordRepository port
│   │   └── uow.go                   # UnitOfWork port (transaction boundary)
│   ├── usecase/
│   │   ├── ingest/                  # IngestRecord (idempotency hash + enqueue outbox)
│   │   ├── outbox/                  # DispatchOutbox — Transactional Outbox: poll + dedup (SKIP LOCKED) + publish + mark
│   │   └── consume/                 # ProcessMessage (inbox dedup + persist in one tx)
│   ├── adapter/
│   │   ├── http/                    # Gin router, handlers, DTOs, middleware
│   │   ├── persistence/             # GORM models + repository implementations + UoW
│   │   └── messaging/               # RabbitMQ publisher + consumer implementations
│   └── infrastructure/
│       ├── config/                  # env-bound config (envconfig)
│       ├── database/                # GORM connect + AutoMigrate + pool tuning
│       └── rabbitmq/                # connection/reconnect + topology declaration
├── deployments/
│   └── docker-compose.yml
├── build/
│   └── Dockerfile                   # multi-stage, ARG SERVICE to build each cmd
├── Makefile
├── go.mod / go.sum
└── .env.example
```

> Note on GORM + Clean Architecture: GORM structs live **only** in
> `adapter/persistence` (tagged DB models). The `domain` entities are plain
> structs; repositories map between the two so the inner layers never import GORM.

### Dependencies
- `github.com/gin-gonic/gin`
- `gorm.io/gorm`, `gorm.io/driver/postgres`
- `github.com/rabbitmq/amqp091-go`
- `github.com/google/uuid`
- `github.com/kelseyhightower/envconfig` (clean env binding; lighter than Viper)

---

## Data model (GORM AutoMigrate for local dev)

**outbox_messages**
- `id` UUID PK
- `idempotency_key` TEXT **UNIQUE NOT NULL** (content hash or client key)
- `aggregate_type` TEXT, `http_method` TEXT, `route` TEXT
- `payload` JSONB, `headers` JSONB
- `status` TEXT (`pending`|`published`|`failed`) — indexed
- `retry_count` INT, `last_error` TEXT
- `created_at`, `published_at`

**inbox_messages** (consumer dedup)
- `message_id` TEXT PK (= outbox `idempotency_key`)
- `status` TEXT, `processed_at`

**records** (sample business entity — generic payload store to demonstrate persistence)
- `id` UUID PK, `source_message_id` TEXT UNIQUE, `method` TEXT, `route` TEXT, `payload` JSONB, `created_at`

> Note: AutoMigrate is used for local-dev speed (fits the framework-oriented
> choice). Production path = `golang-migrate`/`goose` versioned SQL — listed as a
> hardening item, not built now.

---

## RabbitMQ topology (declared idempotently at startup)
- Durable **topic exchange** `outbox.exchange`
- Durable **quorum queue** `outbox.queue` bound by routing key `record.created`
- **Dead-letter**: `outbox.dlx` → `outbox.dlq` for poison messages (after N redeliveries)
- Messages published **persistent** (`DeliveryMode=2`), `MessageId` = idempotency key, **publisher confirms** enabled on the `DispatchOutbox` AMQP channel
- Consumer: `Qos(prefetch)` + **manual ack**

---

## Reliability mechanics (the parts that make it actually safe)
- **API / `IngestRecord`:** outbox insert uses `ON CONFLICT (idempotency_key) DO NOTHING` → duplicate requests are silently deduped at write time.
- **`DispatchOutbox` (the Transactional Outbox core):** `SELECT ... FOR UPDATE SKIP LOCKED` prevents double-publish when multiple ingestion-api replicas run; only marks `published` **after** a publisher-confirm ACK; exponential backoff + max-retries → `failed`; periodic prune ticker removes old `published` rows to keep the table bounded.
- **Consumer / `ProcessMessage`:** inbox check + business write + inbox insert in **one tx**; ack only after commit; redelivery-count → DLQ.
- **Resilience:** RabbitMQ auto-reconnect loop; graceful shutdown on SIGINT/SIGTERM via `context.Context`.

---

## Docker Compose (local dev)
- `postgres:17` — volume, healthcheck (`pg_isready`)
- `rabbitmq:4.3-management` — volume, healthcheck (`rabbitmq-diagnostics ping`), UI on :15672
- `ingestion-api` (:8080, includes the `DispatchOutbox` goroutine), `consumer-worker` — built from `build/Dockerfile` via `SERVICE` arg, `depends_on: condition: service_healthy`
- shared network, env via `.env`

---

## Build order
1. **Docs first (requested):** create `.claude/plan.md` (this plan, verbatim), `CLAUDE.md`, and `README.md`.
   - `CLAUDE.md`: architecture overview, Clean Architecture layer/dependency rule, where things live, conventions (domain has no framework imports), how to build/test/run.
   - `README.md`: tech summary table (Go 1.26, Gin, GORM, RabbitMQ 4.3 quorum, Postgres 17), **mermaid** diagram of the flow + component descriptions, and step-by-step "how to run docker compose".
2. Scaffold module + Clean Architecture folders, Makefile, `.env.example`, `.gitignore`.
3. `docker-compose.yml` with Postgres + RabbitMQ only; verify connectivity.
4. **domain** layer: entities (`OutboxMessage`, `InboxMessage`, `Record`) + ports (repos, `Publisher`, `UnitOfWork`).
5. **infrastructure**: config, GORM connect + AutoMigrate, RabbitMQ connect + topology declare.
6. **adapter/persistence**: GORM models + repository impls + UnitOfWork.
7. **adapter/messaging**: RabbitMQ publisher (confirms) + consumer (manual ack, prefetch).
8. **usecase**: `IngestRecord` (`usecase/ingest`), `DispatchOutbox` (`usecase/outbox` — the Transactional Outbox core), `ProcessMessage` (`usecase/consume`).
9. **adapter/http**: Gin router, handlers, DTOs, middleware; `POST/PUT/PATCH /api/v1/records`, `/healthz`.
10. **cmd/ingestion-api**: wire HTTP server + `DispatchOutbox` goroutine; graceful shutdown.
11. **cmd/consumer-worker**: wire consumer loop + DLQ handling; graceful shutdown.
12. `Dockerfile` (multi-stage) + wire both services into compose; Makefile targets (`up`, `down`, `logs`, `test`, `seed`).

---

## Verification (end-to-end)
1. `docker compose up --build` → all healthy.
2. `curl -X POST localhost:8080/api/v1/records -d '{...}'` → `202` with message id.
3. Inspect `outbox_messages` → row flips `pending`→`published`.
4. RabbitMQ UI (localhost:15672) → message flowed through `outbox.queue`.
5. Inspect `records` + `inbox_messages` → exactly one persisted row.
6. **Idempotency:** repeat the identical `curl` → still one `records` row (deduped at outbox via content hash; second consume short-circuits at inbox).
7. **Loss resistance:** `docker compose stop rabbitmq`, POST a request (still `202`, outbox `pending`), restart rabbitmq → `DispatchOutbox` goroutine drains the pending rows, consumer persists them.
8. **Poison handling:** force a processing error → message lands in `outbox.dlq` after retries, consumer keeps running.

---

## Things you may have missed (review checklist)
1. **Outbox vs direct-publish** — resolved: outbox at API (original flow was lossy).
2. **Content-hash over-dedup** — key = `sha256(method + payload + optional Idempotency-Key)`; clients can force-separate distinct calls via the header (see "How the dedup key behaves").
3. **Dead-letter queue** for poison messages — included (original brief had none).
4. **Publisher confirms** — without them `DispatchOutbox` can mark "published" for a message RabbitMQ never accepted. Included.
5. **Manual ack + prefetch** — auto-ack loses in-flight messages on a consumer crash. Included.
6. **Concurrent dispatch safety** (`SKIP LOCKED`) — multiple ingestion-api replicas can run `DispatchOutbox` safely without double-publishing. Included.
7. **Outbox table pruning** — unbounded growth otherwise. Included in `DispatchOutbox` as a prune ticker.
8. **Ordering** — outbox does not guarantee global ordering; per-key ordering only if needed. Noted, not enforced now.
9. **Production migrations** (golang-migrate), **auth/rate-limiting**, **metrics/tracing** (Prometheus/OTel), **payload size limits** — noted as hardening, out of scope for first build.
