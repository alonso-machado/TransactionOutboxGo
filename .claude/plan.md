# Plan: Transactional Outbox System (Go Monorepo)

## Context

Greenfield Go monorepo implementing a **payments ingestion pipeline**: REST
writes (POST/PUT/PATCH) are accepted, reliably queued through RabbitMQ, and
persisted by a worker — with **no message loss** and **idempotent** processing.

A naive design has the Ingestion API publishing *directly* to RabbitMQ. That is
**not** the Transactional Outbox pattern and is lossy (if RabbitMQ is down when
the request arrives, the message is gone after the client gets a 2xx). Per the
decision, we implement the **Transactional Outbox** pattern, with a twist on
where the business row gets written:

- **Outbox at the API** — the request is committed to Postgres in a
  transaction (the **outbox table only**, never the `payments` table
  directly) and the client gets `201 Created`; the **DispatchOutbox** use
  case publishes to RabbitMQ.
- **Payments table owned by the Consumer** — `consumer-worker` is the
  **only** writer of the `payments` table. There is no separate inbox table;
  dedup happens via a `UNIQUE` constraint on `payments.source_message_id`, so
  a redelivered RabbitMQ message is a safe no-op insert (`ON CONFLICT DO
  NOTHING`).

This gives at-least-once delivery end-to-end + effectively-once persistence,
with a single source of truth per concern: the API never has to keep a
Payments row and an Outbox row consistent with each other (only one write
happens at request time), and the consumer's dedup doesn't need a second
table.

**Two binaries only** (per scope): the `DispatchOutbox` goroutine is **not** a
separate process — it runs inside the ingestion-api process. So the ingestion-api
binary both serves HTTP and dispatches the outbox to RabbitMQ. The consumer-worker
is the second binary. (`SKIP LOCKED` is still used so multiple ingestion-api
replicas don't double-publish.)

### Decisions locked in
- **Durability:** Outbox-only write at the API; `payments` table is written
  exclusively by the consumer.
- **Idempotency key:** `sha256(http_method + sha256(payerID:recipientID:amount:currency) + Idempotency-Key)`
  where the `Idempotency-Key` header is folded into the hash **only when the
  client sends it**, and the hash source **excludes** the server-generated
  Payment UUID (otherwise every request would be unique and dedup would never
  trigger).
  - No header → key = `sha256(method + business-fields-hash)` → byte-identical
    requests are deduped.
  - With header → key includes it → two distinct requests carrying different
    keys never collide.
- **Stack:** framework-oriented — **Gin** (HTTP) + **GORM** (ORM).
- **Outbox status state machine:** `NEW → PUBLISHED`, `NEW → RETRYING`,
  `RETRYING → PUBLISHED`, `RETRYING → DEAD_LETTER` (after `MAX_RETRIES`).
- **HTTP response:** `201 Created` (the outbox row is the unit of work the API
  commits; `202 Accepted` was considered but `201` better signals "the Payment
  resource — identified by the returned `paymentId` — now exists, even though
  it's not yet visible in the `payments` table").

### How the dedup key behaves
This combines content-hashing with optional client control: identical retries
(no header, same payer/recipient/amount/currency) collapse to one outbox row;
genuinely distinct calls can be kept separate by sending different
`Idempotency-Key` values. The same key is carried as the RabbitMQ `MessageId`.
The consumer's dedup is independent of this key — it relies on
`payments.source_message_id` being `UNIQUE`.

### Versions (verified June 2026)
- **Go 1.26.4** (latest stable, released 2026-06-02)
- **RabbitMQ 4.3.2** (`rabbitmq:4.3-management`) — use **quorum queues** (4.x default reliability path)
- **Postgres 17**

---

## Architecture

```
                       ┌──────────── ingestion-api (1 process) ────────────┐
Client ─POST/PUT/PATCH─▶ Gin HTTP handler                                    │
                       │   │ tx: INSERT outbox_messages (idempotency_key UNIQUE, status=NEW)
                       │   ▼                                                  │
                       │ Postgres ── outbox_messages (status=NEW)            │
                       │   ▲              │ poll FOR UPDATE SKIP LOCKED       │
                       │   │              ▼                                   │
                       │   │  DispatchOutbox goroutine─publish(confirm)─┐      │
                       │   │  (dedup via SKIP LOCKED, mark PUBLISHED)  │      │
                       └───┼────────────────────────────────────── │ ────────┘
                           │                                        ▼
                           │                                    RabbitMQ (quorum queue + DLX)
                           │                                        │ consume (manual ack, prefetch)
                           │  INSERT payments ON CONFLICT DO NOTHING ▼
                           └──────────────────────────────── consumer-worker (process 2)
```

### Two binaries (single Go module, `/cmd`)
1. **ingestion-api** — one process that runs **both**:
   - Gin HTTP server: pre-generates the Payment UUID, computes the idempotency
     key from the business fields, writes **only** to the outbox (status
     `NEW`) in a tx, returns `201 Created`.
   - **`DispatchOutbox` goroutine** (started in the same `main`, package
     `usecase/outbox`): the core of the Transactional Outbox pattern — polls
     `NEW`/`RETRYING` outbox rows with `FOR UPDATE SKIP LOCKED` (dedup at
     source), publishes with **publisher confirms**, marks `PUBLISHED`;
     retry/backoff → `RETRYING` → `DEAD_LETTER` after `MAX_RETRIES`; runs the
     prune ticker. Shares the same DB pool and RabbitMQ connection.
2. **consumer-worker** — RabbitMQ consumer. Manual ack + prefetch; the **only
   writer of `payments`**; dedupes via the `source_message_id` `UNIQUE`
   constraint (`ON CONFLICT DO NOTHING`); routes poison messages to the DLQ.

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
        │   │   │  IngestPayment · DispatchOutbox · ProcessMessage            │     │             │
        │   │   │   ┌──────────── domain (entities + ports) ───────────┐      │     │             │
        │   │   │   │  Entities: OutboxMessage, Payment                 │      │     │             │
        │   │   │   │  Ports: OutboxRepository, PaymentRepository,      │      │     │             │
        │   │   │   │         Publisher, UnitOfWork                     │      │     │             │
        │   │   │   └──────────────────────────────────────────────────┘      │     │             │
        │   │   └─────────────────────────────────────────────────────────────┘     │             │
        │   └───────────────────────────────────────────────────────────────────────┘             │
        └─────────────────────────────────────────────────────────────────────────────────────────┘
```

Mapping of the pattern onto the layers:
- **domain** — `OutboxMessage`, `Payment` entities + repository/publisher
  **interfaces** (ports). Pure Go, no imports of frameworks.
- **usecase** — the three application flows: `IngestPayment` (pre-generate
  Payment UUID → compute idempotency key → enqueue outbox), `DispatchOutbox`
  (the Transactional Outbox dispatcher: poll → dedup via SKIP LOCKED →
  publish → mark), `ProcessMessage` (parse payload → persist `Payment` via
  `ON CONFLICT DO NOTHING`).
- **adapter** — concrete implementations: Gin controllers + DTOs, GORM
  repositories, RabbitMQ publisher/consumer. Implement the domain ports.
- **infrastructure** — config, DB/RabbitMQ connection bootstrap, and `main`
  wiring everything together (dependency injection).

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
│   │   ├── payment.go               # Payment + PaymentRepository port
│   │   └── uow.go                   # UnitOfWork port (transaction boundary)
│   ├── usecase/
│   │   ├── ingest/                  # IngestPayment (idempotency hash + enqueue outbox)
│   │   ├── outbox/                  # DispatchOutbox — Transactional Outbox: poll + dedup (SKIP LOCKED) + publish + mark
│   │   └── consume/                 # ProcessMessage (parse payload + persist Payment, dedup via UNIQUE constraint)
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
- `id` UUID PK (= the pre-generated Payment UUID, so the outbox row and the
  eventual `payments` row share the same primary key)
- `idempotency_key` TEXT **UNIQUE NOT NULL** (content hash or client key)
- `aggregate_type` TEXT, `http_method` TEXT, `route` TEXT
- `payload` JSONB (the full Payment data: `paymentId`, `payerId`,
  `recipientId`, `amount`, `currency`), `headers` JSONB
- `status` TEXT (`NEW`|`PUBLISHED`|`RETRYING`|`DEAD_LETTER`) — indexed
- `retry_count` INT, `last_error` TEXT
- `created_at`, `published_at`

**payments** (the business entity — written only by `consumer-worker`)
- `id` UUID PK (matches the outbox row's `id`)
- `source_message_id` TEXT **UNIQUE NOT NULL** (= outbox `idempotency_key` /
  RabbitMQ `MessageId`) — the consumer's sole dedup mechanism
- `payer_id` UUID, `recipient_id` UUID
- `amount` BIGINT (minor units — never float), `currency` TEXT
- `created_at`, `updated_at`

> Note: AutoMigrate is used for local-dev speed (fits the framework-oriented
> choice). Production path = `golang-migrate`/`goose` versioned SQL — listed as a
> hardening item, not built now.

---

## RabbitMQ topology (declared idempotently at startup)
- Durable **topic exchange** `payments.exchange`
- Durable **quorum queue** `payments.queue` bound by routing key `payment.created`
- **Dead-letter**: `payments.dlx` → `payments.dlq` for poison messages (after N redeliveries)
- Messages published **persistent** (`DeliveryMode=2`), `MessageId` = idempotency key, **publisher confirms** enabled on the `DispatchOutbox` AMQP channel
- Consumer: `Qos(prefetch)` + **manual ack**

---

## Reliability mechanics (the parts that make it actually safe)
- **API / `IngestPayment`:** outbox insert uses `ON CONFLICT (idempotency_key) DO NOTHING` → duplicate requests are silently deduped at write time; the response reports `status: "duplicate"` when no row was created.
- **`DispatchOutbox` (the Transactional Outbox core):** `SELECT ... FOR UPDATE SKIP LOCKED` prevents double-publish when multiple ingestion-api replicas run; only marks `PUBLISHED` **after** a publisher-confirm ACK; exponential backoff + max-retries → `RETRYING` then `DEAD_LETTER`; periodic prune ticker removes old `PUBLISHED` rows to keep the table bounded.
- **Consumer / `ProcessMessage`:** parses the payload and calls `PaymentRepository.Save`, which uses `ON CONFLICT (source_message_id) DO NOTHING` — a redelivered message is a safe no-op; ack regardless of whether the insert happened.
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
   - `README.md`: tech summary table (Go 1.26, Gin, GORM, RabbitMQ 4.3 quorum, Postgres 17), **mermaid** diagram of the flow + component descriptions, and step-by-step "how to run via Podman compose".
2. Scaffold module + Clean Architecture folders, Makefile, `.env.example`, `.gitignore`.
3. `docker-compose.yml` with Postgres + RabbitMQ only; verify connectivity.
4. **domain** layer: entities (`OutboxMessage`, `Payment`) + ports (repos, `Publisher`, `UnitOfWork`).
5. **infrastructure**: config, GORM connect + AutoMigrate, RabbitMQ connect + topology declare.
6. **adapter/persistence**: GORM models + repository impls + UnitOfWork.
7. **adapter/messaging**: RabbitMQ publisher (confirms) + consumer (manual ack, prefetch).
8. **usecase**: `IngestPayment` (`usecase/ingest`), `DispatchOutbox` (`usecase/outbox` — the Transactional Outbox core), `ProcessMessage` (`usecase/consume`).
9. **adapter/http**: Gin router, handlers, DTOs, middleware; `POST/PUT/PATCH /api/v1/payments`, `/healthz`.
10. **cmd/ingestion-api**: wire HTTP server + `DispatchOutbox` goroutine; graceful shutdown.
11. **cmd/consumer-worker**: wire consumer loop + DLQ handling; graceful shutdown.
12. `Dockerfile` (multi-stage) + wire both services into compose; Makefile targets (`up`, `down`, `logs`, `test`, `seed`, `lint`) — all routed through Podman, no `go` on the host.

---

## Verification (end-to-end)
1. `podman compose -f deployments/docker-compose.yml up --build -d` → all healthy.
2. `curl -X POST localhost:8080/api/v1/payments -d '{...}'` → `201` with `paymentId`.
3. Inspect `outbox_messages` → row flips `NEW`→`PUBLISHED`.
4. RabbitMQ UI (localhost:15672) → message flowed through `payments.queue`.
5. Inspect `payments` → exactly one persisted row.
6. **Idempotency:** repeat the identical `curl` → still one `payments` row (deduped at outbox via content hash; second consume is a no-op `ON CONFLICT`).
7. **Loss resistance:** stop rabbitmq, POST a request (still `201`, outbox `NEW`), restart rabbitmq → `DispatchOutbox` goroutine drains the pending rows, consumer persists them.
8. **Poison handling:** force a processing error → message lands in `payments.dlq` after retries (`DEAD_LETTER` in the outbox), consumer keeps running.

---

## Things you may have missed (review checklist)
1. **Outbox vs direct-publish** — resolved: outbox at API (direct-publish was lossy).
2. **Content-hash over-dedup** — key = `sha256(method + business-fields-hash + optional Idempotency-Key)`, explicitly excluding the server-generated Payment UUID; clients can force-separate distinct calls via the header.
3. **Dead-letter queue** for poison messages — included.
4. **Publisher confirms** — without them `DispatchOutbox` can mark `PUBLISHED` for a message RabbitMQ never accepted. Included.
5. **Manual ack + prefetch** — auto-ack loses in-flight messages on a consumer crash. Included.
6. **Concurrent dispatch safety** (`SKIP LOCKED`) — multiple ingestion-api replicas can run `DispatchOutbox` safely without double-publishing. Included.
7. **Outbox table pruning** — unbounded growth otherwise. Included in `DispatchOutbox` as a prune ticker.
8. **Two-table consistency avoided by design** — the API never has to keep a `payments` row and an `outbox` row in sync with each other (it only ever writes the outbox); the consumer is the single writer of `payments`, sidestepping the dual-write problem an Inbox table would otherwise solve.
9. **Ordering** — outbox does not guarantee global ordering; per-key ordering only if needed. Noted, not enforced now.
10. **Production migrations** (golang-migrate), **auth/rate-limiting**, **metrics/tracing** (Prometheus/OTel), **payload size limits** — noted as hardening, out of scope for first build (see `.claude/plan-phase2.md`).
