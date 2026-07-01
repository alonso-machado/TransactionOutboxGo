# Plan — Ticket relay + `ticket-consumer-worker` microservice + `tickets` database

> **Phase 6 — planned, not yet implemented.** This is the approved plan to pick
> up in a new session. Two design decisions already made with the user:
> (1) create all five `tickets` tables but only populate `tickets` + upsert
> `locations`/`events` from the order payload (`producers`/`event_areas` stay
> empty for now); (2) QR = a signed validation token **plus** a rendered PNG
> image (adds `github.com/skip2/go-qrcode`).

## Context

`POST /api/v1/ticket` already lands ticket-order events in the `ticket_outbox`
table (outbox DB), but nothing relays or processes them yet. This change closes
the loop, mirroring the payments pipeline:

1. **`outbox-worker`** starts relaying `ticket_outbox` rows (in addition to
   `outbox_messages`) to a **new RabbitMQ quorum queue**.
2. A **new `ticket-consumer-worker` microservice** consumes that queue and writes a
   **new `tickets` database** (third logical DB in the same Postgres instance,
   alongside `outbox` and `payments`).
3. The `tickets` DB gets five tables: `locations`, `events`, `producers`,
   `event_areas` (auxiliary child of `events`), and `tickets`. Per the design
   decisions: **all five tables are created**; the consumer fully populates
   `tickets` (+ a generated QR **PNG** and validation data) and upserts
   `locations`/`events` from the order's `venue`/`event_id`; `producers` and
   `event_areas` are created for a future data source but not populated by this
   flow. Missing columns are nullable.

The order payload only carries `event_id`, `venue{id,name,city}`,
`tickets[]{id,section,row,seat,price,currency}`, `payment_details`, and
`customer`, so the consumer maps what it has and leaves the rest null.

This reuses established patterns already in the repo: the payments two-DB split
(DB plumbing), `consumer-worker` (KEDA-scaled AMQP consumer), and
`DispatchOutbox` (the transactional relay).

---

## Groundwork already in place (done earlier — Phase 6's starting point)

The prior session already built the foundation this plan sits on. **These are
done and committed-pending; Phase 6 does NOT redo them** — it extends them:

- **`outbox-worker` is its own binary** ([cmd/outbox-worker/main.go](cmd/outbox-worker/main.go)) —
  the `DispatchOutbox` relay was split out of `ingestion-api`. Phase 6 adds a
  *second* dispatch loop (tickets) to this same process.
- **Two-database topology** (`outbox` + `payments`, one Postgres instance) with
  full plumbing already split: `observability/postgres/init-payments.sql`,
  `migrate-outbox`/`migrate-payments` compose one-shots, `Makefile` per-DB
  migrate targets, Helm two-URL secrets, KIND `migrate-job.yaml`, Pulumi
  `data.go`. Phase 6 adds a **third** DB (`tickets`) by copying this exact
  pattern.
- **`ingestion-api` runs at a fixed 1 replica** (`ingestionApi.hpa.enabled`
  gates an optional HPA) and **no longer connects to RabbitMQ**.
- **`POST /api/v1/ticket` + the `ticket_outbox` table already exist**
  ([internal/usecase/ticket/](internal/usecase/ticket/),
  [migrations/outbox/000002_ticket_outbox.up.sql](migrations/outbox/000002_ticket_outbox.up.sql)) —
  the endpoint stores orders with status `NEW`; **nothing relays them yet**.
  Phase 6 Part A is what adds the relay.
- **Three per-service CI workflows** exist (ingestion-api / outbox-worker /
  consumer-worker); Phase 6 adds a fourth for `ticket-consumer-worker`.

---

## Part A — Relay `ticket_outbox` from `outbox-worker`

### A1. Migration — extend `ticket_outbox` for the outbox state machine
Amend [migrations/outbox/000002_ticket_outbox.up.sql](migrations/outbox/000002_ticket_outbox.up.sql)
(fresh-DB only, safe to edit): add `retry_count integer DEFAULT 0` and
`last_error text` so ticket dispatch has the same NEW → PUBLISHED / RETRYING →
DEAD_LETTER story as payments (poison messages don't hot-loop). Add a partial
index on `status IN ('NEW','RETRYING')` for the poll.

### A2. Domain — extend the ticket port + add a ticket publisher port
In [internal/domain/ticket.go](internal/domain/ticket.go):
- Extend `TicketOutboxRepository` with `FetchPending`, `MarkPublished`,
  `MarkRetrying`, `MarkDeadLetter`, `CountPending` (mirror `OutboxRepository`,
  [internal/domain/outbox.go](internal/domain/outbox.go)).
- Add `TicketPublisher` port (`Publish` / `PublishBatch` over
  `*TicketOutboxMessage`).

### A3. Persistence — implement the new repo methods
Extend [internal/adapter/persistence/ticket_outbox_repo.go](internal/adapter/persistence/ticket_outbox_repo.go)
with the Fetch/Mark/Count methods, reusing the `FOR UPDATE SKIP LOCKED` +
batch-`MarkPublished` patterns from [outbox_repo.go](internal/adapter/persistence/outbox_repo.go).

### A4. RabbitMQ topology — a dedicated ticket queue
In [internal/infrastructure/rabbitmq/rabbitmq.go](internal/infrastructure/rabbitmq/rabbitmq.go):
add `TicketExchange`/`TicketQueue`/`TicketDLQ`/`TicketRoutingKey` constants and
`DeclareTicketTopology(ch)` — a durable topic (or direct) exchange + one
**quorum** queue with a DLQ, same shape as `declareMethodQueue` but single-queue
(no per-method fan-out).

### A5. Messaging — ticket publisher
New `internal/adapter/messaging/ticket_publisher.go`: `AMQPTicketPublisher`
implementing `domain.TicketPublisher`, its own confirm-enabled channel over the
shared connection (don't reuse the payment publisher's channel — keep the
confirm FIFO separate). Model on [publisher.go](internal/adapter/messaging/publisher.go).

### A6. Use-case — `DispatchTickets`
New `internal/usecase/ticketoutbox/dispatch.go`: a slimmed `DispatchOutbox`
(poll → `FetchPending` → `PublishBatch` → `MarkPublished`/`MarkRetrying`/
`MarkDeadLetter`), poll-only (no LISTEN/NOTIFY for tickets). Reuse the
`observability` metric helpers.

### A7. Wire into `outbox-worker`
[cmd/outbox-worker/main.go](cmd/outbox-worker/main.go): also call
`DeclareTicketTopology`, build the ticket repo + publisher + `DispatchTickets`,
and run it as a second goroutine alongside the payment dispatcher.

### A8. KEDA scaler counts both backlogs
[helmcharts/.../outbox-worker/scaledobject.yaml](helmcharts/transaction-outbox/templates/outbox-worker/scaledobject.yaml):
change the query to sum `outbox_messages` + `ticket_outbox` NEW/RETRYING rows.

---

## Part B — `ticket-consumer-worker` microservice + `tickets` database

### B1. `tickets` database plumbing (mirror the payments split)
- New migration set `migrations/tickets/000001_init.up.sql` (+ down) creating the
  five tables (see B4).
- `observability/postgres/init-payments.sql` → also create `tickets` (rename to
  `init-databases.sql` or add a sibling), and the KIND
  [migrate-job.yaml](infra/kind/migrate-job.yaml) + `createdb`.
- [docker-compose.yml](docker-compose.yml): `migrate-tickets` one-shot +
  `ticket-consumer-worker` service (`DATABASE_URL` → `tickets` DB via PgBouncer,
  `RABBITMQ_URL`, metrics port e.g. `9099`, `TICKET_SIGNING_SECRET`).
- [Makefile](Makefile): `MIGRATE_DSN_TICKETS` + a third migrate run + createdb guard.

### B2. New binary `cmd/ticket-consumer-worker/main.go`
Composition root: connect the `tickets` DB, connect RabbitMQ, declare ticket
topology, build the tickets repos + QR service + `ProcessTicketOrder`, and run a
ticket consumer (manual ack + prefetch; on success ack, on failure
nack→DLQ). Model on [cmd/consumer-worker/main.go](cmd/consumer-worker/main.go).

### B3. Domain + use-case (tickets side)
- Domain entities `Location`, `Event`, `EventArea`, `Producer`, `Ticket` + repo
  ports + a `TicketQR` service port, in `internal/domain/`.
- `internal/usecase/ticketconsume/process.go` — `ProcessTicketOrder`: parse the
  order JSON, upsert `Location` (from `venue`) and `Event` (keyed by
  `event_id`), then for each ticket in the array generate QR+validation and
  insert a `Ticket` row. Idempotent via unique constraints (see B4), so a
  redelivered order is a safe no-op (`ON CONFLICT DO NOTHING`).

### B4. Persistence (tickets DB) + schema
New `internal/adapter/persistence/tickets/` (or same package) GORM models +
repos + a tickets `UnitOfWork`. Table shapes:
- **locations** — `id uuid PK`, `name`, `street/number/city/country/zip_code`,
  `latitude/longitude double precision`, `additional`, `logo_url`,
  `photo_urls text[]`, `location_info jsonb`, `source_venue_id text UNIQUE`
  (dedup key from `venue.id`).
- **producers** — `id uuid PK`, `name`, `logo_url`, `website_url`.
- **events** — `id uuid PK`, `date_utc`, `starting_time`, `ending_time`,
  `event_type`, `event_subtype`, `name`, `description text`, `logo_url`,
  `location_id uuid FK`, `producer_id uuid FK`, `source_event_id text UNIQUE`
  (dedup key from order `event_id`).
- **event_areas** — `id uuid PK`, `event_id uuid FK`, `name`, `size`,
  `price bigint`, `currency` (auxiliary; not populated yet).
- **tickets** — `id uuid PK (v7)`, `event_id uuid FK`, `source_ticket_id text
  UNIQUE` (the `TKT-...` id → idempotency), `section/row/seat`, `price bigint`,
  `currency`, buyer fields from `customer` (`buyer_id/name/email`),
  `qr_png bytea`, `qr_content text`, `validation_code text`, `signature text`,
  `status text DEFAULT 'VALID'`.

### B5. QR + validation
New `internal/adapter/ticketqr/` implementing the `TicketQR` port: for each
ticket, generate `validation_code` (random UUID), `signature =
HMAC-SHA256(ticketID + ":" + validation_code, secret)`, `qr_content` (a compact
validation token/URL embedding those), and render a **PNG** via
`github.com/skip2/go-qrcode` (new go.mod dependency) returned as `[]byte`.
Secret from config `TICKET_SIGNING_SECRET` (dev default in compose, real value
via Helm secret in cloud).

### B6. Ticket consumer (messaging)
New `internal/adapter/messaging/ticket_consumer.go`: `AMQPTicketConsumer`
(prefetch, manual ack, DLQ-on-failure), calling `ProcessTicketOrder`. Simpler
than [consumer.go](internal/adapter/messaging/consumer.go) (single queue, no
per-method retry-TTL) — reuse its ack/nack + otel patterns.

### B7. Config
[internal/infrastructure/config/config.go](internal/infrastructure/config/config.go):
add `TicketSigningSecret` and `TicketQueue` (or reuse a constant). Keep one
`DATABASE_URL` per process (tickets DB for ticket-consumer-worker).

---

## Part C — Deploy, CI, tests, docs

- **Helm**: new `templates/ticket-consumer-worker/` (Deployment + KEDA `ScaledObject`
  with a **rabbitmq** trigger on the ticket queue, mirroring
  [consumer-worker/](helmcharts/transaction-outbox/templates/consumer-worker/));
  `image.ticketConsumerWorker`; `ticketConsumerWorker` values block; secret
  `ticketsDatabaseUrl` + `ticketSigningSecret`.
- **Pulumi** (review-only, never run): `tickets` DB URL + Secrets Manager entry
  in [data.go](infra/pulumi/data.go), `imageTagTicketConsumerWorker` in
  [config.go](infra/pulumi/config.go)/`Pulumi.*.yaml`, wiring in
  [workloads.go](infra/pulumi/workloads.go). RDS `tickets` DB is created
  out-of-band (same note as `payments`).
- **CI**: `.github/workflows/ticket-consumer-worker.yml` (mirror the other three).
- **Integration tests** ([tests/integration](tests/integration)): create the
  `tickets` DB in the suite container + apply its migration set; add a test that
  a ticket order flows outbox → relay → queue → consumer → `tickets` rows with a
  non-empty `qr_png` and a verifiable HMAC signature.
- **Docs**: CLAUDE.md (fourth service binary, `tickets` DB, ticket queue), README
  (components + the new flow), Helm README.

---

## Files at a glance

**Create:** `cmd/ticket-consumer-worker/main.go`; `internal/usecase/ticketoutbox/`,
`internal/usecase/ticketconsume/`; `internal/adapter/messaging/ticket_publisher.go`,
`ticket_consumer.go`; `internal/adapter/ticketqr/`;
`internal/adapter/persistence/tickets_*.go`; tickets domain entities;
`migrations/tickets/000001_init.*`; `helmcharts/.../templates/ticket-consumer-worker/*`;
`.github/workflows/ticket-consumer-worker.yml`; `tests/integration/ticket_flow_test.go`.

**Modify:** `migrations/outbox/000002_ticket_outbox.up.sql`,
`internal/domain/ticket.go`, `internal/adapter/persistence/ticket_outbox_repo.go`,
`internal/infrastructure/rabbitmq/rabbitmq.go`, `cmd/outbox-worker/main.go`,
`internal/infrastructure/config/config.go`, `docker-compose.yml`, `Makefile`,
`observability/postgres/init-*.sql`, `infra/kind/*`, Helm `values.yaml`/secrets,
`infra/pulumi/*`, `go.mod`/`go.sum`, docs.

**Reuse:** `DispatchOutbox` shape ([usecase/outbox/dispatch.go](internal/usecase/outbox/dispatch.go)),
publisher confirm pattern ([publisher.go](internal/adapter/messaging/publisher.go)),
consumer ack/DLQ pattern ([consumer.go](internal/adapter/messaging/consumer.go)),
the payments two-DB plumbing (compose/Makefile/Helm/migrations already split).

---

## Verification

1. `go build ./...`, `golangci-lint run ./...` (0 issues), `go test ./...`.
2. **Migrations**: fresh `make migrate` creates `tickets` with all five tables;
   `outbox` `ticket_outbox` has the new columns.
3. **End-to-end (`make up`)**: `POST /api/v1/ticket` → row in
   `outbox.ticket_outbox` (NEW) → `outbox-worker` flips it PUBLISHED and
   publishes to the ticket queue → `ticket-consumer-worker` writes `tickets.tickets`
   (N rows for N tickets) with a non-empty `qr_png`, plus `locations`/`events`
   rows; redelivery/duplicate order is a no-op.
4. **Integration test** asserts the full flow + that `HMAC(ticketID,
   validation_code)` matches the stored `signature` and `qr_png` decodes.
5. `helm lint` + `helm template` render the new `ticket-consumer-worker` Deployment +
   ScaledObject; KEDA query includes both outbox backlogs.

## Notes / decisions
- **All five tables created; `producers`/`event_areas` unpopulated** for now
  (no data source in the order payload) — per the chosen scope.
- **QR = signed token + PNG image** (adds `github.com/skip2/go-qrcode`).
- Ticket relay is **poll-only** (no LISTEN/NOTIFY) — simpler; low-latency wasn't
  requested for tickets.
- Third logical DB (`tickets`) in the same instance; PgBouncer's wildcard
  `[databases]` already pools it, each process keeps a single `DATABASE_URL`.
