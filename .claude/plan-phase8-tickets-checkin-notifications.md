# Plan — Phase 8: order-status polling, tickets-api, staff-authenticated check-in, ticket-holder update, email delivery, govulncheck

> **Phase 8 — fully implemented (as of 2026-07-08).** This is the plan that
> was executed; kept as the historical record of the decisions made and the
> order they shipped in, not a forward-looking TODO list anymore. Two real
> bugs were found and fixed only by running the integration suite against
> real Postgres + RabbitMQ (not by code review or `go vet`) — see the
> "Implementation notes" section at the bottom for both war stories.

> Builds on Phase 7 (`.claude/plan-phase7-tickets-pivot.md`), which pivoted
> the system to an Event Ticket System and left the pipeline dead-ending
> after fulfillment. This closes four dead ends: the client never learns
> its checkout URL, nobody can check a ticket in at the door, a typo'd
> buyer name can't be fixed, and an issued ticket is never delivered — plus
> closes out Phase 7's Part F, which documented `govulncheck` but
> deliberately never wired it into CI.

## Context

Today: `ingestion-api` writes `order_outbox`/`payment_event_outbox` only;
`outbox-worker` relays both; `order-consumer-worker` reserves tickets +
opens a gateway checkout; `fulfillment-consumer-worker` marks the
Charge/Order `PAID` and issues tickets (QR PNG + HMAC). Nothing downstream
of `IssueTickets` exists yet. Requirements, confirmed with the user already
(not re-litigated here):

1. `GET /orders/{id}` — order status + checkout URL, polled by the client after `POST /orders`'s 201.
2. New service **`tickets-api`** (synchronous REST, reads/writes the `events` DB directly) hosting (1), check-in, and ticket-holder update. `ingestion-api` must keep never touching `events`/RabbitMQ — this is a new binary, not a route bolted onto it.
3. `POST /api/v1/checkin` — **requires a staff Bearer token** (Clerk-verified — see Part L), verify the QR's HMAC signature, surface buyer/section/row/seat to staff for a visual ID check, flip the ticket to a new `CHECKED_IN` status + `checked_in_at`. Idempotent.
4. A ticket-holder **name** update endpoint on `tickets-api` (age is no longer part of this — name only), rate-limited per source IP (429 on excess, no auth — confirmed with the user this one stays open, unlike check-in).
5. Email delivery: a **third outbox table** `ticket_notification_outbox`, written after `MarkIssued`'s transaction commits, relayed by the existing generalized `DispatchOutbox`, consumed by a new **`notification-consumer-worker`** via a new `domain.EmailSender` port (`fake`/`smtp` adapters, mirroring `PaymentGateway`).
6. **`govulncheck` actually wired into CI** on all six workflows (the four from Phase 7 plus the two new ones this phase adds) — Phase 7's Part F only documented this; it was never implemented in YAML.

Two new binaries: `tickets-api` (template: `cmd/ingestion-api/main.go`) and
`notification-consumer-worker` (template: `cmd/fulfillment-consumer-worker/main.go`).

Verified while researching this plan: the outbox/dispatch/consumer layers
are already fully table/stream-agnostic, and
`internal/adapter/ticketqr/ticketqr.go` already exports a working
`Verify(ticketID, validationCode, signature, secret string) bool` — check-in
needed zero new crypto code for the QR half. `internal/domain/pii/redact.go`'s
`sensitiveKeys` is exactly `{email, document, validationcode, signature}` —
buyer name isn't masked, and shouldn't be extended to mask it either (see
Part I).

---

## Part A — Domain layer (`internal/domain/`)

- **`ticket.go`**: `TicketStatusCheckedIn TicketStatus = "CHECKED_IN"` (a 4th enum value, matching the single-status-column convention `OrderStatus`/`ChargeStatus` already use). `CheckedInAt *time.Time` (nullable). **No age/DOB field** — dropped per a mid-session correction; only the buyer's name is editable. `TicketRepository` gained `FindByID`, `CheckIn(ctx, uow, id, checkedInAt) (alreadyCheckedIn bool, err error)`, `UpdateHolderName(ctx, uow, id, name) error`.
- **`order.go`**: `OrderRepository.FindByID(ctx, id) (*Order, error)`.
- **`charge.go`**: `ChargeRepository.FindByOrderID(ctx, orderID) (*Charge, error)` — `charges.order_id` is `uniqueIndex`, a single-row lookup.
- **`event.go`**: `EventRepository.FindByID(ctx, id) (*Event, error)` — needed to resolve a ticket's venue for check-in scoping.
- **`email.go`** (new): `EmailSender` port, mirroring `PaymentGateway`'s shape.
- **`staff.go`** (new): `StaffUser{ID, ClerkUserID, Name, Role, LocationID *uuid.UUID, CreatedAt}` + `StaffUserRepository{FindByClerkUserID}`.
- **`staffauth.go`** (new): `StaffAuthenticator{VerifyToken(token) (clerkUserID string, err error)}`.
- **`outbox.go`**: `AggregateType` gets a third bare-string convention value, `"ticket_notification"`.

## Part B — Use-cases

- **B1. `GET /orders/{id}`** — handler-thin (`internal/adapter/http/order_status_handler.go`), no new use-case package: a pure two-repo read, no transaction. `checkoutUrl: ""` until the Charge exists.
- **B2. `internal/usecase/checkin/checkin.go`** — thick. `Request{TicketID, ValidationCode, Signature, StaffLocationID *uuid.UUID}` → `FindByID` → `ticketqr.Verify` against the **stored** row → optional venue-scope check via `EventRepository.FindByID` → outcomes `CHECKED_IN` / `ALREADY_CHECKED_IN` / `INVALID_SIGNATURE` / `NOT_ISSUED` / `WRONG_VENUE`.
- **B3. `internal/usecase/ticketholder/update_holder.go`** — thin. `Request{TicketID, Name}`. Allows edits on `VALID`/`CHECKED_IN`, rejects `RESERVED`/`VOID` (409). No staff auth.
- **B4. `internal/usecase/notification/send_notification.go`** — implements `messaging.MessageProcessor`, zero repo dependency (fire-and-forget email). Always reports `created=true` — no dedup (documented scope cut).

## Part C — Migrations

- **`migrations/outbox/000002_add_ticket_notification_outbox.{up,down}.sql`** — structural mirror of `payment_event_outbox` (poll-only, no NOTIFY trigger). `idempotency_key` = the ticket's own ID.
- **`migrations/events/000002_add_checkin_holder_and_staff.{up,down}.sql`** — `tickets.checked_in_at` column + partial index; new `staff_users` table (`clerk_user_id` unique, `role`, nullable `location_id` FK to `locations`).

## Part D — RabbitMQ: the notification stream is deliberately *not* sharded

`NotificationStream = Stream{QueuePrefix: "notifications", RoutingPrefix: "notification"}`
bound to exactly **one** queue via a fixed sentinel pair
`NotificationSentinelType`/`NotificationSentinelSubtype = "_ALL"` — never
added to `rmq.EventTypes`. `StreamForAggregateType` gained a
`"ticket_notification"` case; `DeclareTopology` declares the sentinel queue
once, outside the `EventTypes`-driven double loop; `ParseQueueName` gained
a parallel special case (unused in practice —
`notification-consumer-worker` hardcodes the sentinel directly rather than
parsing a `CONSUMER_QUEUE` env var, since there's only ever one queue).

**Critical, easy-to-get-wrong detail** (see Implementation notes): the
sentinel must be stamped onto the *outbox row's* `EventType`/`EventSubtype`
columns (what `AMQPPublisher.fire` uses to compute the routing key), not
just referenced conceptually — the real event type/subtype only belong in
the JSON payload body.

## Part E — `outbox-worker`: third dispatch loop

Third `persistence.NewOutboxRepository(db, "ticket_notification_outbox", ...)`
+ third `outboxuc.New(...)`, run as a third goroutine with a **nil trigger**
(poll-only).

## Part F — `fulfillment-consumer-worker`: enqueue the notification (NOT atomically — see Implementation notes)

`IssueTickets.confirmAndIssue` (events-DB transaction) now returns the
tickets it just issued instead of enqueueing anything itself.
`IssueTickets.Execute` enqueues each one's notification **after** the
events-DB `uow.Execute` call returns successfully, using a **fresh, non-tx
context** (`nil` `uow`) — this write targets the *outbox* database, a
different one than the transaction that just committed, and Postgres has
no cross-database transactions. Best-effort: a failure here is logged, not
returned as an Execute error (the payment confirmation itself already
succeeded and must not be undone/redelivered over a notification failure).
A documented gap: a ticket can end up issued with no notification enqueued
if this second write fails in the narrow window right after the first
commits.

## Part G — HTTP layer: `tickets-api`

DTOs/handlers for order-status, check-in (`CheckinTicketDTO{BuyerName,
Section, Row, Seat}` shown to staff by design), ticket-holder (name-only).
`tickets_router.go`'s `NewTicketsRouter(...)`: `GET /api/v1/orders/:id` and
`POST /api/v1/checkin` unlimited (the latter gated by `staffauth.Middleware`
instead); `PATCH /api/v1/tickets/:id/holder` rate-limited, no auth.

## Part H — Two new binaries

`cmd/tickets-api/main.go` (template `cmd/ingestion-api/main.go`: events DB,
no RabbitMQ). `cmd/notification-consumer-worker/main.go` (template
`cmd/fulfillment-consumer-worker/main.go`: hardcoded sentinel, no DB
access, still needs an unused `DATABASE_URL` for `config.Config`'s shared
`required:"true"` tag). New adapters: `internal/adapter/emailsender/{fake,smtp}`
(`smtp` is real, stdlib `net/smtp` + `mime/multipart`, no new dependency).

## Part I — PII: ticket-holder update needs redaction on error paths only

`pii.Redact` on request/error paths, same as every handler. `sensitiveKeys`
is **not** extended to mask buyer name — both check-in's and
holder-update's success responses must show it in cleartext by design.

## Part J — Helm, CI, docker-compose

`templates/tickets-api/` (deployment/service/hpa — **no** Rollout/canary
variant, a documented scope cut; no Ingress either, since
`GET /api/v1/orders/{id}` would collide with `ingestion-api`'s existing
`POST /api/v1/orders` path on a path-only ALB rule). `templates/
notification-consumer-worker/` (deployment/scaledobject — single instance,
no `eventShards` loop). `values.yaml`/`secret.yaml`/`externalsecret.yaml`
gained the new images, `ticketsApi`/`notificationConsumerWorker` blocks,
`CLERK_SECRET_KEY`/`SMTP_USERNAME`/`SMTP_PASSWORD` secret keys. Two new CI
workflows (`tickets-api.yml`, `notification-consumer-worker.yml`), plus
`govulncheck` added to all four pre-existing ones (Part M).
`docker-compose.yml` gained both services, `tickets-api` defaulting
`STAFF_AUTH_PROVIDER=fake` (no real Clerk account needed for `make up`).

## Part K — Tests

Integration only (`tests/integration/`), matching this repo's existing
convention of testing use-cases via the TestContainers suite rather than
unit tests with fakes (confirmed: every other `usecase/*` package except
`usecase/outbox` has zero unit tests, only integration coverage).
`order_status_test.go`, `checkin_test.go` (full flow + missing-token/
unregistered-staff/wrong-venue/tampered-signature), `ticketholder_test.go`
(happy path + 409 + rate-limit 429), `notification_test.go` (asserts the
outbox row lands `NEW`, then the real dispatch→consume round trip against
a recording fake `EmailSender`). `suite_test.go` gained
`newTicketsRouter`/`newTicketsRouterRateLimited`, `newCheckinUC`/
`newUpdateHolderUC`, `seedStaffUser`/`seedLocation`, `recordingEmailSender`,
and `truncateAll` now also clears `ticket_notification_outbox`/
`staff_users` and purges the notification queue. All 35+ pre-existing
tests plus every new Phase 8 test pass with `-race`.

## Part L — Staff auth for check-in (Clerk integration)

Only `POST /api/v1/checkin` requires staff auth (confirmed: not
holder-update). Real verification via `github.com/clerk/clerk-sdk-go/v2`
(**new dependency**, a deliberate exception to this project's usual
dependency-minimalism instinct — the user explicitly asked for real Clerk
integration). `internal/adapter/staffauth/clerk/clerk.go` implements
`domain.StaffAuthenticator` via `jwt.Decode`/`jwt.GetJSONWebKey`/`jwt.Verify`
against Clerk's JWKS. `internal/adapter/staffauth/fake/fake.go` is the
local-dev/test double (one fixed token → one fixed Clerk user id).
`internal/adapter/http/staffauth/middleware.go`: 401 (missing/invalid
token) or 403 (valid token, not a registered `staff_users` row) before the
handler runs. Venue scoping: `nil` `location_id` on a `StaffUser` means
unscoped (any venue).

## Part M — `govulncheck` for real (closing Phase 7's Part F)

Added `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` to the `lint`
job in all six workflows. Found a real, reachable vulnerability on first
run (see Implementation notes) — fixed, not silenced.

---

## Files at a glance

**Create**: `cmd/tickets-api/main.go`; `cmd/notification-consumer-worker/main.go`; `internal/domain/{email,staff,staffauth}.go`; `internal/usecase/checkin/checkin.go`; `internal/usecase/ticketholder/update_holder.go`; `internal/usecase/notification/send_notification.go`; `internal/adapter/emailsender/{fake,smtp}/*.go`; `internal/adapter/staffauth/{clerk,fake}/*.go`; `internal/adapter/http/staffauth/middleware.go`; `internal/adapter/http/{order_status_handler,order_status_dto,checkin_handler,checkin_dto,ticketholder_handler,ticketholder_dto,tickets_router}.go`; `migrations/outbox/000002_*.sql`; `migrations/events/000002_*.sql`; Helm `templates/tickets-api/*`, `templates/notification-consumer-worker/*`; `.github/workflows/{tickets-api,notification-consumer-worker}.yml`; `tests/integration/{order_status,checkin,ticketholder,notification,phase8_helpers}_test.go`.

**Modify**: `internal/domain/{ticket,order,charge,event,outbox}.go`; `internal/adapter/persistence/{ticket_repo,order_repo,charge_repo,event_repo}.go` + new `staff_user_repo.go`; `internal/usecase/fulfillment/issue_tickets.go`; `internal/infrastructure/rabbitmq/rabbitmq.go`; `internal/infrastructure/config/config.go`; `cmd/outbox-worker/main.go`; `cmd/fulfillment-consumer-worker/main.go`; `helmcharts/transaction-outbox/values.yaml` + `templates/secret.yaml`/`externalsecret.yaml`; `docker-compose.yml`; `tests/integration/suite_test.go`; `go.mod`/`go.sum` (Clerk SDK + patched `go-jose/v3`); the four pre-existing `.github/workflows/*.yml` (govulncheck step).

**Reuse unchanged**: `internal/adapter/ticketqr/ticketqr.go` (`Verify` already existed); `internal/adapter/persistence/{models,outbox_repo,uow}.go`; `internal/usecase/outbox/dispatch.go`; `internal/adapter/messaging/{consumer,publisher}.go`; `internal/adapter/http/ratelimit/*`; `internal/domain/pii/redact.go`.

---

## Verification

1. `make build`, `make lint` (0 issues, incl. `--set canary.enabled=true` helm lint, `govulncheck`), `go vet -tags=integration ./...`.
2. Fresh `make migrate` applies both new migrations cleanly on top of the Phase 7 schema.
3. `go test -tags=integration -race -timeout=300s ./tests/integration/...` — all tests pass (existing suite + every new Phase 8 test).
4. `helm lint`/`helm template` render `tickets-api` and `notification-consumer-worker` for both `canary.enabled` values.
5. `govulncheck` passes (0 reachable vulnerabilities) on all six workflows, including the new Clerk SDK dependency.

---

## Implementation notes — two real bugs, found only by running the suite

Both of these compiled fine, passed `go vet`/`golangci-lint` with zero
issues, and looked correct on read-through. Neither was theoretical — both
were caught by actually running `tests/integration` against real Postgres
18 + RabbitMQ 4.3, which is exactly why this repo's convention is
integration tests over usecase-layer unit tests with fakes.

1. **Cross-database transaction contamination.** `fulfillment.IssueTickets`'s
   `confirmAndIssue` runs inside `uc.uow.Execute(...)`, where `uc.uow` wraps
   the **events** database. The first draft called
   `uc.notificationOutboxRepo.Enqueue(ctx, uc.uow, msg)` from inside that
   same closure. `GORMOutboxRepository.Enqueue`'s implementation calls
   `TxFromContext(ctx, r.db)`, which — because `ctx` already carried the
   events-DB transaction set by `uow.Execute` — returned that transaction
   instead of `r.db` (the **outbox** database `notificationOutboxRepo` was
   actually constructed against). The INSERT silently ran against the
   wrong database, failing with `relation "ticket_notification_outbox" does
   not exist` (that table doesn't exist in `events`). Root cause: Postgres
   has no cross-database transactions, so "the same transaction as
   MarkIssued" was never achievable for a write that has to land in a
   different logical database — the fix is enqueueing separately, after the
   events-DB transaction commits, with a transaction-free `ctx`.
2. **Topic-exchange black hole from the routing-key columns.**
   `AMQPPublisher.fire` computes the routing key as
   `rmq.RoutingKeyFor(stream, msg.EventType, msg.EventSubtype)` — directly
   from the outbox row's own columns, not from any separate stream
   metadata. The first draft stamped the ticket's **real**
   `EventType`/`EventSubtype` (e.g. `"CONCERT"`/`"ROCK"`) onto the
   notification outbox row "to keep it meaningful for reporting" — but
   `notification-consumer-worker`'s queue is bound only to routing key
   `notification._all._all`. Every enqueued notification silently vanished
   into the topic exchange with no matching binding — no error anywhere,
   since a topic exchange with no match for a routing key just drops the
   message. Fixed by stamping the sentinel (`"_ALL"`/`"_ALL"`) onto the
   `OutboxMessage.EventType`/`EventSubtype` columns instead, keeping the
   real values only in the JSON payload body (which the routing layer never
   reads).

A third, smaller correction during the same pass: the original test design
assumed `GET /orders/{id}` would return `200` with an empty `checkoutUrl`
immediately after `POST /orders`, before `order-consumer-worker` runs. In
fact `checkout.ProcessOrder` creates the `orders` row and the `Charge` in
one transaction — there is no `orders` row at all until that runs, so an
early poll legitimately `404`s. Not a bug; the test (and this doc, and
`CLAUDE.md`) were corrected to describe the real behavior instead.
