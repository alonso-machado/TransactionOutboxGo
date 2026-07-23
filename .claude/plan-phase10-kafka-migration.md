# Plan — Phase 10: RabbitMQ → Kafka (KRaft) migration

> **Not yet implemented.** Forward-looking plan.

> Builds on Phase 9 (`.claude/plan-phase9-backstage.md`), which must be
> fully implemented and verified working *before* this phase starts — see
> that doc's exit gate. This phase is expected to touch messaging internals
> only; `ingestion-api`'s and `tickets-api`'s HTTP/Swagger contracts (what
> Backstage's API entities catalog) shouldn't meaningfully change, so
> Phase 9's `catalog-info.yaml`/API entities/TechDocs content should need
> **no edits** here. Re-verifying that Phase 9's catalog still renders
> correctly after this migration is part of this phase's own verification.

## Context

Today: RabbitMQ (topic exchange `tickets.exchange` + DLX `tickets.dlx`,
one quorum queue per `(event_type, event_subtype)` shard per stream) is the
only broker, declared in `internal/infrastructure/rabbitmq/rabbitmq.go` and
driven through `internal/adapter/messaging/{publisher,consumer}.go`. This
phase replaces it end-to-end — local podman-compose, KIND/Helm, KEDA
autoscaling, integration tests — with Kafka running in KRaft mode (no
ZooKeeper). Decisions already made with the user, not re-litigated here:

- **Bitnami Kafka Helm chart** on KIND (KRaft mode), added as a Helm
  dependency — not Strimzi, not hand-written manifests.
- **`apache/kafka`** official image for local podman-compose (KRaft-native
  since 3.7+).
- **`segmentio/kafka-go`** as the Go client — pure Go, no cgo/librdkafka,
  keeps the existing cgo-free `golang:1.26-alpine` build working unchanged.
- KEDA consumer scaling moves from RabbitMQ queue-depth to Kafka
  consumer-group lag (`type: kafka` trigger).

## Part A — The existing seam is already broker-agnostic (reuse, don't redesign)

`internal/domain.Publisher` (`Publish`/`PublishBatch(ctx, msg(s)) error`)
and `internal/usecase/outbox.DispatchOutbox` have **zero RabbitMQ imports**
today — `dispatch.go` only calls the `Publisher` port. Same for
`internal/adapter/messaging`'s `MessageProcessor` interface, which both
consumer workers already depend on abstractly. This migration is fully
contained to:

- `internal/infrastructure/rabbitmq/` → new `internal/infrastructure/kafka/`
- `internal/adapter/messaging/{publisher,consumer}.go` internals
- `cmd/outbox-admin/main.go`
- `internal/infrastructure/config/config.go`
- `docker-compose.yml`, `helmcharts/`, `infra/kind/`
- `tests/integration/`

Nothing in `internal/usecase/` or `internal/domain/` needs to change, beyond
relocating the now-misplaced `EventTypes` registry (Part B).

## Part B — Naming translation (topics replace queues)

Preserve today's naming scheme as closely as possible so the mental model transfers:

| RabbitMQ (today) | Kafka (new) |
|---|---|
| Exchange `tickets.exchange` + routing key `order.concert.rock` | *(removed — no exchange concept)* |
| Queue `events.concert.rock.queue` | Topic `events.concert.rock` |
| DLQ `events.concert.rock.queue.dlq` | Topic `events.concert.rock.dlq` |
| Retry queue `...retry` (TTL-based redelivery) | Topic `events.concert.rock.retry` (see Part D — no native TTL-requeue in Kafka) |
| `x-retry-count` header | Kafka message header `retry-count` (same idea, carries over verbatim) |
| One queue = one consumer target | Consumer group `order-consumer-worker.events.concert.rock` (same one-per-shard-per-worker-type granularity KEDA already scales on) |

`EventTypes` (the `event_type → []event_subtype` registry) moves out of the
broker package into a broker-neutral home (e.g. `internal/domain` or a new
`internal/infrastructure/eventtypes`) since `internal/adapter/http` already
depends on it for request validation and it was never actually
RabbitMQ-specific.

## Part C — Partitioning: an actual upgrade, not just a lateral swap

Today, RabbitMQ ordering/throughput per shard is capped by one quorum queue.
Kafka topics support N partitions with per-key ordering. Use
`OutboxMessage.IdempotencyKey` (already a stable per-order/per-webhook-event
identity — `internal/domain/outbox.go`) as the Kafka message key:

- Per-order/per-event ordering is preserved (same key → same partition →
  in-order delivery).
- KEDA can scale consumers up to the partition count for real parallelism,
  vs. today's single-queue ceiling.
- Default 3 partitions per shard topic locally/in KIND (configurable via
  Helm values), replication factor 1 (single-broker KRaft, matching today's
  single-node RabbitMQ setup).

## Part D — Retry/DLQ semantics: highest-risk area, prototype first

Kafka has no native per-message TTL-requeue (RabbitMQ's retry-holding-queue
trick). Closest behavioral match to today's manual `x-retry-count` + backoff
logic in `internal/adapter/messaging/consumer.go`:

- On failure: produce to the shard's `.retry` topic with headers
  `retry-count` (incremented) and `retry-not-before` (unix ts, same backoff
  formula already in `RetryBackoffBase`/`RetryBackoffCap` config).
- The retry-topic consumer reads its own topic; if `retry-not-before` hasn't
  elapsed it holds/re-produces rather than processing immediately (retry
  volume is inherently low — this repo already treats retries as the
  low-volume path, so an in-process wait is acceptable).
- On `MaxDeliveries` exhaustion (or `domain.ErrUnknownSchemaVersion`, same
  as today): produce to the `.dlq` topic instead (produce-only, no
  consumer — same as today's DLQ queue).

**Prototype this narrow piece first**, in isolation, before touching
compose/Helm/tests — it's the one part of the migration with no direct
RabbitMQ equivalent; everything else below is a comparatively mechanical
swap.

## Part E — Code changes

- `internal/infrastructure/kafka/kafka.go` (replaces `rabbitmq.go`):
  `Connect`/dialer config, topic-naming helpers (`TopicFor`,
  `RetryTopicFor`, `DLQTopicFor`, `ConsumerGroupFor`), `DeclareTopics`
  (admin client, called once from `outbox-worker` startup — matching
  today's `DeclareTopology` vs. per-consumer `DeclareQueue` split).
- `internal/adapter/messaging/publisher.go`: `KafkaPublisher` using
  `kafka.Writer` (`segmentio/kafka-go`), `RequiredAcks: kafka.RequireAll` —
  preserves the hard invariant "never mark PUBLISHED before broker ack"
  (`PublishBatch` maps directly onto `writer.WriteMessages`).
- `internal/adapter/messaging/consumer.go`: `KafkaConsumer` using
  `kafka.Reader` with `GroupID` set, manual `CommitMessages` only after the
  DB transaction commits (preserves "ack after commit, never before") —
  same `MessageProcessor` interface, no usecase-layer changes.
- `cmd/outbox-admin/main.go`: replace RabbitMQ DLQ replay/drain with Kafka
  DLQ-topic consume-and-republish; same `--outbox`/`--stream`/
  `--event-type`/`--event-subtype` flag surface.
- `internal/infrastructure/config/config.go`:
  `RabbitMQURL`/`RabbitMQTLS`/`ConsumerQueue`/`PrefetchCount` →
  `KafkaBrokers`/`KafkaTLS`/`ConsumerTopic`/`ConsumerGroupID`;
  `MaxDeliveries`/`RetryBackoffBase`/`RetryBackoffCap` unchanged. Keep the
  existing "required but unused by some services" precedent for whichever
  services don't touch Kafka directly.
- `go.mod`: drop `github.com/rabbitmq/amqp091-go`, add
  `github.com/segmentio/kafka-go`.

## Part F — `docker-compose.yml`

- Replace the `rabbitmq` service with `kafka` (`apache/kafka`, KRaft env
  vars: `KAFKA_PROCESS_ROLES=broker,controller`, `KAFKA_NODE_ID`,
  `KAFKA_CONTROLLER_QUORUM_VOTERS`,
  `KAFKA_LISTENERS`/`KAFKA_ADVERTISED_LISTENERS` — no ZooKeeper service at
  all). Healthcheck via the image's broker-api-versions script.
- Every app service's `RABBITMQ_URL` env → `KAFKA_BROKERS`; keep the same
  `depends_on: condition: service_healthy` pattern for `outbox-worker` and
  the shard consumer services.

## Part G — Helm / KIND

- Add Bitnami Kafka as a Helm dependency in
  `helmcharts/transaction-outbox/Chart.yaml`, with a `values.yaml` `kafka:`
  section (`kraft.enabled: true`, single-broker sizing for KIND resource
  limits).
- `templates/order-consumer-worker/scaledobject.yaml` /
  `fulfillment-consumer-worker/scaledobject.yaml`: swap the `rabbitmq` KEDA
  trigger for `type: kafka` (`bootstrapServers`, `consumerGroup`, `topic`,
  `lagThreshold`, `offsetResetPolicy`) — still wrapped in the existing
  `{{- range .Values.eventShards }}` loop, one ScaledObject per shard per
  worker type, unchanged shape. `outbox-worker`'s `postgresql`-scaler
  ScaledObject is untouched (scales on outbox backlog, not broker state).
- Retire `infra/kind/postgres-rabbitmq.yaml` (a hand-written raw manifest)
  and `helmcharts/.../templates/kind-local/rabbitmq-nodeport.yaml` —
  consolidate on the new Helm-managed Kafka dependency instead of
  maintaining two deployment mechanisms (today's RabbitMQ setup is
  inconsistently split between a raw manifest in KIND and Helm-external
  values; this migration is a chance to fix that, not just replicate it).

## Part H — Tests

- `tests/integration/suite_test.go`: swap
  `testcontainers-go/modules/rabbitmq` for `testcontainers-go/modules/kafka`
  (KRaft-mode supported).
- AMQP-specific test helpers (`newOrderDispatchWithConn`, `amqpDial`,
  `newCheckoutConsumer`, `newFulfillmentConsumer`) and the raw-channel
  assertions in `routing_test.go` (`ch.Get()`) get Kafka equivalents
  (`kafka.Reader`/`kafka.Conn.ReadMessage` reading a specific
  topic/partition).
- No CI workflow YAML changes expected — RabbitMQ was never a CI service
  container (it's provisioned inside the Go test process via
  TestContainers), and that stays true for Kafka too.

## Part I — Docs pass (last step)

- `CLAUDE.md`: broad terminology sweep (queue→topic, exchange/DLX→removed,
  AMQP→Kafka) across the architecture description, "Key conventions", the
  "Where things live" table, and the `-consumer-worker` naming-convention
  rationale (the convention itself — services consuming from the broker get
  that suffix — still holds, just reword away from "RabbitMQ"
  specifically). Add this file's own link to the phase-doc list.
- `README.md`, `docs/runbook.md`,
  `helmcharts/transaction-outbox/README.md`,
  `.github/workflows/README.md`: same sweep wherever they reference
  RabbitMQ/queues/AMQP.

---

## Execution order

1. Part D's retry/DLQ prototype in isolation (a throwaway spike, not wired
   into the rest of the system) — validate the one piece with no direct
   RabbitMQ equivalent before touching anything else.
2. Part E (code) → Part F (local compose) → Part G (Helm/KIND) → Part H
   (tests) → Part I (docs), matching how Phase 7's pivot was sequenced.
3. After Parts E–H, re-verify Phase 9's Backstage catalog and API docs
   still render correctly, unchanged — that's the whole point of having
   done Phase 9 first.

## Verification

- `make lint` (zero issues) after every code change.
- `make build` / `make test` (unit) after Part E's code changes, before
  touching infra.
- `make up` (podman-compose) → confirm `ingestion-api`/`tickets-api` still
  accept and process orders end-to-end through Kafka in place of RabbitMQ
  (`make seed`).
- Integration suite (`go test -tags=integration ./tests/integration/...`)
  green after Part H's TestContainers swap — the strongest signal the
  retry/DLQ semantics (Part D) hold up under the existing concurrent-order
  test scenarios (e.g. the 50-concurrent-orders test).
- KIND: confirm KEDA scales an order-consumer-worker shard Deployment up
  under synthetic backlog on its Kafka topic (mirroring however the
  RabbitMQ queue-depth scaling was previously validated), and confirm
  Backstage's catalog page (Phase 9) still renders both API entities
  correctly post-migration.
