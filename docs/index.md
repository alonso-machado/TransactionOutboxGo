# Transaction Outbox — Event Ticket System

A reliable ticket-ordering pipeline that accepts REST writes
(`POST /api/v1/orders`), guarantees **no order loss**, charges each order
**exactly once** through a pluggable payment gateway, and issues signed,
QR-coded tickets once payment is confirmed — implemented with the
**Transactional Outbox** pattern over RabbitMQ and Postgres.

This page is TechDocs source, rendered inside Backstage for each Component
in [`catalog-info.yaml`](../catalog-info.yaml) via its
`backstage.io/techdocs-ref: dir:.` annotation. The full write-up (tech
stack table, CI/CD, deploy instructions, env var reference) lives in the
repo's [`README.md`](https://github.com/alonsomachado/transaction-outbox-go/blob/main/README.md) —
this page focuses on the architecture diagram a reader needs first.

## Services

| Service | Role |
|---|---|
| `ingestion-api` | Fixed 1 replica. HTTP write path: orders + payment-gateway webhooks. Writes only the two outbox tables. |
| `outbox-worker` | The Transactional Outbox relay (`DispatchOutbox`), two dispatch loops in one process, KEDA-scaled on summed backlog (min 1). |
| `order-consumer-worker` | One instance per `(event_type, event_subtype)` shard. Reserves tickets, opens a gateway checkout. |
| `fulfillment-consumer-worker` | One instance per shard. Marks Charge/Order `PAID`, issues tickets (QR + HMAC), emails the ticket synchronously. |
| `tickets-api` | Fixed 1 replica. Order-status polling, staff-authenticated check-in, ticket-holder name correction. Never touches RabbitMQ or the outbox database. |
| `notification-retry-cron` | Kubernetes CronJob (not a `-consumer-worker`). Retries any ticket whose confirmation email didn't send the first time. |
| `outbox-admin` | One-shot maintenance CLI: DLQ replay/drain. |

## Architecture

```mermaid
flowchart LR
    Client([Client])
    Gateway([Payment gateway<br/>Stripe / fake])

    subgraph API["ingestion-api (fixed 1 replica)"]
        H[Gin HTTP handler<br/>orders + webhooks]
    end

    subgraph OW["outbox-worker (KEDA, min 1)"]
        R1[DispatchOutbox: orders<br/>poll · LISTEN/NOTIFY · publish · mark]
        R2[DispatchOutbox: payment events<br/>poll · publish · mark]
    end

    subgraph DB[(PostgreSQL 18 — one instance)]
        subgraph DBO["outbox database"]
            OBX[[order_outbox]]
            PEO[[payment_event_outbox]]
        end
        subgraph DBE["events database"]
            ORD[[orders / tickets / charges]]
        end
    end

    subgraph MQ["RabbitMQ 4.3 (quorum, one queue + DLQ per shard, per stream)"]
        EX{{tickets.exchange<br/>topic}}
        QO[["events.&lt;type&gt;.&lt;subtype&gt;.queue"]]
        QP[["payments.&lt;type&gt;.&lt;subtype&gt;.queue"]]
        DLX{{tickets.dlx}}
        EX --> QO & QP
        QO -. "max deliveries" .-> DLX
        QP -. "max deliveries" .-> DLX
    end

    subgraph OCW["order-consumer-worker — one per shard"]
        PO[ProcessOrder:<br/>reserve tickets, open checkout]
    end
    subgraph FCW["fulfillment-consumer-worker — one per shard"]
        IT[IssueTickets:<br/>QR + HMAC on CONFIRMED]
    end

    Client -- "POST /api/v1/orders" --> H
    H -- "tx: INSERT (idempotency_key UNIQUE, status=NEW, event_type/subtype)" --> OBX
    H -- "201 Created" --> Client
    OBX -. "NOTIFY order_outbox_new" .-> R1
    R1 -- "SELECT ... FOR UPDATE SKIP LOCKED" --> OBX
    R1 -- "publish (confirm, routing key = order.&lt;type&gt;.&lt;subtype&gt;)" --> EX
    R1 -- "mark PUBLISHED" --> OBX
    QO --> PO
    PO -- "reserve RESERVED tickets + PENDING charge" --> ORD
    PO -- "CreateCheckout" --> Gateway
    Gateway -- "webhook: POST /api/v1/webhooks/payments/{provider}" --> H
    H -- "tx: INSERT" --> PEO
    PEO -. "poll" .-> R2
    R2 -- "publish (routing key = payment.&lt;type&gt;.&lt;subtype&gt;)" --> EX
    QP --> IT
    IT -- "mark PAID + issue VALID tickets (QR/HMAC)" --> ORD
```

> This diagram is the core dataflow — see the README's own copy for the
> cloud-deployment caveats (ALB/WAF, canary rollouts, observability) that
> don't fit on this simplified page.

## Two databases, one Postgres instance

`ingestion-api` and `outbox-worker` use the **`outbox`** database
(`order_outbox` + `payment_event_outbox`); `order-consumer-worker`,
`fulfillment-consumer-worker`, `tickets-api`, and `notification-retry-cron`
use the **`events`** database. No transaction spans the two — a hard
Postgres limitation, not a convention. `ingestion-api` never writes to the
`events` database, and symmetrically `tickets-api` never touches the
outbox tables or RabbitMQ.

## Where to go next

- API references: see this System's `ingestion-api` and `tickets-api` API
  entities in the catalog (backed by their generated `swagger.json`).
- [Operations Runbook](runbook.md) for outbox replay / DLQ drain procedures.
- Full README (tech stack, CI/CD, deploy, env vars):
  [`README.md`](https://github.com/alonsomachado/transaction-outbox-go/blob/main/README.md).
