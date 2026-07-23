# transaction-outbox Helm chart

Deploys the Transactional Outbox Event Ticket System:

- one `ingestion-api` Deployment + Service (fixed replica count by default;
  `ingestionApi.hpa.enabled: true` opts back into a CPU/memory HPA);
- one `outbox-worker` Deployment + KEDA `ScaledObject` (the Transactional
  Outbox relay — runs two dispatch loops, one per outbox table, and scales
  on the SUMMED backlog of both via the postgresql scaler,
  `outboxWorker.keda.minReplicaCount` defaults to 1 — see
  `templates/outbox-worker/`);
- one `order-consumer-worker` Deployment + KEDA `ScaledObject` pair and one
  `fulfillment-consumer-worker` Deployment + KEDA `ScaledObject` pair per
  `(event_type, event_subtype)` shard (driven by `values.yaml`'s
  `eventShards` list — see `templates/order-consumer-worker/`,
  `templates/fulfillment-consumer-worker/`); `fulfillment-consumer-worker`
  also sends the issued ticket's email synchronously, no separate consumer;
- one `tickets-api` Deployment + Service (fixed replica count, same shape as
  `ingestion-api` — no Rollout/canary variant yet, no Ingress yet either, see
  `templates/tickets-api/`);
- one `notification-retry-cron` Kubernetes `CronJob` (no RabbitMQ, no
  Deployment/ScaledObject) that retries any `ticket_notifications` row still
  missing `email_sent_timestamp` — see `templates/notification-retry-cron/`.

Two logical databases (one Postgres/RDS instance): ingestion-api and
outbox-worker use `secret.databaseUrl` (the `outbox` DB — `order_outbox` +
`payment_event_outbox`); order-consumer-worker, fulfillment-consumer-worker,
tickets-api, and notification-retry-cron use `secret.eventsDatabaseUrl` (the
`events` DB — locations/events/orders/tickets/charges/staff_users/
ticket_notifications).

## Install

This chart is the deploy/test path for the project — apply it to a
[KIND](https://kind.sigs.k8s.io/) cluster locally (`infra/kind/`, `make
k8s-apply`) or to any conformant Kubernetes cluster. There is no cloud
provisioning tool wired up in this repo right now (Pulumi was removed); point
`--set`/a values override file at your own cluster's ingress class, secrets
backend, and DB/broker endpoints.

```bash
helm upgrade --install transaction-outbox helmcharts/transaction-outbox \
  --namespace transaction-outbox --create-namespace
```

Override `secret.databaseUrl` / `secret.eventsDatabaseUrl` /
`secret.rabbitmqUrl` / `secret.stripeSecretKey` / `secret.stripeWebhookSecret`
/ `secret.ticketSigningSecret` (and any other value) via `--set` or a
`-f custom-values.yaml` file — never commit real connection strings or
secrets into `values.yaml`.

## Adding an event-type shard

Append an entry to `eventShards` in `values.yaml` (`eventType`,
`eventSubtype`, `name`, `orderQueue`, `paymentQueue`, `orderMetricsPort`,
`fulfillmentMetricsPort` — the metrics ports must not collide with an
existing entry). No new template files are needed — the Deployment/Rollout
and ScaledObject templates render one set per list entry. The
`(event_type, event_subtype)` pair must also exist in the application-side
registry (`internal/infrastructure/rabbitmq.EventTypes`) or the RabbitMQ
queue this shard's `orderQueue`/`paymentQueue` names won't exist to bind to.

## RabbitMQ in production

No RabbitMQ manifest is included in this chart. Production deployments
should use the [RabbitMQ Cluster Operator](https://www.rabbitmq.com/kubernetes/operator/operator-overview.html)
to manage a highly-available, properly-monitored cluster (quorum queues,
TLS, upgrades, backups) instead of a hand-rolled Deployment/StatefulSet here.

Local/dev only uses the `rabbitmq:4.3-management` image via
`docker-compose.yml` (or, for KIND, `infra/kind/postgres-rabbitmq.yaml`).
Each KEDA `ScaledObject` rendered by `templates/order-consumer-worker/
scaledobject.yaml`/`templates/fulfillment-consumer-worker/scaledobject.yaml`
talks to whatever RabbitMQ endpoint is configured via
`RABBITMQ_MANAGEMENT_URL` (in `values.yaml`'s `configMap.data`), regardless
of how that RabbitMQ instance is deployed.
