# transaction-outbox Helm chart

Deploys the Transactional Outbox payments system:

- one `ingestion-api` Deployment + Service (fixed replica count by default;
  `ingestionApi.hpa.enabled: true` opts back into a CPU/memory HPA);
- one `outbox-worker` Deployment + KEDA `ScaledObject` (the Transactional
  Outbox relay — scales on outbox backlog via the postgresql scaler,
  `outboxWorker.keda.minReplicaCount` defaults to 1 — see
  `templates/outbox-worker/`);
- one `consumer-worker` Deployment + KEDA `ScaledObject` pair per payment
  method (driven by `values.yaml`'s `paymentMethods` list — see
  `templates/consumer-worker/`).

Two logical databases (one Postgres/RDS instance): ingestion-api and
outbox-worker use `secret.databaseUrl` (the `outbox` DB); consumer-worker uses
`secret.paymentsDatabaseUrl` (the `payments` DB).

## Install

```bash
helm upgrade --install transaction-outbox helmcharts/transaction-outbox \
  --namespace transaction-outbox --create-namespace
```

Override `secret.databaseUrl` / `secret.paymentsDatabaseUrl` /
`secret.rabbitmqUrl` (and any other value) via `--set` or a
`-f custom-values.yaml` file — never commit real connection strings into
`values.yaml`.

## Adding a payment method

Append an entry to `paymentMethods` in `values.yaml` (`name`, `queue`,
`metricsPort`). No new template files are needed — the Deployment and
ScaledObject templates render one pair per list entry.

## RabbitMQ in production

No RabbitMQ manifest is included in this chart. Production deployments
should use the [RabbitMQ Cluster Operator](https://www.rabbitmq.com/kubernetes/operator/operator-overview.html)
to manage a highly-available, properly-monitored cluster (quorum queues,
TLS, upgrades, backups) instead of a hand-rolled Deployment/StatefulSet here.

Local/dev only uses the `rabbitmq:4.3-management` image via
`docker-compose.yml`. Each KEDA `ScaledObject` rendered by
`templates/consumer-worker/scaledobject.yaml` talks to whatever RabbitMQ
endpoint is configured via `RABBITMQ_MANAGEMENT_URL` (in `values.yaml`'s
`configMap.data`), regardless of how that RabbitMQ instance is deployed.
