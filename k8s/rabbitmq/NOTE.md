# RabbitMQ in production

No RabbitMQ manifest is provided in this directory. Production deployments
should use the [RabbitMQ Cluster Operator](https://www.rabbitmq.com/kubernetes/operator/operator-overview.html)
to manage a highly-available, properly-monitored cluster (quorum queues,
TLS, upgrades, backups) instead of a hand-rolled Deployment/StatefulSet here.

Local/dev only uses the `rabbitmq:4.3-management` image via
`deployments/docker-compose.yml`. The KEDA `ScaledObject` in
`k8s/consumer-worker/scaledobject.yaml` talks to whatever RabbitMQ endpoint
is configured via `RABBITMQ_MANAGEMENT_URL`, regardless of how that
RabbitMQ instance is deployed.
