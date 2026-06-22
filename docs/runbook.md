# Operational Runbook — Disaster Recovery, Replay & Rebuild

This runbook covers the recovery procedures for the Transaction Outbox
system: RDS point-in-time restore, dead-letter replay, and rebuilding the
RabbitMQ broker from scratch. It assumes the Phase 5 Track 5 infra
(`infra/pulumi/data.go`: KMS-encrypted RDS with automated backups + PITR
and a cross-region AWS Backup copy) is deployed. See [`SECURITY.md`](../SECURITY.md)
for the PCI-DSS posture this runbook supports.

---

## 1. Architectural DR property: the outbox is the replay log

Before reaching for any of the procedures below, remember the core property
the Transactional Outbox pattern buys for free: **every payment is durably
written to `outbox_messages` inside the same DB transaction as the
business write, before it is ever published to RabbitMQ.** This means:

- A **RabbitMQ outage or total broker loss never loses a payment** — every
  row that hasn't reached `PUBLISHED` is still sitting in Postgres as
  `NEW` or `RETRYING`. Once the broker is back (see §4), `DispatchOutbox`
  resumes polling and republishes everything that's pending. No replay
  tooling is needed for this case — it's the steady-state behavior.
- The **only** data that can be permanently lost in a broker outage is a
  message that was already `PUBLISHED` but never consumed because the
  underlying queue/messages themselves were destroyed (e.g. the broker's
  storage volume is gone, not just unreachable). In that scenario, see §4.

This is the single most important fact for an on-call engineer: **don't
panic about a RabbitMQ outage — check `outbox_messages` status counts
first.**

```sql
SELECT status, count(*) FROM outbox_messages GROUP BY status;
```

If `RabbitMQURL` is unreachable, `DispatchOutbox`'s publish attempts fail
and rows stay `NEW`/move to `RETRYING` with backoff — they are not lost,
and will drain automatically once connectivity is restored.

---

## 2. RPO / RTO

| | Target | Backed by |
|---|---|---|
| **RPO** (Recovery Point Objective) | ≤ 5 minutes (same-region PITR), ≤ 24h (cross-region) | RDS continuous PITR (`BackupRetentionPeriod: 7` days in `data.go` enables transaction-log-based restore to any second in that window); AWS Backup's daily cross-region copy (`newBackupPlan`, `cron(30 3 * * ? *)`) |
| **RTO** (Recovery Time Objective) | ~30–60 minutes | Time to provision a new RDS instance from a PITR/snapshot restore, repoint `DATABASE_URL`, and roll the app pods — see §3 below |

These targets assume the demo's traffic profile (Track 5.C is explicit that
this is not a production-traffic-shaped system) — re-tune
`BackupRetentionPeriod`/the cross-region copy schedule per real RPO/RTO
requirements before relying on this for an actual production deployment.

---

## 3. Restore from RDS PITR (same-region)

Use when: the primary RDS instance is corrupted, had a bad migration
applied, or data needs to be recovered to a specific point in time.

1. **Identify the target restore time** (UTC). PITR supports any second
   within the `BackupRetentionPeriod` window (7 days by default — see
   `infra/pulumi/data.go`).
2. **Restore to a new instance** (never in-place — RDS PITR always creates
   a new instance):
   ```bash
   aws rds restore-db-instance-to-point-in-time \
     --source-db-instance-identifier transaction-outbox-db \
     --target-db-instance-identifier transaction-outbox-db-restored \
     --restore-time 2026-06-22T10:00:00Z \
     --db-subnet-group-name <same subnet group as data.go's dbSubnetGroup> \
     --vpc-security-group-ids <same security group as data.go's dataSG>
   ```
3. **Validate the restored instance** — connect with `psql` from a bastion
   or a temporary pod inside the cluster, spot-check `outbox_messages` and
   `payments_*` row counts against expectations for the restore time.
4. **Cut over**: update the `DATABASE_URL` Secrets Manager entry
   (`transaction-outbox/<env>/database-url`, created in `data.go`) to point
   at the restored instance's new endpoint. If `externalSecrets.enabled`
   (Track 5.A) is on, ESO re-syncs the K8s `Secret` within its
   `refreshInterval` (5m, see `templates/externalsecret.yaml`) — restart
   the `ingestion-api`/`consumer-worker` deployments to pick up the new
   `Secret` immediately rather than waiting for the next pod restart:
   ```bash
   kubectl rollout restart deployment -n transaction-outbox -l app.kubernetes.io/part-of=transaction-outbox
   ```
5. **Decommission the old instance** once the restored one is confirmed
   healthy and traffic has been cut over (don't delete immediately —
   keep it available as a fallback for at least one full on-call shift).

## 3b. Restore from cross-region AWS Backup copy

Use when: the entire primary region is unavailable (region-wide outage).

1. In the DR region (`transaction-outbox:drRegion` in Pulumi config), open
   the AWS Backup console/CLI and locate the vault
   `transaction-outbox-db-dr` — recovery points are copied here daily by
   the `newBackupPlan` rule in `data.go`.
2. Restore the most recent recovery point to a new RDS instance in the DR
   region (`aws backup start-restore-job`).
3. Stand up the application stack in the DR region (a second, idle Pulumi
   stack pointed at the DR region's VPC/EKS cluster is the
   expected shape — not automated by the current Pulumi program, which
   targets a single region per stack).
4. Repoint DNS / the ALB target, and follow step 4 above to cut over
   `DATABASE_URL`.

This path accepts the larger RPO (up to 24h, the daily backup cadence) in
exchange for surviving a full regional outage.

---

## 4. Rebuilding the RabbitMQ broker from the outbox

Use when: the Amazon MQ broker (or local RabbitMQ container) is destroyed,
corrupted, or needs a topology reset, and in-flight messages already
`PUBLISHED` but not yet consumed are presumed lost along with it.

1. **Provision a new broker** — `pulumi up` re-creates the `mq.Broker`
   resource if it was deleted from state, or manually via the AWS console/
   CLI for an out-of-band rebuild. Locally, `podman compose up -d rabbitmq`
   recreates the container (volumes are unnamed/ephemeral by design for
   local dev — see `docker-compose.yml`).
2. **Re-declare the topology.** Both `ingestion-api` (on startup, via
   `rmq.DeclareTopology`) and each `consumer-worker` (via `rmq.DeclareQueue`)
   idempotently declare the exchange/DLX/queues/retry-queues they need —
   simply restarting the pods/containers against the new broker recreates
   the full topology with no manual `QueueDeclare` calls needed.
3. **Replay anything still pending in the outbox.** Because the outbox is
   authoritative (see §1), once `DispatchOutbox`'s next poll tick runs, it
   picks up every `NEW`/`RETRYING` row and republishes against the fresh
   broker — no action needed beyond restarting `ingestion-api` if it
   wasn't already running.
4. **Anything already `PUBLISHED` before the broker died is NOT
   automatically replayed** — by design, `DispatchOutbox` never
   re-publishes a row once it has a confirmed broker ACK, since that's
   the dedup boundary (`MessageId` = idempotency key) the rest of the
   pipeline relies on. If a genuine gap is suspected (broker died with
   unconsumed messages still queued), the operator can manually reset
   affected rows back to `NEW` via direct SQL — this is a deliberate,
   audited manual step, not an automated retry, precisely because
   `PUBLISHED` is meant to mean "the broker confirmed receipt":
   ```sql
   UPDATE outbox_messages
   SET status = 'NEW', next_retry_at = NULL
   WHERE status = 'PUBLISHED' AND published_at > '<broker-death-timestamp>';
   ```
   Confirm message uniqueness (the `UNIQUE` constraint backing dedup) is
   intact before doing this so re-publishing is safe — a consumer reading
   the same `source_message_id` twice is a no-op thanks to the
   `payments.source_message_id` UNIQUE constraint (Phase 1 design).

---

## 5. Dead-letter queue replay (Phase 5 Track 2.C)

Use when: messages have exhausted `MAX_DELIVERIES` and landed in a
method's `.dlq`, or the outbox itself has rows in `DEAD_LETTER` status
after exhausting `OUTBOX_MAX_RETRIES`.

### 5a. Replay outbox `DEAD_LETTER` rows

```bash
# Replay every dead-lettered outbox row across all methods (limit 100 default)
make replay-dead

# Replay only PIX, limit 500
make replay-dead METHOD=PIX LIMIT=500
```

This resets matching rows from `DEAD_LETTER` back to `NEW`, which
`DispatchOutbox`'s next poll picks up for a fresh publish attempt. Use
this once the root cause of the original failures (bad broker connectivity,
a downstream consumer bug, etc.) has been fixed — replaying blindly into a
broken broker just re-exhausts the same retries.

### 5b. Drain a RabbitMQ DLQ back onto its live queue

```bash
# Move every message in payments.pix.dlq back onto payments.pix.queue,
# resetting the x-retry-count header so it gets a fresh set of attempts
make drain-dlq METHOD=PIX
```

Inspect the DLQ contents first (via the RabbitMQ management UI, or
`rabbitmqadmin get queue=payments.pix.dlq`) before draining — a DLQ with
poison messages (e.g. malformed payloads that will never successfully
process) should not be blindly drained, since it'll just dead-letter again
after `MAX_DELIVERIES`. Fix the root cause or discard truly unprocessable
messages first.

---

## 6. Quick reference

| Symptom | First check | Procedure |
|---|---|---|
| RabbitMQ unreachable | `outbox_messages` status counts (§1) | Usually self-heals once broker is back; rebuild topology if broker was rebuilt (§4) |
| RDS corruption / bad migration | Backup/PITR window availability | §3 (same-region) or §3b (cross-region) |
| Outbox rows stuck `DEAD_LETTER` | Root cause of original publish failures | §5a (`make replay-dead`) |
| Messages stuck in a method's `.dlq` | Inspect DLQ contents for poison messages | §5b (`make drain-dlq`) |
| Entire broker destroyed, in-flight `PUBLISHED` rows in doubt | Broker death timestamp vs. `published_at` | §4 (rebuild + manual reset if needed) |
