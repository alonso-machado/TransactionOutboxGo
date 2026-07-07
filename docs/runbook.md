# Operational Runbook — Disaster Recovery, Replay & Rebuild

This runbook covers dead-letter/outbox replay and rebuilding the RabbitMQ
broker from scratch for the Event Ticket System (§1, §4, §5 — fully
up to date with the current `order_outbox`/`payment_event_outbox` shape and
`cmd/outbox-admin`'s current flags). §3/§3b (RDS point-in-time restore /
cross-region restore) describe the *procedure* a real deployment should
follow, but the RDS/KMS/AWS Backup infrastructure they assume was previously
provisioned by a Pulumi program that has since been **removed**
(`infra/pulumi/` is gone — Helm + KIND is the deploy/test path now, see
[README.md's Deploying section](../README.md#deploying)). Nothing in this
repo currently provisions PITR or cross-region backups; §3/§3b are kept as
reference for whatever re-provisions cloud infrastructure next, not a
currently-runnable procedure.

See [`SECURITY.md`](../SECURITY.md) for the security posture this runbook
supports.

---

## 1. Architectural DR property: the outbox is the replay log

Before reaching for any of the procedures below, remember the core property
the Transactional Outbox pattern buys for free: **every order and every
payment-gateway confirmation is durably written to an outbox table
(`order_outbox` or `payment_event_outbox`) inside the same DB transaction as
the business write, before it is ever published to RabbitMQ.** This means:

- A **RabbitMQ outage or total broker loss never loses an order or a
  payment confirmation** — every row that hasn't reached `PUBLISHED` is
  still sitting in Postgres as `NEW` or `RETRYING`. Once the broker is back
  (see §4), `outbox-worker`'s two `DispatchOutbox` loops resume polling and
  republish everything pending. No replay tooling is needed for this
  case — it's the steady-state behavior.
- The **only** data that can be permanently lost in a broker outage is a
  message that was already `PUBLISHED` but never consumed because the
  underlying queue/messages themselves were destroyed (e.g. the broker's
  storage volume is gone, not just unreachable). In that scenario, see §4.

This is the single most important fact for an on-call engineer: **don't
panic about a RabbitMQ outage — check both outbox tables' status counts
first**, on the `outbox` database:

```sql
SELECT status, count(*) FROM order_outbox GROUP BY status;
SELECT status, count(*) FROM payment_event_outbox GROUP BY status;
```

If `RABBITMQ_URL` is unreachable, `DispatchOutbox`'s publish attempts fail
and rows stay `NEW`/move to `RETRYING` with backoff — they are not lost,
and will drain automatically once connectivity is restored.

---

## 2. RPO / RTO

| | Target | Backed by |
|---|---|---|
| **RPO** (Recovery Point Objective) | ≤ 5 minutes (same-region PITR), ≤ 24h (cross-region) | RDS continuous PITR + AWS Backup's daily cross-region copy — **not currently provisioned** (see the note at the top of this document) |
| **RTO** (Recovery Time Objective) | ~30–60 minutes, once PITR/backup infrastructure exists | Time to provision a new RDS instance from a PITR/snapshot restore, repoint `DATABASE_URL`, and roll the app pods — see §3 below |

These targets describe what a real cloud deployment should aim for and are
**not currently backed by any provisioning in this repo** — re-establish
the underlying RDS PITR/AWS Backup setup (or an equivalent for whatever
Postgres hosting is actually used) before relying on these numbers.

---

## 3. Restore from RDS PITR (same-region)

> Reference procedure only — see the note at the top of this document.
> Assumes an RDS instance with `BackupRetentionPeriod` set for PITR, which
> nothing in this repo currently provisions.

Use when: the primary RDS instance is corrupted, had a bad migration
applied, or data needs to be recovered to a specific point in time.

1. **Identify the target restore time** (UTC). PITR supports any second
   within the configured `BackupRetentionPeriod` window.
2. **Restore to a new instance** (never in-place — RDS PITR always creates
   a new instance):
   ```bash
   aws rds restore-db-instance-to-point-in-time \
     --source-db-instance-identifier transaction-outbox-db \
     --target-db-instance-identifier transaction-outbox-db-restored \
     --restore-time 2026-06-22T10:00:00Z \
     --db-subnet-group-name <subnet group used by the current provisioning> \
     --vpc-security-group-ids <security group used by the current provisioning>
   ```
3. **Validate the restored instance** — connect with `psql` from a bastion
   or a temporary pod inside the cluster, spot-check `order_outbox`/
   `payment_event_outbox` (on the `outbox` database) and
   `orders`/`tickets`/`charges` (on the `events` database) row counts
   against expectations for the restore time.
4. **Cut over**: update the `DATABASE_URL` secret (Secrets Manager entry, if
   using ESO) to point at the restored instance's new endpoint for each
   affected service — remember `ingestion-api`/`outbox-worker` point at the
   `outbox` database and `order-consumer-worker`/`fulfillment-consumer-worker`
   point at the `events` database, so confirm which database(s) the restore
   actually affected before repointing everything blindly. If
   `externalSecrets.enabled` is on, ESO re-syncs the K8s `Secret` within its
   `refreshInterval` (5m, see `templates/externalsecret.yaml`) — restart the
   affected deployments to pick up the new `Secret` immediately rather than
   waiting for the next pod restart:
   ```bash
   kubectl rollout restart deployment -n transaction-outbox -l app.kubernetes.io/part-of=transaction-outbox
   ```
5. **Decommission the old instance** once the restored one is confirmed
   healthy and traffic has been cut over (don't delete immediately —
   keep it available as a fallback for at least one full on-call shift).

## 3b. Restore from cross-region AWS Backup copy

> Reference procedure only — see the note at the top of this document.

Use when: the entire primary region is unavailable (region-wide outage).

1. In the DR region, open the AWS Backup console/CLI and locate the vault
   holding the cross-region recovery point copies.
2. Restore the most recent recovery point to a new RDS instance in the DR
   region (`aws backup start-restore-job`).
3. Stand up the application stack in the DR region — a second cluster (or
   Helm release) pointed at the DR region's VPC/EKS cluster and the
   restored database is the expected shape.
4. Repoint DNS / the ALB target, and follow step 4 above to cut over
   `DATABASE_URL`.

This path accepts the larger RPO (up to 24h, a daily backup cadence) in
exchange for surviving a full regional outage.

---

## 4. Rebuilding the RabbitMQ broker from the outbox

Use when: the Amazon MQ broker (or local RabbitMQ container) is destroyed,
corrupted, or needs a topology reset, and in-flight messages already
`PUBLISHED` but not yet consumed are presumed lost along with it.

1. **Provision a new broker.** In cloud, however the broker is currently
   provisioned (Amazon MQ or a self-managed RabbitMQ on the cluster).
   Locally, `podman compose -f docker-compose.yml up -d rabbitmq` recreates
   the container (volumes are unnamed/ephemeral by design for local dev —
   see `docker-compose.yml`).
2. **Re-declare the topology.** `outbox-worker` (on startup, via
   `rmq.DeclareTopology`) and each `order-consumer-worker`/
   `fulfillment-consumer-worker` instance (via `rmq.DeclareQueue` for its own
   `CONSUMER_QUEUE`) idempotently declare the exchange/DLX/queues/retry-queues
   they need — simply restarting the pods/containers against the new broker
   recreates the full topology (`tickets.exchange`, every registered
   `(stream, event_type, event_subtype)` shard's queue + DLQ) with no manual
   `QueueDeclare` calls needed.
3. **Replay anything still pending in either outbox.** Because the outbox is
   authoritative (see §1), once `outbox-worker`'s next poll tick runs for
   each of the two dispatch loops, it picks up every `NEW`/`RETRYING` row in
   `order_outbox` and `payment_event_outbox` and republishes against the
   fresh broker — no action needed beyond making sure `outbox-worker` is
   running.
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
   -- on the outbox database, pick the affected table:
   UPDATE order_outbox
   SET status = 'NEW', next_retry_at = NULL
   WHERE status = 'PUBLISHED' AND published_at > '<broker-death-timestamp>';

   UPDATE payment_event_outbox
   SET status = 'NEW', next_retry_at = NULL
   WHERE status = 'PUBLISHED' AND published_at > '<broker-death-timestamp>';
   ```
   Confirm message uniqueness (the `UNIQUE` constraint backing dedup) is
   intact before doing this so re-publishing is safe — a consumer reading
   the same order/webhook twice is a no-op thanks to the
   `orders.source_order_id` UNIQUE constraint (order side) and the
   payment-gateway event id dedup (fulfillment side).

---

## 5. Dead-letter queue replay

Use when: messages have exhausted `MAX_DELIVERIES` and landed in a shard's
`.dlq`, or an outbox table itself has rows in `DEAD_LETTER` status after
exhausting `OUTBOX_MAX_RETRIES`.

### 5a. Replay outbox `DEAD_LETTER` rows

```bash
# Replay every dead-lettered row in order_outbox, event type CONCERT (limit 100 default)
make replay-dead OUTBOX=order EVENT_TYPE=CONCERT

# Replay payment_event_outbox instead, limit 500
make replay-dead OUTBOX=payment LIMIT=500
```

This resets matching rows from `DEAD_LETTER` back to `NEW`, which
`outbox-worker`'s next poll picks up for a fresh publish attempt. Use this
once the root cause of the original failures (bad broker connectivity, a
downstream consumer bug, etc.) has been fixed — replaying blindly into a
broken broker just re-exhausts the same retries.

### 5b. Drain a RabbitMQ DLQ back onto its live queue

```bash
# Move every message in events.concert.rock.dlq back onto events.concert.rock.queue,
# resetting the x-retry-count header so it gets a fresh set of attempts
make drain-dlq STREAM=order EVENT_TYPE=CONCERT EVENT_SUBTYPE=ROCK
```

Inspect the DLQ contents first (via the RabbitMQ management UI, or
`rabbitmqadmin get queue=events.concert.rock.dlq`) before draining — a DLQ
with poison messages (e.g. malformed payloads that will never successfully
process) should not be blindly drained, since it'll just dead-letter again
after `MAX_DELIVERIES`. Fix the root cause or discard truly unprocessable
messages first — see `make purge-loadtest-dlq` for the special case of
scrubbing only load-test-marked messages out of a DLQ that also holds real
ones.

---

## 6. Quick reference

| Symptom | First check | Procedure |
|---|---|---|
| RabbitMQ unreachable | `order_outbox`/`payment_event_outbox` status counts (§1) | Usually self-heals once broker is back; rebuild topology if broker was rebuilt (§4) |
| RDS corruption / bad migration | Backup/PITR availability (currently not provisioned — see the note at the top) | §3 (same-region) or §3b (cross-region), once such infrastructure exists |
| Outbox rows stuck `DEAD_LETTER` | Root cause of original publish failures | §5a (`make replay-dead`) |
| Messages stuck in a shard's `.dlq` | Inspect DLQ contents for poison messages | §5b (`make drain-dlq`) |
| Entire broker destroyed, in-flight `PUBLISHED` rows in doubt | Broker death timestamp vs. `published_at` | §4 (rebuild + manual reset if needed) |
