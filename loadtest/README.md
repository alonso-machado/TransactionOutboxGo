# Load tests (Phase 3 Track 6)

## KIND environment setup (for 6.2, and 6.1/6.3 against a real cluster)

6.2 needs a real KEDA `ScaledObject`, which only Kubernetes provides — a
local [KIND](https://kind.sigs.k8s.io/) cluster is the cheapest way to get
one without touching AWS. Infra (Postgres/RabbitMQ) lives in the
`default` namespace; the app (ingestion-api/consumer-worker) is the Helm
release in its own `transaction-outbox` namespace — same split the chart
and `infra/kind/*.yaml` assume everywhere else in this repo.

```bash
# 1. Create the cluster (skip if `kind get clusters` already shows one).
#    infra/kind/kind-cluster-config.yaml maps host 15673 -> the RabbitMQ
#    management NodePort set up in step 6 below — recreate the cluster with
#    this config if you already have one without that port mapping.
kind create cluster --name kind-cluster --config infra/kind/kind-cluster-config.yaml

# 2. KEDA — the chart's ScaledObject CRDs need the KEDA operator installed
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace

# 3. metrics-server — ingestionApi.hpa (templates/ingestion-api/hpa.yaml)
#    targets CPU/memory utilization, which is a no-op without this: `kubectl
#    top` and the HPA's metrics both come from here, and KIND doesn't ship
#    it by default. --kubelet-insecure-tls is required because KIND's
#    kubelet serving certs aren't signed for metrics-server's normal
#    verification path — fine for a local throwaway cluster, never do this
#    on a real one. Same pattern as KEDA above: cluster-level infra,
#    installed once, NOT part of the transaction-outbox chart, namespaced
#    to "default" here (its upstream manifest defaults to kube-system) to
#    match this repo's KIND-infra convention.
curl -sL -o /tmp/metrics-server.yaml \
  https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
sed -i 's/namespace: kube-system/namespace: default/g' /tmp/metrics-server.yaml
sed -i '/--kubelet-use-node-status-port/a\        - --kubelet-insecure-tls' /tmp/metrics-server.yaml
# The auth-reader RoleBinding must stay in kube-system — it binds to the
# "extension-apiserver-authentication-reader" Role, which only exists
# there (RoleBindings can't reference a Role outside their own namespace).
# Its ServiceAccount *subject* staying "default" is fine — that's a
# cross-namespace reference, which RoleBindings do support.
sed -i '/name: metrics-server-auth-reader/{n;s/namespace: default/namespace: kube-system/}' /tmp/metrics-server.yaml
kubectl apply -f /tmp/metrics-server.yaml
kubectl wait --for=condition=available deployment/metrics-server -n default --timeout=60s

# 4. Infra (Postgres + RabbitMQ) — plain Deployments in "default", not the
#    chart's normal RDS/Amazon MQ targets (see infra/kind/postgres-rabbitmq.yaml)
kubectl apply -f infra/kind/postgres-rabbitmq.yaml
kubectl wait --for=condition=ready pod -l app=postgres --timeout=60s
kubectl wait --for=condition=ready pod -l app=rabbitmq --timeout=60s

# 5. Migrations — one-shot Job sourced from ConfigMaps (KIND has no host-path
#    mount for this cluster, unlike compose's volume mount). Two-DB split: one
#    ConfigMap per migration set (the sets have overlapping version numbers and
#    golang-migrate tracks schema_migrations per database, so they can't share
#    a path). The Job also CREATE-DATABASEs `payments` first — see
#    infra/kind/migrate-job.yaml.
kubectl create configmap migrations-outbox   --from-file=migrations/outbox/
kubectl create configmap migrations-payments --from-file=migrations/payments/
kubectl apply -f infra/kind/migrate-job.yaml
kubectl wait --for=condition=complete job/migrate --timeout=60s

# 6. Build the app images and load them straight into KIND's node (no
#    registry push needed — `:kindtest` matches infra/kind/values-kind.yaml)
podman build --build-arg SERVICE=ingestion-api   -t localhost/transaction-outbox-go/ingestion-api:kindtest   .
podman build --build-arg SERVICE=outbox-worker   -t localhost/transaction-outbox-go/outbox-worker:kindtest   .
podman build --build-arg SERVICE=consumer-worker -t localhost/transaction-outbox-go/consumer-worker:kindtest .
podman save localhost/transaction-outbox-go/ingestion-api:kindtest   -o /tmp/ingestion-api.tar
podman save localhost/transaction-outbox-go/outbox-worker:kindtest   -o /tmp/outbox-worker.tar
podman save localhost/transaction-outbox-go/consumer-worker:kindtest -o /tmp/consumer-worker.tar
kind load image-archive /tmp/ingestion-api.tar   --name kind-cluster
kind load image-archive /tmp/outbox-worker.tar   --name kind-cluster
kind load image-archive /tmp/consumer-worker.tar --name kind-cluster

# 7. Install the chart with the KIND overrides (KEDA-driven autoscaling,
#    in-cluster DB/MQ endpoints, rate limiter off, kindLocal.enabled=true
#    which renders templates/kind-local/rabbitmq-nodeport.yaml — see
#    infra/kind/values-kind.yaml's comments)
helm upgrade --install transaction-outbox helmcharts/transaction-outbox \
  --namespace transaction-outbox --create-namespace \
  -f infra/kind/values-kind.yaml
```

Re-run step 6 + `helm upgrade --install ...` (step 7) whenever app code
changes — `kind load image-archive` replaces the node's cached image, but
existing pods keep running the old one until rolled, so also run:

```bash
kubectl rollout restart deployment -n transaction-outbox
```

### Reaching things from the host

RabbitMQ's management UI/API is reachable straight at `localhost:15673` —
no `port-forward` needed — once steps 1 and 6 above are both in place:
`kind-cluster-config.yaml`'s `extraPortMappings` map host 15673 to the
control-plane node's 30673, and `kindLocal.enabled: true` in
`values-kind.yaml` makes the chart render a NodePort Service
(`templates/kind-local/rabbitmq-nodeport.yaml`) publishing RabbitMQ's
15672 on that same 30673. `loadtest-consumer`'s
`curl -u guest:guest http://localhost:15672/...` examples below become
`http://localhost:15673/...` against the KIND cluster's RabbitMQ instead of
compose's (which keeps the plain 15672).

`ingestion-api` gets the same treatment for 6.1/6.2's `TARGET_URL` — `k6`
runs on the Windows host, so a fixed NodePort beats keeping a
`kubectl port-forward` alive in another terminal for the whole test run.
`kindLocal.enabled: true` forces `templates/ingestion-api/service.yaml` to
`type: NodePort` on a fixed `nodePort` (`kindLocal.ingestionApiNodePort`,
default `30080`), and `kind-cluster-config.yaml` maps that to host `8081`
(not `8080`, so it doesn't clash with compose's own host-published
ingestion-api):

```bash
make loadtest-autoscale METHOD=PIX TARGET_URL=http://localhost:8081
```

Only Postgres has no host-reachable port — nothing in this load-test suite
needs to reach it directly; `kubectl port-forward -n default svc/postgres
5432:5432` works the same way as RabbitMQ's did before it got a NodePort, if
you ever need to.

Three k6 scripts, run separately with **opposite consumer setups** — don't
mix them up:

| Script | Drives | Consumer capacity | Measures |
|---|---|---|---|
| `k6-baseline.js` | HTTP `POST /payments` | **pinned at 1 pod/queue** (KEDA disabled/paused) | P95/P99 latency at a fixed, known capacity |
| `k6-autoscale.js` | HTTP `POST /payments` | **KEDA active** (`min 0 / max 10`, target 1000 msgs) | autoscaling: scale-up past 1000 backlog, then scale-down to 0 |
| `k6-consumer.js` | AMQP publish straight onto a queue (bypasses ingestion-api) | pinned 1 pod for a clean number, or unpinned | queue backlog/drain rate (RabbitMQ side) + consumer-worker's outcome breakdown (`ack`/`duplicate`/`unknown_schema_version`/...) via its `/metrics`, **DB write included** but checked separately, not by k6 |

## 6.1 — Latency baseline

```bash
make loadtest-up
make loadtest TARGET_URL=http://localhost:8080
make loadtest-report TARGET_URL=http://localhost:8080   # + summary-baseline.json
```

Two 5-minute phases at a fixed 100 virtual users: Phase A round-robins all 5
methods, Phase B hits PIX only. **Pin every consumer to 1 replica first** —
both compose files already run a single instance per consumer (leave it);
on Kubernetes, set the KEDA `ScaledObject`'s `minReplicaCount`/
`maxReplicaCount` to 1, or annotate it `autoscaling.keda.sh/paused-replicas: "1"`.

Reports the full k6 default summary (P95/P99, RPS, failure rate, the works) —
every request is tagged `{ method: ... }` so the summary/JSON export can be
sliced per method to find the bottleneck. Watch `dropped_iterations`: if
non-zero, the load generator didn't sustain the target rate.

### Results (local compose, 2026-06-23)

`make loadtest-up` subset only (`postgres` + `rabbitmq` + `ingestion-api` +
`consumer-pix` — **not** the other 4 consumers), default config, 6-vCPU/2GB
Podman VM. Numbers below are the baseline to diff future runs against.

| Metric | Value |
|---|---|
| Total requests | 811,065 |
| Throughput | 1,351 req/s |
| P50 | 48.24ms |
| P90 | 141.59ms |
| **P95** | **199.68ms** (threshold `<500ms` ✓) |
| **P99** | **465.22ms** (threshold `<1000ms` ✓) |
| Max | 2.84s |
| Error rate | 0.00% (threshold `<1%` ✓) |
| Outbox `NEW` (undispatched) at test end | 800,165 |
| Outbox `PUBLISHED` at test end | 10,900 |
| `payments` rows persisted | 2,218 (PIX only — only `consumer-pix` was running) |

**Key finding: the HTTP/DB write path is not the bottleneck — `DispatchOutbox`'s
default tuning is.** `OUTBOX_DISPATCH_BATCH_SIZE=50` every
`OUTBOX_DISPATCH_INTERVAL_MS=500` is a hard ceiling of ~100 rows/sec
dispatched to RabbitMQ, regardless of how fast rows are inserted. At
1,351 req/s sustained for 10 minutes (811K inserts), the dispatcher could only
ever clear ~60,000 of them in real time — hence the 800K-row `NEW` backlog.
ingestion-api's own write path (Gin → Postgres `INSERT ... ON CONFLICT`) kept
up fine; `DispatchOutbox`'s poll batch/interval is the actual throughput
ceiling for outbox→RabbitMQ delivery at this load level. Bumping
`OUTBOX_DISPATCH_BATCH_SIZE`/lowering `OUTBOX_DISPATCH_INTERVAL_MS` (or
relying more on the LISTEN/NOTIFY wakeup, Phase 5 Track 3.A, for lower
*latency* on the first row of a batch — it doesn't raise the *per-batch* cap)
is the lever to pull if real-time dispatch at this rate matters.

## 6.2 — Autoscaling (Kubernetes-only)

```bash
make loadtest-autoscale METHOD=PIX TARGET_URL=<ingestion-api-url>
```

Floods one method (default `PIX`) at a rate that outpaces its single
consumer, driving that method's queue backlog past the KEDA trigger
(`queueLengthValue: "1000"`, see `helmcharts/transaction-outbox/values.yaml`).
**Do not pin consumers for this test** — remove any `paused-replicas`
annotation left over from 6.1 first. Watch `kubectl get pods,scaledobject -n
transaction-outbox`: the target method's consumer should scale 0→1→…→10 (and
never above), then back to 0 once the load stage ends and the queue drains
(after `cooldownPeriod`). The scaling is the system under test, observed
out-of-band — k6 only produces the load.

## 6.3 — Consumer-worker in isolation

Needs a custom k6 binary with the `xk6-amqp` extension (`build/k6/Dockerfile`).
k6 only does the publish side here — **consumer-worker's own behavior is
read from its Prometheus `/metrics` endpoint**, not from k6 — see "Checking
consumer behavior" below.

```bash
make loadtest-up
make k6-ext-build
make loadtest-consumer METHOD=PIX N=50000 \
  RABBITMQ_URL=amqp://guest:guest@localhost:5672/
```

Publishes `N` messages at a **fixed 100 VUs** straight onto
`payments.<method>.queue` (the exact shape `DispatchOutbox`'s publisher puts
on the wire — bypassing ingestion-api entirely), hitting RabbitMQ as hard as
it can. Every run is a **mix** of three message shapes by default, so a
single invocation exercises all of the consumer's outcomes in one pass:

- the rest — unique, well-formed → `outcome=ack`
- `DUP_FRACTION` (10%) — reuses a prior iteration's identity → `outcome=duplicate`
- `SCHEMA_FRACTION` (2%) — unrecognized `schemaVersion` → `outcome=unknown_schema_version`
  (rejected straight to the DLQ on the first attempt, no retry wait)

Set either fraction to `0` (`-e DUP_FRACTION=0`) for a clean-only run.

### Multiple methods at once, multiple consumers per method

`METHODS` (comma-separated, e.g. `PIX,TRANSFER`) replaces `METHOD` to split
`N` evenly across methods via round-robin — each method still writes to its
own hypertable (`payments_pix`/`payments_transfer`), so this is also how you
compare two methods' consumer+DB throughput side by side. Bring up a second
instance bound to the same queue to test whether adding consumers actually
scales DB write throughput, or whether Postgres/PgBouncer becomes the
bottleneck first:

```bash
make loadtest-up
podman compose up -d consumer-pix-extra consumer-transfer   # 2nd PIX consumer + the TRANSFER one
make k6-ext-build
make loadtest-consumer METHODS=PIX,TRANSFER N=150000 \
  RABBITMQ_URL=amqp://guest:guest@localhost:5672/
```

`consumer-pix-extra` (`docker-compose.yml`) is bound to the same
`payments.pix.queue` as `consumer-pix` — RabbitMQ round-robins deliveries
between the two (the competing-consumers pattern), so PIX effectively runs
with 2 consumer instances against TRANSFER's 1. See Results below for what
that comparison actually showed.

### Checking consumer behavior

```bash
# Snapshot before (e.g. right after `podman restart` to zero counters),
# then again after the run finishes draining, and diff. One curl per
# consumer instance — :9091/:9097 are consumer-pix/consumer-pix-extra,
# :9093 is consumer-transfer (see docker-compose.yml's METRICS_PORTs).
curl -s http://localhost:9091/metrics | grep consumer_messages_processed_total
curl -s http://localhost:9097/metrics | grep consumer_messages_processed_total
curl -s http://localhost:9093/metrics | grep consumer_messages_processed_total
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/payments.pix.queue | grep -o '"messages":[0-9]*'
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/payments.pix.dlq | grep -o '"messages":[0-9]*'
```

`consumer_messages_processed_total{outcome=...}` is the per-outcome counter
(`ack`/`duplicate`/`retry_scheduled`/`poison_dlq`/`unknown_schema_version`);
summed across every instance bound to a method's queue, the delta should
always equal that method's published count exactly — no message is ever
silently lost (see Results below for a worked example). `make
purge-loadtest-dlq METHOD=PIX` removes only the `providerName="LOADTEST"`
messages a run leaves in the DLQ afterward, without touching any real
message — see the main [README.md](../README.md)'s `outbox-admin` section.

### Results (local compose, 2026-06-23)

`make loadtest-up` subset plus `consumer-pix-extra` + `consumer-transfer`
(`postgres` + `rabbitmq` + `ingestion-api` + 2× PIX consumer + 1× TRANSFER
consumer), `METHODS=PIX,TRANSFER`, `N=150000`, default fractions, 6-vCPU/2GB
Podman VM. All three consumer instances were restarted immediately before
this run to zero their `/metrics` counters.

| Metric | PIX (2 consumers) | TRANSFER (1 consumer) |
|---|---|---|
| Published | 75,022 | 74,978 |
| `outcome=ack` | 73,029 (36,515 + 36,514 — near-perfectly split between the two instances) | 72,966 |
| `outcome=duplicate` | 661 | 648 |
| `outcome=unknown_schema_version` | 1,332 | 1,364 |
| **Reconciliation** | 73,029+661+1,332=**75,022** exact | 72,966+648+1,364=**74,978** exact |
| DLQ depth after drain | 1,332 (exact match) | 1,364 (exact match) |
| Backlog fully drained | **~40s** after publish finished | **~100s** after publish finished |

Publish itself: 150,000 messages in 61.2s (2,452 msg/s), split almost
exactly 50/50 by the round-robin (75,022 / 74,978).

**The actual finding:** 2 consumers drained PIX's backlog roughly **2.5x**
faster than TRANSFER's 1 consumer drained an equal-sized backlog — at or
above linear scaling with consumer count. That means Postgres/PgBouncer is
**not yet the bottleneck** at 2 concurrent consumer connections per method
on this box; the per-message write path itself scales with consumer count
here. If you need to find where the DB *does* start to cap throughput, the
next step is adding more instances (`consumer-pix-3`, ...) until the
combined drain rate stops scaling linearly with instance count.

(Fewer duplicates land as `outcome=duplicate` than were flagged at publish
time — the script flags one by reusing a prior iteration's identity, but
with `shared-iterations` spread across 100 concurrent VUs, that prior
iteration may not have published yet, or may itself be schema-flagged, by
the time its pair runs. A property of the publish-side simulation under
concurrency, not a consumer bug — the reconciliation above is what actually
matters. The dup-reuse step is `METHODS.length` iterations back, not 1, so a
duplicate always lands on the same method/table as its pair — otherwise a
PIX/TRANSFER pair would land in different hypertables and never collide.)

A real, reproducible bug surfaced getting to this result: `xk6-amqp`'s
connection registry is an unsynchronized map, and every VU calling
`amqp.start()` concurrently at VU-init could hit a `fatal error: concurrent
map writes` that crashed the whole process. Fixed in `k6-consumer.js` by
starting the connection once in `setup()` (runs single-threaded before any
VU starts) and sharing it via `connection_id`.

Pin the target method's consumer count for a clean, repeatable number; an
unpinned rerun on Kubernetes lets the same backlog drive KEDA 0→N→0,
mirroring 6.2 from the queue side.

## Shared

`payloads.js` builds a valid wire-format body for every method
(`PIX`/`BOLETO`/`TRANSFER`/`CARTAO_CREDITO`/`CARTAO_DEBITO`) with a unique
`eventId`/`Idempotency-Key` per call — without that, dedup collapses every
iteration into one outbox row and the test measures nothing.
