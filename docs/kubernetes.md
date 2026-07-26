# Kubernetes deployment

The stack runs on Docker Compose for local development; Kubernetes is the
**deployment target** — the same containers, orchestrated declaratively with
self-healing, rolling updates, and horizontal scaling. This is the migration of a
known-good system from Compose to K8s, done in two steps:

- **The stateful backbone** — Kafka + MinIO as StatefulSets, on a local
  [kind](https://kind.sigs.k8s.io/) cluster.
- **The app tier** — producer, Spark jobs (streaming Deployments + a batch Job),
  serving API, and dashboard, layered on top.

## Why Kubernetes (and why kind locally)

Compose is great for wiring a local stack, but it doesn't self-heal, reschedule,
scale, or manage rollouts. Kubernetes does — declaratively — and it's the lingua
franca of production container orchestration. **kind** ("Kubernetes IN Docker")
runs a real, conformant cluster inside Docker containers: faster and lighter than
a VM-based minikube, and trivial to create/destroy for a reproducible demo.

## Compose → Kubernetes mapping

Translating the stack is mostly a matter of picking the right object per concern:

| Compose concept | Kubernetes object |
|---|---|
| Stateless service (producer, serving, dashboard) | **Deployment** + **Service** *(Day 25)* |
| Stateful service (Kafka, MinIO) | **StatefulSet** + headless **Service** + **PVC** |
| Named volume (`minio-data`, `kafka-data`) | **PersistentVolumeClaim** (via `volumeClaimTemplates`) |
| Inline env | container `env` / **ConfigMap** |
| Inline credentials | **Secret** |
| One-shot init container (`createbuckets`) | **Job** (idempotent, retrying) |
| `depends_on` ordering | retrying Jobs / probes / init-containers |
| Published port | **Service** + `kubectl port-forward` (or NodePort) |
| The default network | a **namespace** + cluster DNS (`svc.cluster.local`) |

**StatefulSet vs. Deployment** is the key call: Kafka and MinIO own durable state
and need a stable identity (`kafka-0`, `minio-0`) plus a per-pod persistent
volume, so they're StatefulSets. The app tier is stateless and interchangeable —
those become Deployments in Day 25.

## What's here

```
k8s/
  kind-cluster.yaml        # local cluster definition
  # --- stateful backbone: `kubectl apply -k k8s/` ---
  namespace.yaml           # the `crypto` namespace
  minio-secret.yaml        # MinIO credentials (dev only)
  minio.yaml               # MinIO StatefulSet + Services + PVC
  minio-buckets-job.yaml   # creates bronze/silver/gold buckets
  kafka.yaml               # Kafka StatefulSet + Services + PVC
  kafka-topic-job.yaml     # creates crypto.trades.raw
  kustomization.yaml
  apps/                    # --- app tier: `kubectl apply -k k8s/apps/` ---
    lakehouse-config.yaml  # shared non-secret env (endpoints, topic)
    producer.yaml          # Deployment: Coinbase WS -> Kafka
    spark-streaming.yaml   # Deployments: bronze ingest + speed layer
    spark-batch-job.yaml   # Job: silver -> gold -> DQ (run on demand)
    serving.yaml           # Deployment + NodePort: FastAPI (30800 -> :8000)
    dashboard.yaml         # Deployment + NodePort: Streamlit (30850 -> :8501)
    kustomization.yaml
```

## Run it

> **Run one orchestrator at a time.** `kind-cluster.yaml` maps host ports
> 8000/8501/9000/9001 for URL parity with Compose, so a running Compose stack
> holds those ports and cluster creation fails with `port is already allocated`.
> Stop Compose first: `docker compose down` (this keeps the named data volumes).

```bash
# 1. Install kind (once): https://kind.sigs.k8s.io/docs/user/quick-start/
brew install kind                     # macOS (Homebrew)
# ...or the binary directly if brew is unhappy:
#   curl -fsSL -o /usr/local/bin/kind \
#     https://github.com/kubernetes-sigs/kind/releases/latest/download/kind-darwin-arm64
#   chmod +x /usr/local/bin/kind

# 2. Create the cluster (stop Compose first — see note above)
kind create cluster --config k8s/kind-cluster.yaml

# 3. Apply the stateful backbone
kubectl apply -k k8s/

# 4. Watch it come up (StatefulSets first, then the init Jobs complete)
kubectl -n crypto get pods,statefulsets,jobs -w
```

Expect `kafka-0` and `minio-0` to reach `Running`, and the
`kafka-create-topic` / `minio-createbuckets` Jobs to reach `Completed` (they
retry until the backends accept connections).

## Verify

```bash
# MinIO buckets exist
kubectl -n crypto logs job/minio-createbuckets

# Kafka topic exists
kubectl -n crypto exec kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic crypto.trades.raw
```

## Deploy the app tier

The app images are built locally, so they must be **side-loaded into the kind
node** (there's no registry to pull from); the manifests use
`imagePullPolicy: IfNotPresent` to match.

```bash
# 1. Build the images (spark bakes the job code in — K8s has no bind-mount)
docker build -f producer/Dockerfile  -t crypto-lakehouse-producer:0.1.0  .
docker build -f serving/Dockerfile   -t crypto-lakehouse-serving:0.1.0   .
docker build -f dashboard/Dockerfile -t crypto-lakehouse-dashboard:0.1.0 .
docker build -f spark_jobs/Dockerfile -t crypto-lakehouse-spark:3.5.3    .

# 2. Load them into the cluster
for i in producer:0.1.0 serving:0.1.0 dashboard:0.1.0 spark:3.5.3; do
  kind load docker-image "crypto-lakehouse-$i" --name crypto-lakehouse
done

# 3. Deploy producer + streaming Spark jobs + serving + dashboard
kubectl apply -k k8s/apps/
kubectl -n crypto get pods -w
```

Once the producer and `bronze-ingest` have run for a minute (bronze needs data
before silver can read it), run the **batch medallion** to build silver + gold:

```bash
kubectl -n crypto delete job spark-batch-pipeline --ignore-not-found
kubectl -n crypto apply -f k8s/apps/spark-batch-job.yaml
kubectl -n crypto logs -f job/spark-batch-pipeline    # silver -> gold -> DQ
```

Then the API and dashboard are live at the host ports kind maps
(`kind-cluster.yaml`):

```bash
curl 'http://localhost:8000/candles?product=BTC-USD&limit=2'   # serving API
open http://localhost:8501                                     # dashboard
```

> **Spark on one node.** The jobs run `spark-submit --master local[*]` inside a
> single pod (driver = executors), exactly as Compose did — not native
> Spark-on-K8s cluster mode (separate executor pods), which is a larger lift and
> unneeded at this scale. Running several drivers on one node causes "batch
> falling behind" warnings under CPU contention; they're harmless here (Kafka lag
> stays at 0). Scale a streaming job down with
> `kubectl -n crypto scale deploy/speed-metrics --replicas=0` to free resources.

## Access from your laptop

The backbone is reached in-cluster by its Service names (`kafka:9092`,
`minio:9000`) — the same names Compose used, so app config is unchanged. To reach
MinIO from the host, port-forward:

```bash
kubectl -n crypto port-forward svc/minio 9000:9000 9001:9001
# console: http://localhost:9001  (minioadmin / minioadmin)
```

## Teardown

```bash
kubectl delete -k k8s/            # remove the workloads
kind delete cluster --name crypto-lakehouse   # or nuke the whole cluster
```

## Deferred (Day 26)

Production hardening is intentionally kept out of these manifests so the focus
stays on the Compose→K8s translation:

- resource **requests/limits** on every workload,
- **liveness / readiness / startup probes**,
- packaging everything as a **Helm chart** for a one-command, parameterized deploy.

Also future: running **Airflow** on K8s (today the batch medallion runs as a
one-shot Job; Airflow orchestration stays in Compose), and native
**Spark-on-K8s** cluster mode (separate executor pods) instead of `local[*]`.
