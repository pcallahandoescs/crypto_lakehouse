# ADR 0016: Deployment on Kubernetes (kind), stateful backbone as StatefulSets

**Status:** Accepted
**Date:** 2026-07-24

## Context

The stack runs on Docker Compose, which is ideal for local wiring but is not a
deployment story: it doesn't self-heal, reschedule failed containers, roll out
updates, or scale. The project's goal includes a real, declarative deployment —
so the known-good Compose stack is migrated to Kubernetes. The first step is the
stateful backbone (Kafka + MinIO); the app tier follows.

## Decision

Deploy to **Kubernetes**, targeting a local **kind** cluster, with the stateful
services modeled as **StatefulSets**.

- **kind over minikube** for local: it runs a conformant cluster inside Docker
  (no VM), so it's fast to create/destroy and mirrors CI-friendly setups.
- **StatefulSets for Kafka and MinIO.** Both own durable state and need a stable
  network identity plus a per-pod PersistentVolume (`volumeClaimTemplates`), which
  is exactly what StatefulSets provide and Deployments do not. Each gets a
  **headless Service** for stable pod DNS and a **ClusterIP Service** whose name
  matches the Compose service (`kafka`, `minio`) so app config is unchanged.
- **Kustomize** (`kubectl apply -k k8s/`) applies the whole backbone as one unit,
  namespaced to `crypto`.
- **Credentials in a Secret**; bootstrap (buckets, topic) as **idempotent,
  retrying Jobs** — the direct analogue of Compose's `createbuckets` container and
  `create_topics.sh`.
- Kafka stays **single-node KRaft** (broker+controller in one pod), advertising
  its ClusterIP Service FQDN so bootstrapping clients get a resolvable address.

## Consequences

**Positive**

- A real, declarative deployment target: self-healing, reschedulable, scalable.
- The Compose→K8s mapping is explicit and documented (`docs/kubernetes.md`),
  which is the transferable skill.
- Service names carry over from Compose, so app configuration doesn't change.

**Negative**

- Single-replica StatefulSets aren't highly available (fine for a local demo;
  real HA means multi-broker Kafka + distributed MinIO, out of scope).
- Two orchestrators to keep in sync (Compose for dev, K8s for deploy); the shared
  container images keep the drift small.

## Alternatives considered

- **minikube** — solid, but VM-based and heavier; kind is faster for
  create/destroy and closer to how clusters run in CI.
- **Deployments + external PVCs for Kafka/MinIO** — loses stable identity and the
  clean per-pod volume story; StatefulSets are the purpose-built fit.
- **A Kafka/MinIO operator (Strimzi, MinIO Operator)** — production-grade and
  powerful, but a large abstraction to take on for a single-node demo; hand-written
  manifests keep the K8s concepts visible, which is the point here.

## Follow-ups

- **App tier (done)** — producer, serving, and dashboard as Deployments (serving/
  dashboard exposed via NodePort); the Spark streaming jobs (bronze, speed) as
  Deployments running `spark-submit --master local[*]`, and the batch medallion
  (silver → gold → DQ) as a one-shot Job. Locally-built images are side-loaded
  with `kind load docker-image`. Verified end-to-end on kind: live trades flow
  through to candles served by the API and rendered in the dashboard.
- **Packaging + hardening (done)** — resource requests/limits, health probes, and
  a Helm chart for a one-command parameterized deploy. See
  [ADR 0017](./0017-helm-packaging-production-hardening.md).
