# ADR 0017: Helm packaging + production hardening (resources, probes)

**Status:** Accepted
**Date:** 2026-07-25

## Context

The raw Kubernetes manifests ([ADR 0016](./0016-deployment-kubernetes-kind.md))
deploy the stack but are deliberately plain: no resource requests/limits, no
health probes, hard-coded image tags and credentials, and multi-step apply
ordering (backbone, then app tier, then the batch Job by hand). Plain YAML is the
clearest way to *read* what each object is, but it's not how you *operate* a
service. Deployment needs a packaged, parameterized, hardened artifact.

## Decision

Package the whole stack as a single **Helm chart** (`helm/crypto-lakehouse/`) and
add the production polish that was kept out of the raw manifests.

- **One chart, one command.** `helm install crypto-lakehouse helm/crypto-lakehouse`
  deploys the backbone and app tier together. A single `values.yaml` centralizes
  images, credentials, replica counts, resources, and probe toggles.
- **Resource requests/limits on every workload.** Requests give the scheduler
  something to bin-pack; limits protect the single kind node from a runaway
  container. Spark limits sit above `--driver-memory` to leave room for JVM
  non-heap + Python overhead.
- **Health probes** where there's a real signal: `httpGet` liveness/readiness/
  startup for MinIO (`/minio/health/live`), serving (`/health`), and dashboard
  (`/_stcore/health`); `tcpSocket:9092` for Kafka with a generous startup budget.
  Probes are kubelet-run, so `httpGet` works even on the shell-less Chainguard
  MinIO image. The Spark streaming drivers get **no liveness probe** on purpose —
  a false positive would kill a healthy driver that self-recovers from its Delta
  checkpoint.
- **Bootstrap Jobs as Helm hooks.** Bucket + topic creation run as
  `post-install,post-upgrade` hooks with `before-hook-creation` delete policy, so
  they re-run cleanly on every release (Jobs are otherwise immutable) and replace
  the manual apply-ordering. The batch medallion is a gated `post-upgrade` hook
  (`spark.batch.enabled`), off by default since it reads bronze.
- **Fixed Service names, not release-prefixed.** `kafka`, `minio`, `serving`, and
  `dashboard` stay literal so the in-cluster DNS contract (and app config) is
  identical across Compose, raw manifests, and Helm.
- **Credentials stay swappable.** `credentials.existingSecret` skips the chart's
  dev Secret entirely in favor of one from a real secret store.

The raw manifests in `k8s/` are kept as the readable reference; the chart is the
recommended deploy path.

## Consequences

**Positive**

- Deploy/upgrade/rollback/uninstall become single Helm commands; hooks remove
  manual ordering.
- Requests/limits and probes make the workloads schedulable and self-healing —
  the actual point of running on Kubernetes.
- Every knob (images, credentials, resources, replicas) is one file, so promoting
  across environments is a values override, not a manifest fork.

**Negative**

- Two representations of the same objects (raw YAML + templated chart) to keep in
  sync; mitigated by treating raw manifests as the reference and the chart as the
  source of truth for deploys.
- Go templating adds indirection over plain YAML — a readability cost paid for
  parameterization.

## Alternatives considered

- **Kustomize overlays only** — already used for the raw manifests and great for
  environment patches, but it has no packaging/release lifecycle (install,
  upgrade, rollback, hooks) or values UX. Helm is the ecosystem standard for a
  shippable chart.
- **Plain manifests + a wrapper script** — reinvents a worse Helm.
- **An umbrella chart pulling community subcharts (Bitnami Kafka/MinIO)** —
  production-grade but heavy, and it hides the concepts this project exists to
  show; a self-contained chart over our own images keeps things legible.

## Validation

`helm lint`, `helm template` (renders all objects for default, batch-enabled, and
`existingSecret` value sets), and `kubectl apply --dry-run=client` over the
rendered output (every object passes API schema validation) — layered on the raw
manifests already running end-to-end on kind.
