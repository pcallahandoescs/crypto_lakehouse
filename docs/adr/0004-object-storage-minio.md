# 0004. Object storage: MinIO (S3-compatible)

- **Status:** Accepted
- **Date:** 2026-07-05

## Context

A lakehouse stores table data as files in **object storage**, decoupled from the
compute that reads/writes them. We need something that runs locally (for a
self-contained Compose/K8s stack) yet mirrors how real cloud lakehouses work, so
the design is portable to any cloud later without rework.

## Decision

We will use **MinIO** as the object store, with bucket/prefixes for the bronze,
silver, and gold layers. MinIO speaks the **S3 API**, so Spark reaches it via the
standard S3A connector — the identical code path used against real S3.

## Consequences

- Storage and compute are decoupled (scale/replace independently) — the defining
  lakehouse property.
- S3 compatibility makes the whole thing **cloud-portable**: swapping MinIO for
  S3/GCS/ADLS is a config change, not a rewrite.
- One more stateful service to run locally (becomes a StatefulSet + PVC on K8s).

## Alternatives considered

- **Cloud object storage (S3/GCS/ADLS)** — the production target, but not
  self-hosted/free for a local stack. The S3A path means we can move
  there trivially. Deferred, not rejected.
- **Local filesystem / HDFS** — filesystem isn't object storage (misses the
  decoupling/portability lesson); HDFS is heavy and legacy for this use. Rejected.
