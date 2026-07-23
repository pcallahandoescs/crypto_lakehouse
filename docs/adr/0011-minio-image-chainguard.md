# 0011. MinIO container image: Chainguard

- **Status:** Accepted
- **Date:** 2026-07-06

## Context

[ADR 0004](./0004-object-storage-minio.md) chose MinIO for S3-compatible object
storage. That decision is about the *software*; this one is about which *image*
to run. Circumstances changed the obvious answer:

- As of **October 2025, MinIO stopped publishing container images to Docker Hub
  and Quay** (`minio/minio`, `quay.io/minio/minio`).
- The images left behind are **unmaintained** and carry a known, won't-fix
  vulnerability (**CVE-2025-62506**).

Running a knowingly-vulnerable, unmaintained image contradicts this project's
security posture (we already run non-root containers, keep secrets out of images,
etc.), so the default `minio/minio:latest` is off the table.

## Decision

We will run MinIO from **Chainguard's** maintained, secure-by-default images:
`cgr.dev/chainguard/minio` (server) and `cgr.dev/chainguard/minio-client` (`mc`,
for bucket creation).

## Consequences

- Maintained, regularly-patched, minimal (low-CVE) images — consistent with the
  project's security stance.
- Chainguard's **minimal images have no shell** (and no `curl`), which changes two
  things: (1) bucket creation can't use a `sh -c` wait-loop — we pass `mc` args
  directly, target via `MC_HOST_*`, and let `restart: on-failure` handle waiting;
  (2) the server can't self-probe, so it runs with **no container healthcheck** —
  readiness is delegated to clients, which retry. (See `docs/minio_setup.md`.)
- On Chainguard's **free tier only `:latest` is available** (pinned/versioned tags
  need a paid subscription). So we can't fully pin the MinIO version for free —
  an accepted tradeoff for a local project; a funded team would pin.

## Alternatives considered

- **`quay.io/minio/minio:RELEASE...` (pinned)** — pinnable, but unmaintained and
  vulnerable post-Oct-2025. Rejected on security grounds.
- **Bitnami MinIO image** — also no longer maintained. Rejected.
- **AWS S3 directly (skip MinIO)** — not self-hosted/free/local; loses the
  one-command local stack. Deferred to the cloud story, not a local replacement.
