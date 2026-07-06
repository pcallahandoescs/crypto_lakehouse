# MinIO Setup (Day 8): object storage for the lakehouse

The storage foundation. **MinIO** is an S3-compatible object store, defined in
[`docker-compose.yml`](../docker-compose.yml). The Delta medallion tables
(bronze → silver → gold) will live here starting Day 9.

## Why object storage (and not a database or a filesystem)

A lakehouse separates **storage** from **compute**. The data lives as files in an
object store; engines like Spark read/write those files independently.

- **Cheap & scalable** — object storage is the cheapest durable storage at scale
  and grows without capacity planning.
- **Decoupled storage/compute** — you can scale, restart, or replace the compute
  (Spark) without touching the data, and vice versa. This is *the* defining
  lakehouse property; a traditional database couples the two.
- **Not a filesystem** — object stores expose a flat key→object API (`PUT`/`GET`
  by key), not POSIX files. "Folders" are just key prefixes. This model is what
  makes them cheap, massively parallel, and cloud-native.

## Why S3-compatible (MinIO specifically)

MinIO speaks the **Amazon S3 API**. That means Spark reaches it through the exact
same `s3a://` connector and code path it would use against real S3 — so the whole
design is **cloud-portable**: swapping MinIO for S3/GCS/ADLS later is a config
change (endpoint + credentials), not a rewrite. MinIO gives us a realistic,
free, local stand-in for cloud object storage.

## Image choice: Chainguard (not Docker Hub / Quay)

As of **October 2025, MinIO stopped publishing images to Docker Hub and Quay**.
The images that remain are unmaintained and carry a known, won't-fix
vulnerability (CVE-2025-62506). We therefore use **Chainguard's** maintained,
secure-by-default image:

- `cgr.dev/chainguard/minio` — the server
- `cgr.dev/chainguard/minio-client` — `mc`, for bucket creation

Rationale (and the tradeoff of Chainguard's free `:latest`-only tags) is recorded
in [ADR 0011](./adr/0011-minio-image-chainguard.md).

## The medallion buckets

We create three buckets, one per medallion layer:

| Bucket | Layer | Contents (from Day 9 on) |
|---|---|---|
| `bronze` | raw | trades exactly as ingested, append-only, immutable |
| `silver` | clean | parsed, typed, deduplicated trades |
| `gold` | curated | OHLC candles, VWAP, volume; `gold_realtime` metrics |

> **Layout choice:** separate buckets per layer (clean isolation, easy per-layer
> policies/lifecycle later). The common alternative is a single `lakehouse`
> bucket with `bronze/`, `silver/`, `gold/` *prefixes* — equally valid; in object
> storage a "folder" is just a key prefix, so this is a soft, reversible choice.

## How buckets get created (the shell-free init pattern)

Bucket creation is a one-shot `createbuckets` service in Compose using the
Chainguard `mc` image. Two details worth understanding:

1. **No shell in the image.** Chainguard's minimal images ship only the app (here,
   the `mc` binary) — no `/bin/sh`. So the usual
   `entrypoint: sh -c "until ...; mc mb ..."` wait-loop is impossible. Instead we
   pass `mc` arguments directly as the `command` and configure the target via the
   `MC_HOST_local` environment variable (no `mc alias set` step needed).
2. **Waiting without a loop.** Since we can't script a retry, we let **Docker do
   it**: `restart: on-failure` re-runs the container until MinIO is accepting
   connections. `mc mb --ignore-existing` is idempotent, so re-runs are safe.

The same shell-less constraint is also why the `minio` service has **no
healthcheck**: there's no `curl`/shell in the image to hit `/minio/health/live`
from inside. Rather than fight it, we delegate readiness to clients (they retry).
This is a deliberate tradeoff of the secure minimal image; a team that needs a
container healthcheck could use Chainguard's `-dev` image variant (it bundles a
shell) purely for that probe.

This means buckets are created automatically on `docker compose up`, and can be
re-created any time with:

```bash
docker compose run --rm createbuckets
```

## Run it

```bash
docker compose up -d minio createbuckets   # or just: docker compose up -d
docker compose ps                          # minio: Up (no healthcheck — see note)
docker compose logs createbuckets          # shows the 3 buckets created

# Browse the console:
open http://localhost:9001                  # login: minioadmin / minioadmin
```

You should see `bronze`, `silver`, and `gold` in the console. The S3 API is on
`http://localhost:9000` (host) and `http://minio:9000` (in-network, for Spark).

## Credentials note

`minioadmin/minioadmin` are **local-dev only**. In production these are the root
credentials and would be injected as secrets (and you'd create scoped, least-
privilege access keys per service rather than sharing root). Called out here so
the local shortcut isn't mistaken for a production pattern.
