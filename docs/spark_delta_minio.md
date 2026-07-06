# Spark + Delta + MinIO (Day 9)

The trickiest integration in the project, validated **in isolation** with a
hello-world before real jobs run on it. Proves the engine (Spark), the table
format (Delta), and the storage (MinIO over S3A) all talk to each other.

- Image: [`spark_jobs/Dockerfile`](../spark_jobs/Dockerfile)
- Smoke test: [`spark_jobs/test_delta.py`](../spark_jobs/test_delta.py)
- Runner: the `spark` service in [`docker-compose.yml`](../docker-compose.yml)

## The version-matching problem (why this is finicky)

Spark, Hadoop's S3A connector, and Delta must all line up, or you get cryptic
`ClassNotFound` / `NoSuchMethod` errors at runtime. The verified-compatible set:

| Component | Version | Constraint |
|---|---|---|
| Spark | 3.5.3 | base engine |
| Delta (`delta-spark_2.12`, `delta-storage`) | 3.2.0 | Delta 3.2.x ↔ Spark 3.5.x |
| `hadoop-aws` | 3.3.4 | must match Spark's bundled Hadoop client (3.3.4) |
| `aws-java-sdk-bundle` | 1.12.530 | the AWS SDK `hadoop-aws` 3.3.4 uses |

These JARs are **baked into the image** (see the Dockerfile) rather than fetched
at runtime via `--packages`, so job runs are reproducible and need no network.

## The S3A config that makes MinIO work

Spark reaches MinIO through the S3A connector. Four settings matter, three of
them MinIO-specific (they'd differ against real AWS S3):

```python
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")          # point at MinIO, not AWS
.config("spark.hadoop.fs.s3a.path.style.access", "true")              # MinIO needs path-style URLs
.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")       # local MinIO is plain HTTP
.config("spark.hadoop.fs.s3a.access.key", "...")                      # credentials
.config("spark.hadoop.fs.s3a.secret.key", "...")
```

- **path-style access**: AWS S3 defaults to *virtual-host* style
  (`bucket.s3.amazonaws.com`). MinIO uses *path* style (`minio:9000/bucket`), so
  this must be `true` or bucket resolution fails.
- **ssl disabled**: our local MinIO serves HTTP, not HTTPS. In the cloud this
  flips to `true`.

The two `spark.sql.*` Delta configs register Delta's SQL extensions and catalog,
which is what enables `format("delta")`, time travel, `MERGE`, etc.

## What `_delta_log` is (Delta = ACID on object storage)

Object storage has **no transactions** — a bare pile of Parquet files can be seen
half-written, and concurrent writers clobber each other. Delta adds ACID *on top*
of the files with a **transaction log**: the `_delta_log/` directory next to the
data.

- Every write creates the data files (Parquet) **plus** an atomic JSON commit in
  `_delta_log/` (`00000000000000000000.json`, then `...001.json`, ...).
- Each commit lists **actions**: `protocol` (reader/writer versions), `metaData`
  (schema, partitioning), and `add`/`remove` (which files belong to this
  version). A reader reconstructs the table by replaying the log — **the log, not
  the file listing, is the source of truth.**
- Atomicity comes from the single JSON commit "flipping" a version live: readers
  only ever see complete versions, never a half-written state.
- Because old versions' `add` entries persist, you get **time travel**
  (`versionAsOf 0`) and safe concurrent reads/writes.

The smoke test makes this concrete: it does two commits (an overwrite then an
append), so you'll see **two** JSON files in `_delta_log`, read the current
3-row table, *and* read the 2-row version 0 via time travel.

## Run it

```bash
# First run builds the image (downloads Spark + the jars; the AWS SDK bundle is
# ~280 MB, so the first build is slow). Subsequent runs are instant.
docker compose run --rm spark
```

Expected: `Spark 3.5.3 started ...`, two commits written, a 3-row current table,
a 2-row version-0 table, two files in `_delta_log`, and the decoded log actions.

Then browse MinIO (http://localhost:9001) → `bronze` bucket → `_smoke/test_delta`
to see the Parquet files and the `_delta_log/` directory on disk.

## Why "prove it in isolation" first

Spark ↔ S3A ↔ Delta is the single most failure-prone wiring in the stack. Getting
a hello-world green *before* building the streaming bronze job (Day 10) means that
when something breaks later, we know it's our *logic*, not the plumbing. This is
the same reason we stood up Kafka before the producer.
