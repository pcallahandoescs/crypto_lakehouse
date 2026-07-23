# Serving API

The gold layer is only useful if something can read it. The serving layer is the
lakehouse's front door: a small, read-only **FastAPI** service that returns gold
rows over HTTP — historical candles, real-time metrics, and pipeline health.

The key design choice: it reads Delta **without Spark**. A request/response API
needs millisecond-cheap table opens, not a multi-second JVM session — so it uses
[**delta-rs**](https://delta-io.github.io/delta-rs/) (`deltalake`, a Rust
implementation) to read the `_delta_log` + Parquet straight from MinIO and hand
back Apache Arrow. See [ADR 0014](./adr/0014-serving-api-fastapi-deltars.md).

## Endpoints

| Method | Path | Returns |
|---|---|---|
| `GET` | `/health` | Liveness + which gold tables are reachable |
| `GET` | `/products` | The product ids this deployment serves |
| `GET` | `/candles` | Historical OHLC candles from `gold/ohlc` |
| `GET` | `/metrics/realtime` | Latest speed-layer windows from `gold/realtime_metrics` |
| `GET` | `/metrics/runs` | Recent job-run records from the observability table |

Interactive OpenAPI docs are served at **`/docs`**.

### `/candles`

| Query param | Type | Default | Notes |
|---|---|---|---|
| `product` | string | — | **Required**, e.g. `BTC-USD` |
| `start` | datetime | — | Inclusive lower bound on `interval_start` (ISO 8601) |
| `end` | datetime | — | Inclusive upper bound on `interval_start` |
| `limit` | int | `200` | 1–5000; rows are newest-first |

```bash
curl 'http://localhost:8000/candles?product=BTC-USD&limit=3'
```

```json
[
  {
    "product_id": "BTC-USD",
    "interval_start": "2026-07-18T12:00:00Z",
    "interval_end": "2026-07-18T12:01:00Z",
    "open": "60000.00", "high": "60100.00", "low": "59900.00", "close": "60050.00",
    "volume": "1.5", "vwap": 60010.5, "trade_count": 42
  }
]
```

Money (`open/high/low/close/volume`) is serialized as a **string** to preserve
exact `DECIMAL` precision (see [ADR 0008](./adr/0008-data-contract-pydantic.md));
`vwap`/`price_volatility` are floats, matching how the Spark jobs derive them.

## Design

- **`serving/config.py`** — env-driven settings (`MINIO_ENDPOINT`, credentials,
  `GOLD_BASE_URI`, `PRODUCTS`). Defaults target the compose network; point
  `MINIO_ENDPOINT` at real S3/GCS/ADLS with no code change.
- **`serving/store.py`** — `DeltaStore`, the only thing that touches Delta. The
  `product_id` filter is pushed down to Arrow; the time clip, ordering, and limit
  run in Python. A missing table returns `[]` (an unrun job isn't a 500).
- **`serving/models.py`** — Pydantic response schemas (the public data contract).
- **`serving/main.py`** — the routes; store + settings are injected as FastAPI
  dependencies so tests swap in a fake.

## Running it

```bash
# Local stack (starts with the rest of the services):
docker compose up -d serving
open http://localhost:8000/docs

# Or run against MinIO from the host:
MINIO_ENDPOINT=http://localhost:9000 uv run uvicorn serving.main:app --reload
```

## Testing

`tests/test_serving.py` overrides the store/settings dependencies with in-memory
fakes and drives the full HTTP surface with FastAPI's `TestClient` — routing,
query validation, field mapping, and Decimal serialization — with **no MinIO,
Spark, or JVM**. It runs in the fast gate (`make check`).

## Scale-up path

At higher traffic the two cheap wins are (1) caching Delta table handles instead
of opening per request, and (2) pushing the time-range predicate down to Arrow
(partition-pruning on the `date` column). Beyond that, precomputing hot
aggregates into a serving store (Redis/Postgres) is the standard next step.
