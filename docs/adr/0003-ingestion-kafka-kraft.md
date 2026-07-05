# 0003. Ingestion: Apache Kafka in KRaft mode

- **Status:** Accepted
- **Date:** 2026-07-05

## Context

Between a live producer and downstream consumers (Spark speed + batch layers) we
need a durable buffer that (a) decouples producer from consumers so either can
fail/restart independently, (b) **retains** messages so history can be replayed
for backfills, and (c) preserves per-key ordering. The source is live-only, so
without a retaining log we could never reprocess.

## Decision

We will use **Apache Kafka** as the ingestion log, running in **KRaft mode**
(Kafka's built-in Raft metadata quorum, no ZooKeeper). Locally it's a single
broker acting as both broker and controller, defined in `docker-compose.yml`.
Trades go to the `crypto.trades.raw` topic (6 partitions, keyed by
`product_id`). Details in [`docs/kafka_setup.md`](../kafka_setup.md).

## Consequences

- Durable, replayable event log — the foundation for exactly-once bronze
  ingestion, backfills, and the Lambda/Kappa discussion.
- KRaft means one system to run instead of two (no ZooKeeper): simpler ops,
  faster startup/failover. In Kafka 4.x ZooKeeper is removed anyway.
- Single broker ⇒ replication factor 1 (no fault tolerance) — an accepted local
  limitation; production would run 3+ brokers with RF=3.
- Partition count is chosen for future headroom, not current load (~59 msg/s
  peak is trivial); increasing it later is a real migration (rehashing keys).

## Alternatives considered

- **Redpanda** — Kafka-API-compatible, lighter (single binary, no JVM). Great
  choice; Kafka picked for being the industry standard and the exact skill to
  demonstrate.
- **Cloud (Kinesis/PubSub)** — managed, but not self-hostable in a local,
  portable Compose/K8s stack. Rejected for this project's constraints.
- **Direct WebSocket → storage (no log)** — loses decoupling and replay.
  Rejected.
