# Kafka Setup (Day 3): single-broker KRaft in Docker

The ingestion backbone. A single Apache Kafka broker running in **KRaft mode**,
defined in [`docker-compose.yml`](../docker-compose.yml).

## Why Kafka at all

Kafka is a durable, replayable **event log** that sits between the producer
(Coinbase → Kafka) and the consumers (Spark). It decouples them: the producer
can publish even if Spark is down, and Spark can re-read ("replay") history
because Kafka retains messages. That replayability is what later makes backfills
and the Lambda/Kappa story possible.

## Why KRaft (not ZooKeeper)

Kafka needs a place to store cluster **metadata**: which broker leads each
partition, topic configs, ACLs, etc.

- **Old way — ZooKeeper:** a *separate* system you had to deploy, secure, tune,
  and monitor alongside Kafka. Two moving parts, two failure domains.
- **New way — KRaft (Kafka Raft):** Kafka manages its own metadata internally
  using the Raft consensus protocol. One system. Faster failovers, faster
  startup, simpler ops, and higher metadata scalability.

As of **Kafka 4.x, ZooKeeper is removed entirely** — KRaft is the only option.
For local dev this is ideal: our single container is the whole Kafka, playing
both **broker** (data) and **controller** (metadata/Raft) roles.

## Listener design (the "advertised listeners" gotcha)

Kafka clients first connect to a bootstrap address, then Kafka tells them the
**advertised** address to actually use. If that advertised address isn't
reachable from where the client runs, connections fail — the #1 Docker+Kafka
footgun. We define three listeners:

| Listener | Bind | Advertised as | Used by |
|---|---|---|---|
| `HOST` | `0.0.0.0:9092` | `localhost:9092` | tools/producer on the laptop |
| `DOCKER` | `0.0.0.0:29092` | `kafka:29092` | other containers (Spark, Week 2) |
| `CONTROLLER` | `0.0.0.0:9093` | — | internal KRaft consensus |

Only `9092` is published to the host. In-network services will use `kafka:29092`.
Setting this up now means we don't rewire networking when Spark arrives.

## Topic: `crypto.trades.raw`

Created via [`scripts/create_topics.sh`](../scripts/create_topics.sh):
**6 partitions, replication factor 1**.

### Why 6 partitions

Partitions are Kafka's unit of parallelism and ordering. The decision is a
tradeoff, informed by the Day 2 findings (see
[`coinbase_websocket_schema.md`](./coinbase_websocket_schema.md)):

- On Day 4 we **key messages by `product_id`** so each market's trades stay
  ordered. Kafka routes by `hash(key) % partitions`, so a product always lands
  in the same partition. With only **2 products today (BTC, ETH)**, at most 2
  partitions will hold data — the rest sit idle (our observed **skew**).
- Throughput is a non-factor: peak was ~59 msg/s, trivial for one broker. So the
  partition count is **not** about load — it's about **future headroom**.
- **6** leaves room to add products (SOL, DOGE, ...) and consumer parallelism
  without being so large that most partitions are permanently empty.

### Replication factor 1

Only one broker exists, so RF must be 1 (no other broker to hold a replica). In
production you'd run 3+ brokers with RF=3 for fault tolerance. Documented as a
known single-node limitation.

### Caveat: don't casually add partitions later

Increasing a topic's partition count changes `hash(key) % partitions`, so
existing keys can remap to different partitions — breaking strict per-key
ordering across the change and scrambling any partition-based assumptions.
Choose the count deliberately up front; treat later increases as a real
migration.

## Run it

```bash
# 1. Start Docker Desktop, then:
docker compose up -d

# 2. Wait until healthy:
docker compose ps

# 3. Create the topic:
./scripts/create_topics.sh

# 4. Verify end-to-end (two terminals) — see the runbook / commands below.
```
