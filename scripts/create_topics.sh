#!/usr/bin/env bash
# Create (idempotently) the raw trades topic, then print its description.
# Run AFTER `docker compose up -d` and the broker is healthy:
#     ./scripts/create_topics.sh
#
# Safe to run repeatedly (--if-not-exists).
set -euo pipefail

TOPIC="crypto.trades.raw"
PARTITIONS="${PARTITIONS:-6}"
REPLICATION="${REPLICATION:-1}"   # single broker -> RF must be 1
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

echo "Creating topic '${TOPIC}' (partitions=${PARTITIONS}, rf=${REPLICATION}) ..."
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}"

echo
echo "Topic description:"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --describe --topic "${TOPIC}"
