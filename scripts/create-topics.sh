#!/bin/bash

sleep 10

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="kafka-broker-1:9092"

echo "Creating topics..."

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists --topic lamp-telemetry \
  --partitions 3 --replication-factor 3 \
  --config retention.ms=86400000 --config cleanup.policy=delete

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists --topic lamp-commands \
  --partitions 2 --replication-factor 3 \
  --config retention.ms=604800000 --config cleanup.policy=delete

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists --topic lamp-analytics \
  --partitions 1 --replication-factor 3 \
  --config retention.ms=2592000000 --config cleanup.policy=delete

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists --topic lamp-commands-dlq \
  --partitions 1 --replication-factor 3 \
  --config retention.ms=604800000 --config cleanup.policy=delete

echo "Topics created:"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe
