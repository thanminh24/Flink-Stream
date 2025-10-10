#!/usr/bin/env bash
set -euo pipefail

DEBUG="${DEBUG:-0}"
[[ "$DEBUG" == "1" ]] && set -x

log() { printf "\n=== %s ===\n" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SVC_KAFKA="CDC-Kafka"
RETRY_ATTEMPTS=10
RETRY_DELAY=15
DELAY_AFTER_STARTUP=30
DELAY_BEFORE_SERVER=20

log "Cleaning Kafka and Docker..."
docker compose down -v --remove-orphans || true
rm -rf "${ROOT_DIR}/data/kafka/tmp" || true

log "Starting Docker Compose..."
docker compose up -d

sleep "${DELAY_AFTER_STARTUP}"

# Check containers
for container in CDC-Kafka CDC-Flink-JobManager; do
  log "Checking $container..."
  for attempt in $(seq 1 $RETRY_ATTEMPTS); do
    if docker ps --filter "name=$container" --format '{{.Status}}' | grep -q "Up\|healthy"; then
      log "$container ready"
      break
    else
      log "Attempt $attempt: Restarting..."
      docker restart $container
      sleep $RETRY_DELAY
    fi
    if [ $attempt -eq $RETRY_ATTEMPTS ]; then
      die "$container not ready"
    fi
  done
done

log "Copying JARs to Flink lib..."
docker exec CDC-Flink-JobManager bash -lc "mkdir -p /opt/flink/lib && cp /flink/plugins/*.jar /opt/flink/lib/ || true"

log "Removing duplicate metrics..."
docker exec CDC-Flink-JobManager bash -lc "rm -f /opt/flink/lib/metrics-core*.jar || true"

log "Restarting Flink..."
docker restart CDC-Flink-JobManager

sleep 30

# Create topics
log "Creating topics..."
create_topic() {
  local t="$1"
  docker exec $SVC_KAFKA /opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.17.26.218:9092 --create --topic "$t" --partitions 1 --replication-factor 1 || true
}
create_topic "raw_json_account"
create_topic "raw_json_product"

sleep "${DELAY_BEFORE_SERVER}"

log "Starting Kafka Connect..."
docker exec -d $SVC_KAFKA /opt/kafka/bin/connect-standalone.sh /opt/kafka/config-cdc/connect-standalone.properties /opt/kafka/config-cdc/connect-postgres-source.json /opt/kafka/config-cdc/connect-iceberg-sink.json

log "Submitting PyFlink job..."
docker exec CDC-Flink-JobManager /opt/flink/bin/flink run -d -py /opt/flink/usrlib/flink_json_to_iceberg.py || die "Job failed"

echo "Setup complete. Flink UI: http://10.17.26.218:8081"