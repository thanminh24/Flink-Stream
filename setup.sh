#!/usr/bin/env bash
# setup.sh: Launch infrastructure and Connect after dependencies
set -euo pipefail

DEBUG="${DEBUG:-0}"  # default OFF
[[ "$DEBUG" == "1" ]] && set -x

log() { printf "\n=== %s ===\n" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SVC_KAFKA="CDC-Kafka"
DELAY_BEFORE_SERVER="${DELAY_BEFORE_SERVER:-20}"
DELAY_AFTER_STARTUP="${DELAY_AFTER_STARTUP:-30}"
RETRY_ATTEMPTS=10
RETRY_DELAY=15

log "Setup variables:"
echo "  ROOT_DIR: $ROOT_DIR"
echo "  SVC_KAFKA: $SVC_KAFKA"
echo "  DELAY_BEFORE_SERVER: $DELAY_BEFORE_SERVER"
echo "  DELAY_AFTER_STARTUP: $DELAY_AFTER_STARTUP"
echo "  RETRY_ATTEMPTS: $RETRY_ATTEMPTS"
echo "  RETRY_DELAY: $RETRY_DELAY"

# Clean Kafka metadata and Docker resources
log "Cleaning Kafka metadata and Docker resources..."
docker compose down -v --remove-orphans || true
rm -rf "${ROOT_DIR}/data/kafka/tmp" || true

# Start Docker Compose
log "Starting Docker Compose..."
docker compose up -d

# Wait for containers to stabilize
log "Waiting ${DELAY_AFTER_STARTUP}s for containers to stabilize..."
sleep "${DELAY_AFTER_STARTUP}"

# Check Kafka status
log "Checking Kafka container status..."
for attempt in $(seq 1 $RETRY_ATTEMPTS); do
  if docker ps --filter "name=^CDC-Kafka$" --format '{{.Status}}' | grep -q "Up"; then
    log "CDC-Kafka is running"
    break
  else
    log "Attempt $attempt/$RETRY_ATTEMPTS: CDC-Kafka not running, retrying in ${RETRY_DELAY}s..."
    docker compose restart CDC-Kafka
    sleep $RETRY_DELAY
  fi
  if [ $attempt -eq $RETRY_ATTEMPTS ]; then
    die "CDC-Kafka container is not running after $RETRY_ATTEMPTS attempts"
  fi
done

# Check JobManager status
log "Checking JobManager container status..."
for attempt in $(seq 1 $RETRY_ATTEMPTS); do
  if docker ps --filter "name=^CDC-Flink-JobManager$" --format '{{.Status}}' | grep -q "healthy"; then
    log "CDC-Flink-JobManager is healthy"
    break
  else
    log "Attempt $attempt/$RETRY_ATTEMPTS: CDC-Flink-JobManager not healthy, retrying in ${RETRY_DELAY}s..."
    docker compose restart CDC-Flink-JobManager
    sleep $RETRY_DELAY
  fi
  if [ $attempt -eq $RETRY_ATTEMPTS ]; then
    die "CDC-Flink-JobManager container is not healthy after $RETRY_ATTEMPTS attempts"
  fi
done

# Check TaskManager status
log "Checking TaskManager container status..."
for attempt in $(seq 1 $RETRY_ATTEMPTS); do
  if docker ps --filter "name=^CDC-Flink-TaskManager$" --format '{{.Status}}' | grep -q "Up"; then
    log "CDC-Flink-TaskManager is running"
    break
  else
    log "Attempt $attempt/$RETRY_ATTEMPTS: CDC-Flink-TaskManager not running, retrying in ${RETRY_DELAY}s..."
    docker compose restart CDC-Flink-TaskManager
    sleep $RETRY_DELAY
  fi
  if [ $attempt -eq $RETRY_ATTEMPTS ]; then
    die "CDC-Flink-TaskManager container is not running after $RETRY_ATTEMPTS attempts"
  fi
done

# Copy Flink JARs to /opt/flink/lib (classpath location)
log "Copying Flink JARs to /opt/flink/lib..."
docker compose exec CDC-Flink-JobManager bash -lc "mkdir -p /opt/flink/lib && cp /flink/plugins/*.jar /opt/flink/lib/ || true"
docker compose exec CDC-Flink-TaskManager bash -lc "mkdir -p /opt/flink/lib && cp /flink/plugins/*.jar /opt/flink/lib/ || true"

# Remove duplicate metrics JARs to avoid conflicts
log "Removing duplicate metrics JARs to avoid conflicts..."
docker compose exec CDC-Flink-JobManager bash -lc "rm -f /opt/flink/lib/metrics-core*.jar || true"
docker compose exec CDC-Flink-TaskManager bash -lc "rm -f /opt/flink/lib/metrics-core*.jar || true"

# Restart Flink containers to load new JARs
log "Restarting Flink containers to load new JARs..."
docker compose restart CDC-Flink-JobManager CDC-Flink-TaskManager

# Wait for restart to complete
sleep 30

# Verify KafkaSource exists in the SQL uber-jar
log "Verifying KafkaSource class is available in Flink lib..."
docker compose exec CDC-Flink-JobManager bash -lc \
  "if command -v jar >/dev/null 2>&1; then for j in /opt/flink/lib/*kafka*jar; do jar tf \"\$j\" | grep -q '^org/apache/flink/connector/kafka/source/KafkaSource.class$' && echo OK: \$(basename \"\$j\"); done; else echo 'jar command not found, skipping verification'; fi" || true

# Clean up target directory
log "Cleaning up target directory..."
rm -rf "${ROOT_DIR}/target" || true


# Create/validate Kafka topics
log "Creating/validating Kafka topics..."
create_topic() {
  local t="$1"
  docker compose exec "$SVC_KAFKA" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-standalone:19092 --list | grep -qx "$t" && return 0
  docker compose exec "$SVC_KAFKA" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-standalone:19092 \
    --create --topic "$t" --partitions 1 --replication-factor 1 || true
}
create_topic "raw_json_account"
create_topic "raw_json_product"
docker compose exec "$SVC_KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-standalone:19092 --list

# Start Kafka Connect
log "Waiting ${DELAY_BEFORE_SERVER}s before starting Connect..."
sleep "${DELAY_BEFORE_SERVER}"
log "Starting Kafka Connect (standalone)..."
docker compose exec -d "$SVC_KAFKA" \
  /opt/kafka/bin/connect-standalone.sh \
  /opt/kafka/config-cdc/connect-standalone.properties \
  /opt/kafka/config-cdc/connect-postgres-source.json \
  /opt/kafka/config-cdc/connect-iceberg-sink.json

# Start Flink job (detached)
log "Starting Flink and submitting PyFlink JSON-to-Iceberg job (detached)..."
docker compose exec CDC-Flink-JobManager /opt/flink/bin/flink run -d -py /opt/flink/usrlib/flink_json_to_iceberg.py || die "PyFlink job submission failed"

echo
echo "Tail logs:"
echo "  docker compose logs -f CDC-Kafka"
echo "  docker compose logs -f CDC-Flink-JobManager"
echo "  docker compose logs -f CDC-Flink-TaskManager"
echo
echo "Check status:"
echo "  curl -s http://localhost:8083/connectors | jq"
echo "  curl -s http://localhost:8083/connectors/dbz-pg-source/status | jq"
echo "  curl -s http://localhost:8083/connectors/iceberg-sink/status | jq"
echo "  Flink UI: http://localhost:8081"
echo "  To cancel job: docker compose exec CDC-Flink-JobManager /opt/flink/bin/flink cancel <JobID>"