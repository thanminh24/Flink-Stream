#!/usr/bin/env bash
set -euo pipefail

DEBUG="${DEBUG:-1}"  # Enable debug mode
[[ "$DEBUG" == "1" ]] && set -x

log() { printf "\n=== %s ===\n" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

RETRY_ATTEMPTS=30
RETRY_DELAY=15
CONTAINER_NAME="CDC-Flink-TaskManager"

log "Adding hostname to /etc/hosts..."
sudo bash -c 'echo "10.17.26.217 cdc-flink-taskmanager" >> /etc/hosts' || log "Warning: Failed to update /etc/hosts"
sudo bash -c 'echo "127.0.1.1 trino" >> /etc/hosts' || log "Warning: Failed to add host hostname to /etc/hosts"

docker compose down 
log "Starting Docker Compose..."
docker compose up -d || die "Failed to start Docker Compose"

sleep 30

log "Copying JARs..."
docker exec $CONTAINER_NAME bash -lc "mkdir -p /opt/flink/lib && cp /flink/plugins/*.jar /opt/flink/lib/ || true" || die "Failed to copy JARs"

log "Removing duplicate metrics..."
docker exec $CONTAINER_NAME bash -lc "rm -f /opt/flink/lib/metrics-core*.jar || true" || die "Failed to remove metrics JARs"

log "Restarting TaskManager..."
docker restart $CONTAINER_NAME || die "Failed to restart TaskManager"

echo "Setup complete. Check JobManager UI for registration: http://10.17.26.218:8081"