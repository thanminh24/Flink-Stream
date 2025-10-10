#!/usr/bin/env bash
# dependencies.sh: Resolve dependencies for Debezium, Iceberg, and Flink using Maven
# Usage: ./dependencies.sh
set -euo pipefail

DEBUG="${DEBUG:-0}"
[[ "$DEBUG" == "1" ]] && set -x

log() { printf "\n=== %s ===\n" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# ---- Paths ----
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGINS_DIR="${ROOT_DIR}/kafka/plugins"
DBZ_PLUGIN_DIR="${PLUGINS_DIR}/debezium-connector-postgres"
IC_PLUGIN_DIR="${PLUGINS_DIR}/iceberg-kafka-connect"
FLINK_PLUGIN_DIR="${ROOT_DIR}/flink/plugins"
STAGE_ROOT="${ROOT_DIR}/target/dependencies"
STAGE_DBZ="${STAGE_ROOT}/debezium-subfolder"
STAGE_IC="${STAGE_ROOT}/iceberg-subfolder"
STAGE_FLINK="${STAGE_ROOT}/flink-subfolder"
STAGE_SHARED="${STAGE_ROOT}/shared-subfolder"

# ---- Maven flags ----
if [[ "$DEBUG" == "1" ]]; then
  MVN=(mvn -U -e -X -DskipTests)
else
  MVN=(mvn -U -B -q -DskipTests)
fi

# ---- Clean & prep ----
log "Cleaning directories..."
rm -rf "${ROOT_DIR}/kafka/plugins" "${ROOT_DIR}/flink/plugins" "${ROOT_DIR}/target" || true
mkdir -p "${DBZ_PLUGIN_DIR}" "${IC_PLUGIN_DIR}" "${FLINK_PLUGIN_DIR}" "${STAGE_DBZ}" "${STAGE_IC}" "${STAGE_FLINK}" "${STAGE_SHARED}"

log "Java & Maven versions"
(java -version || true) 2>&1
(mvn -version || true) 2>&1

# ---- Resolve all deps via Maven ----
log "Resolving and copying all dependencies via Maven..."
"${MVN[@]}" clean generate-resources

# ---- Debug: List staged files ----
log "Listing files in STAGE_IC (${STAGE_IC})"
ls -l "${STAGE_IC}" || true
log "Listing files in STAGE_FLINK (${STAGE_FLINK})"
ls -l "${STAGE_FLINK}" || true
log "Listing files in STAGE_SHARED (${STAGE_SHARED})"
ls -l "${STAGE_SHARED}" || true

# ---- Post-processing ----
# Extract Debezium plugin ZIP
DBZ_ZIP="$(ls -1 "${STAGE_DBZ}"/debezium-connector-postgres-*plugin.zip | head -n1 || true)"
[[ -n "${DBZ_ZIP}" ]] || die "Debezium plugin ZIP not found in ${STAGE_DBZ}"
log "Extract Debezium plugin"
if command -v unzip >/dev/null 2>&1; then unzip -q -o -d "${STAGE_DBZ}" "${DBZ_ZIP}"; else ( cd "${STAGE_DBZ}" && jar xf "${DBZ_ZIP}" ); fi
log "Install Debezium jars -> ${DBZ_PLUGIN_DIR}"
find "${STAGE_DBZ}" -type f -name "*.jar" -print0 | xargs -0 -I{} cp -f "{}" "${DBZ_PLUGIN_DIR}/" || true

# Modify iceberg-aws-bundle to remove conflicting SLF4J bindings for Connect
log "Modifying iceberg-aws-bundle for Connect..."
command -v zip >/dev/null 2>&1 || die "zip command not found; install it (e.g., 'dnf install zip' on Red Hat)"
cd "${STAGE_IC}"
zip -d iceberg-aws-bundle-*.jar 'org/slf4j/impl/*' || true
cd -

# Install Iceberg + Shared to Connect plugin dir
log "Install Iceberg + Shared jars -> ${IC_PLUGIN_DIR}"
find "${IC_PLUGIN_DIR}" -type f -name "*.jar" -delete || true
find "${STAGE_IC}" -type f -name "*.jar" -print0 | xargs -0 -I{} cp -f "{}" "${IC_PLUGIN_DIR}/" || true
find "${STAGE_SHARED}" -type f -name "*.jar" -print0 | xargs -0 -I{} cp -f "{}" "${IC_PLUGIN_DIR}/" || true

# Modify iceberg-aws-bundle to remove conflicting SLF4J bindings for Flink
log "Modifying iceberg-aws-bundle for Flink..."
cd "${STAGE_FLINK}"
zip -d iceberg-aws-bundle-*.jar 'org/slf4j/impl/*' || true
cd -

# Install Flink deps
log "Install Flink jars -> ${FLINK_PLUGIN_DIR}"
find "${FLINK_PLUGIN_DIR}" -type f -name "*.jar" -delete || true
find "${STAGE_FLINK}" -type f -name "*.jar" -print0 | xargs -0 -I{} cp -f "{}" "${FLINK_PLUGIN_DIR}/" || true
find "${STAGE_SHARED}" -type f -name "*.jar" -print0 | xargs -0 -I{} cp -f "{}" "${FLINK_PLUGIN_DIR}/" || true

# Remove conflicting Log4j
log "Removing conflicting Log4j..."
find "${FLINK_PLUGIN_DIR}" -type f -name "log4j-*-2.25.1.jar" -delete || true  # Hardcoded version

# Remove conflicting metrics-core JARs
log "Removing conflicting metrics-core JARs..."
find "${STAGE_FLINK}" -type f -name "metrics-core*.jar" -delete || true
find "${FLINK_PLUGIN_DIR}" -type f -name "metrics-core*.jar" -delete || true

# ---- Sanity-check staged jars (Hive/HMS checks removed) ----
log "C3) Sanity-check staged jars"
CLASS_HTTP_METHOD='org/apache/hc/core5/http/Method.class'
CLASS_HTTP_SDKEXC='org/apache/hc/client5/http/impl/async/CloseableHttpAsyncClient.class'  # From httpclient5
CLASS_REST_CATALOG='org/apache/iceberg/rest/RESTCatalog.class'  # Iceberg REST core
CLASS_REST_ENDPOINT='org/apache/iceberg/rest/Endpoint.class'    # Iceberg REST endpoint
CLASS_S3FILEIO='org/apache/iceberg/aws/s3/S3FileIO.class'
CLASS_SDKEXC='software/amazon/awssdk/core/exception/SdkException.class'
CLASS_S3EXC='software/amazon/awssdk/services/s3/model/S3Exception.class'
CLASS_JOBCONF='org/apache/hadoop/mapred/JobConf.class'
CLASS_PARQUET='org/apache/iceberg/parquet/Parquet.class'
CLASS_ORC='org/apache/iceberg/orc/ORC.class'
CLASS_AVRO='org/apache/avro/Schema.class'
CLASS_PARQUET_WRITER='org/apache/iceberg/parquet/ParquetValueWriter.class'
CLASS_HADOOP_CONF='org/apache/hadoop/conf/Configuration.class'
CLASS_HDFS_CONF='org/apache/hadoop/hdfs/HdfsConfiguration.class'
CLASS_KAFKA_CLIENTS='org/apache/kafka/clients/consumer/KafkaConsumer.class'
CLASS_LOG4J_SLF4J='org/apache/logging/slf4j/Log4jLoggerFactory.class'
CLASS_WOODSTOX='com/ctc/wstx/io/InputBootstrapper.class'
CLASS_STAX2_API='org/codehaus/stax2/XMLInputFactory2.class'
CLASS_SHADED_GUAVA='org/apache/hadoop/thirdparty/com/google/common/collect/Interners.class'
CLASS_COMMONS_CONF2='org/apache/commons/configuration2/Configuration.class'
CLASS_PLATFORMNAME='org/apache/hadoop/util/PlatformName.class'
CLASS_KAFKA_SOURCE='org/apache/flink/connector/kafka/source/KafkaSource.class'
CLASS_FAILSAFE='dev/failsafe/FailsafeException.class'
CLASS_STRING_UTILS='org/apache/commons/lang/StringUtils.class'

find_one_with() {
  local c="$1" dir="${2:-${STAGE_SHARED}}"
  shopt -s nullglob
  for j in "${dir}"/*.jar; do
    if command -v jar >/dev/null 2>&1; then
      jar tf "$j" | grep -q "$c" && { echo "$j"; return 0; }
    else
      echo "jar command not found, skipping verification for $c in $j" >&2
    fi
  done
  return 1
}

find_one_any() {
  local c="$1"; shift
  local hit=""
  for d in "$@"; do
    hit="$(find_one_with "$c" "$d" || true)"
    [[ -n "$hit" ]] && { echo "$hit"; return 0; }
  done
  return 1
}

J_HTTP_METHOD="$(find_one_any "$CLASS_HTTP_METHOD" "${STAGE_IC}" "${STAGE_SHARED}" "${IC_PLUGIN_DIR}" || true)"
J_HTTP_SDKEXC="$(find_one_any "$CLASS_HTTP_SDKEXC" "${STAGE_IC}" "${STAGE_SHARED}" "${IC_PLUGIN_DIR}" || true)"
J_REST_CATALOG="$(find_one_any "$CLASS_REST_CATALOG" "${STAGE_IC}" "${IC_PLUGIN_DIR}" || true)"
J_REST_ENDPOINT="$(find_one_any "$CLASS_REST_ENDPOINT" "${STAGE_IC}" "${IC_PLUGIN_DIR}" || true)"
J_S3="$(find_one_any "$CLASS_S3FILEIO" "${STAGE_FLINK}" "${STAGE_IC}" "${STAGE_SHARED}" "${IC_PLUGIN_DIR}" "${FLINK_PLUGIN_DIR}" || true)"
J_SK="$(find_one_any "$CLASS_SDKEXC" "${STAGE_IC}" "${STAGE_FLINK}" "${FLINK_PLUGIN_DIR}" || true)"
J_S3E="$(find_one_any "$CLASS_S3EXC" "${STAGE_IC}" "${STAGE_FLINK}" "${FLINK_PLUGIN_DIR}" || true)"
J_JC="$(find_one_with "$CLASS_JOBCONF" || true)"
J_PQ="$(find_one_with "$CLASS_PARQUET" "${STAGE_IC}" || true)"
J_ORC="$(find_one_with "$CLASS_ORC" "${STAGE_IC}" || true)"
J_AVRO="$(find_one_with "$CLASS_AVRO" || true)"
J_PQ_WRITER="$(find_one_with "$CLASS_PARQUET_WRITER" "${STAGE_IC}" || true)"
J_HADOOP_CONF="$(find_one_with "$CLASS_HADOOP_CONF" || true)"
J_HDFS_CONF="$(find_one_with "$CLASS_HDFS_CONF" || true)"
J_KAFKA_CLIENTS="$(find_one_with "$CLASS_KAFKA_CLIENTS" || true)"
J_LOG4J_SLF4J="$(find_one_with "$CLASS_LOG4J_SLF4J" || true)"
J_WOODSTOX="$(find_one_with "$CLASS_WOODSTOX" || true)"
J_STAX2_API="$(find_one_with "$CLASS_STAX2_API" || true)"
J_SHADED_GUAVA="$(find_one_with "$CLASS_SHADED_GUAVA" || true)"
J_COMMONS_CONF2="$(find_one_with "$CLASS_COMMONS_CONF2" || true)"
J_PLATFORMNAME="$(find_one_any "$CLASS_PLATFORMNAME" "${STAGE_SHARED}" "${FLINK_PLUGIN_DIR}" "${IC_PLUGIN_DIR}" || true)"
J_KAFKA_SOURCE="$(find_one_any "$CLASS_KAFKA_SOURCE" "${STAGE_FLINK}" "${FLINK_PLUGIN_DIR}" || true)"
J_FAILSAFE="$(find_one_with "$CLASS_FAILSAFE" "${STAGE_SHARED}" || true)"
J_STRING_UTILS="$(find_one_with "$CLASS_STRING_UTILS" "${STAGE_SHARED}" || true)"

[[ -n "$J_HTTP_METHOD" ]] || die "Missing httpcore5 Method ($CLASS_HTTP_METHOD)"
[[ -n "$J_HTTP_SDKEXC" ]] || die "Missing httpclient5 CloseableHttpAsyncClient ($CLASS_HTTP_SDKEXC)"
[[ -n "$J_REST_CATALOG" ]] || die "Missing Iceberg RESTCatalog ($CLASS_REST_CATALOG)"
[[ -n "$J_REST_ENDPOINT" ]] || die "Missing Iceberg REST Endpoint ($CLASS_REST_ENDPOINT)"
[[ -n "$J_S3" ]] || die "Missing S3FileIO ($CLASS_S3FILEIO)"
[[ -n "$J_SK" ]] || die "Missing AWS SDK core ($CLASS_SDKEXC)"
[[ -n "$J_S3E" ]] || die "Missing AWS S3Exception ($CLASS_S3EXC)"
[[ -n "$J_JC" ]] || die "Missing Hadoop JobConf ($CLASS_JOBCONF)"
[[ -n "$J_PQ" ]] || die "Missing Iceberg Parquet ($CLASS_PARQUET)"
[[ -n "$J_ORC" ]] || die "Missing Iceberg ORC ($CLASS_ORC)"
[[ -n "$J_AVRO" ]] || die "Missing Avro for Iceberg ($CLASS_AVRO)"
[[ -n "$J_PQ_WRITER" ]] || die "Missing ParquetValueWriter ($CLASS_PARQUET_WRITER)"
[[ -n "$J_HADOOP_CONF" ]] || die "Missing Hadoop Configuration ($CLASS_HADOOP_CONF)"
[[ -n "$J_HDFS_CONF" ]] || die "Missing HdfsConfiguration ($CLASS_HDFS_CONF)"
[[ -n "$J_KAFKA_CLIENTS" ]] || die "Missing Kafka Clients ($CLASS_KAFKA_CLIENTS)"
[[ -n "$J_LOG4J_SLF4J" ]] || die "Missing Log4j SLF4J ($CLASS_LOG4J_SLF4J)"
[[ -n "$J_WOODSTOX" ]] || die "Missing Woodstox InputBootstrapper ($CLASS_WOODSTOX)"
[[ -n "$J_STAX2_API" ]] || die "Missing Stax2 XMLInputFactory2 ($CLASS_STAX2_API)"
[[ -n "$J_SHADED_GUAVA" ]] || die "Missing Hadoop shaded-Guava ($CLASS_SHADED_GUAVA)"
[[ -n "$J_COMMONS_CONF2" ]] || die "Missing commons-configuration2 ($CLASS_COMMONS_CONF2)"
[[ -n "$J_PLATFORMNAME" ]] || die "Missing Hadoop PlatformName ($CLASS_PLATFORMNAME)"
[[ -n "$J_KAFKA_SOURCE" ]] || die "Missing Flink KafkaSource ($CLASS_KAFKA_SOURCE)"
[[ -n "$J_FAILSAFE" ]] || die "Missing Failsafe ($CLASS_FAILSAFE)"
[[ -n "$J_STRING_UTILS" ]] || die "Missing Commons Lang StringUtils ($CLASS_STRING_UTILS)"

log "Installed plugin dirs:"
echo "  - Debezium -> ${DBZ_PLUGIN_DIR}"
echo "  - Iceberg  -> ${IC_PLUGIN_DIR}"
echo "  - Flink    => ${FLINK_PLUGIN_DIR}"
