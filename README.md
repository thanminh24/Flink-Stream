# Postgres to Lakehouse CDC (Flink Standalone)

## Directory Structure
```
.
├── docker-compose.yaml
├── Dockerfile.flink
├── dependencies.sh
├── setup.sh
├── .env
├── pom.xml
├── postgres/
│   ├── postgresql.conf
│   └── scripts/
│       └── seed/
│           ├── 000_init.sql
│           └── 001_insert.sql
├── kafka/
│   ├── config/
│   │   ├── connect-standalone.properties
│   │   ├── connect-postgres-source.json
│   │   └── connect-iceberg-sink.json
│   └── plugins/            # Populated by dependencies.sh
├── flink/
│   ├── conf/
│   │   └── flink-conf.yaml
│   ├── jobs/
│   │   └── flink_json_to_iceberg.py
│   └── plugins/            # Populated by dependencies.sh
├── test_cdc.py
├── snapshot_mgmt.py
└── data/                   # Kafka tmp/out (ephemeral)
```

## Description
This proof of concept demonstrates change data capture (CDC) from PostgreSQL into a Lakehouse stack using Kafka, Debezium, and Apache Iceberg via the Iceberg REST catalog with MinIO/S3, with optional querying through Trino. It seeds a small `commerce` schema in Postgres, streams changes via Kafka Connect, and persists them as Iceberg tables. A PyFlink job is also included to ingest simple JSON topics into Iceberg.

Flink here runs in standalone mode via Docker Compose: one JobManager and one TaskManager container on the same host. There is no external resource manager (no Kubernetes/YARN) and no remote TaskManagers required.


## Installation

### Prerequisites
- Docker and Docker Compose v2
- Java 17 and Maven (for connector/runtime dependencies)
- Python 3.10 with `requests`
- zip/unzip installed

### Setup Guide
1) Resolve dependencies (Debezium, Iceberg, Flink connectors)
```bash
./dependencies.sh
```
2) Start services, connectors, and the PyFlink job
```bash
./setup.sh
```
3) Verify topics and connectors
```bash
docker exec CDC-Kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-standalone:19092 --list

curl -s http://localhost:8083/connectors | jq
curl -s http://localhost:8083/connectors/dbz-pg-source/status | jq
curl -s http://localhost:8083/connectors/iceberg-sink/status | jq
```
Expected output (abridged)
```
__consumer_offsets
cdc.commerce.account
cdc.commerce.product
raw_json_account
raw_json_product
[
  "dbz-pg-source",
  "iceberg-sink"
]
{ "name": "dbz-pg-source", "connector": {"state": "RUNNING"}, "type": "source" }
{ "name": "iceberg-sink",   "connector": {"state": "RUNNING"}, "type": "sink" }
```


## PostgreSQL Source Connector (Debezium)
Minimal example (see `kafka/config/connect-postgres-source.json` for the actual config):
```json
{
  "name": "dbz-pg-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "<POSTGRES_HOST>",
    "database.port": "5432",
    "database.user": "<POSTGRES_USER>",
    "database.password": "<POSTGRES_PASSWORD>",
    "database.dbname": "<POSTGRES_DB>",
    "topic.prefix": "cdc",
    "schema.include.list": "commerce",
    "heartbeat.interval.ms": "1000",
    "plugin.name": "pgoutput"
  }
}
```

## Iceberg Sink Connector
Minimal example (see `kafka/config/connect-iceberg-sink.json` for the actual config). The repo now uses the Iceberg REST catalog (no Hive Metastore):
```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "1",
    "topics.regex": "cdc.commerce.*",
    "transforms": "debezium",
    "transforms.debezium.type": "org.apache.iceberg.connect.transforms.DebeziumTransform",
    "transforms.debezium.cdc.target.pattern": "cdc.{table}_postgres",
    "iceberg.tables.route-field": "_cdc.target",
    "iceberg.tables.dynamic-enabled": "true",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.catalog": "iceberg",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://<REST_HOST>:8181/catalog",
    "iceberg.catalog.warehouse": "<WAREHOUSE_NAME_OR_PATH>",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "http://<MINIO_HOST>:9000",
    "iceberg.catalog.s3.access-key-id": "<S3_ACCESS_KEY>",
    "iceberg.catalog.s3.secret-access-key": "<S3_SECRET_KEY>",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.s3.region": "<S3_REGION>"
  }
}
```

Create connectors via REST if needed
```bash
curl -X POST -H "Content-Type: application/json" \
  --data @kafka/config/connect-postgres-source.json \
  http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" \
  --data @kafka/config/connect-iceberg-sink.json \
  http://localhost:8083/connectors
```


## Flink Streaming Ingest (JSON → Iceberg)
`flink/jobs/flink_json_to_iceberg.py` consumes `raw_json_account` and `raw_json_product` (raw JSON) and upserts into `cdc.account_json` and `cdc.product_json` (Iceberg v2, `write.upsert.enabled=true`). It:
- Creates an Iceberg catalog (`iceberg`) using the Iceberg REST catalog and MinIO/S3 FileIO (no Hive Metastore)
- Ensures database `cdc` exists and creates upsert tables
- Defines raw Kafka sources and parses fields with `JSON_VALUE(...)`
- Enables checkpointing (10s) and uses `/opt/flink/lib` jars configured via `pipeline.jars`

Flink UI: `http://localhost:8081`

Check job status (CLI and REST)
```bash
docker exec CDC-Flink-JobManager /opt/flink/bin/flink list -a
curl -s http://localhost:8081/jobs/overview | jq
curl -s http://localhost:8081/jobs/<jobId> | jq
```


## Usage
1) Insert test rows and push JSON to Kafka
```bash
python3 test_cdc.py
```
Note: `test_cdc.py` defaults to `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`. Override only if needed, e.g. when running inside a container on the Docker network: `KAFKA_BOOTSTRAP_SERVERS=kafka-standalone:19092 python3 test_cdc.py`.
Expected output (abridged)
```
Querying Postgres (before insert)...
Postgres account: [(1, 'alice@example.com', ...), (2, 'bob@example.com', ...)]
Postgres product: [(1, 'Live Edge Dining Table', ...), (2, 'Simple Teak Dining Chair', ...)]
Inserting test data into PostgreSQL...
Pushed JSON to Kafka: {"user_id": 1234, ...}, {"product_id": 5678, ...}
```
2) Query with Trino (optional)
```sql
SELECT * FROM iceberg.cdc.account_json ORDER BY ts DESC LIMIT 10;
SELECT * FROM iceberg.cdc.product_json ORDER BY ts DESC LIMIT 10;
```


## Files You May Customize (Environment)
Defaults are set for a single‑host, standalone setup. Adjust only if your endpoints differ. File:line references are included for convenience.
- `docker-compose.yaml:12` — Kafka `KAFKA_ADVERTISED_LISTENERS` (host `localhost:9092`, internal `kafka-standalone:19092`)
- `docker-compose.yaml:78,110` — Flink `JOB_MANAGER_RPC_ADDRESS=flink-jobmanager` (already set for standalone)
- `flink/conf/flink-conf.yaml:4` — `jobmanager.rpc.address: flink-jobmanager` (already set)
- `flink/conf/flink-conf.yaml:1,8` — Confirm ports `6124` and `6125` if changed
- `flink/jobs/flink_json_to_iceberg.py:31` — Catalog impl `org.apache.iceberg.rest.RESTCatalog`
- `flink/jobs/flink_json_to_iceberg.py:32` — REST `uri` (e.g., `http://<REST_HOST>:8181/catalog`)
- `flink/jobs/flink_json_to_iceberg.py:33` — Iceberg `warehouse` (e.g., a catalog namespace like `demo`)
- `flink/jobs/flink_json_to_iceberg.py:35` — MinIO/S3 `s3.endpoint`
- `flink/jobs/flink_json_to_iceberg.py:37` — `s3.access-key-id`
- `flink/jobs/flink_json_to_iceberg.py:38` — `s3.secret-access-key`
- `flink/jobs/flink_json_to_iceberg.py:102,129` — Kafka bootstrap (`kafka-standalone:19092`) for sources
- `kafka/config/connect-iceberg-sink.json:19` — `iceberg.catalog.uri` (REST)
- `kafka/config/connect-iceberg-sink.json:20` — `iceberg.catalog.warehouse`
- `kafka/config/connect-iceberg-sink.json:22-24` — MinIO/S3 endpoint and credentials
- `kafka/config/connect-iceberg-sink.json:28-29` — `id-columns` per table
- `setup.sh:119-126` — Kafka bootstrap used for topic creation
- `snapshot_mgmt.py:4` — Trino connection
- `test_cdc.py:1-40` — Postgres and Kafka defaults
- `.env` — MinIO and Trino endpoints/credentials (HMS vars are legacy and unused)

Snippets
```yaml
# docker-compose.yaml (Kafka advertised listeners; defaults for standalone)
environment:
  - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT_HOST://localhost:9092,PLAINTEXT://kafka-standalone:19092
```
```yaml
# flink/conf/flink-conf.yaml (standalone)
jobmanager.rpc.address: flink-jobmanager
blob.server.port: 6124
query.server.port: 6125
```
```python
# flink/jobs/flink_json_to_iceberg.py (catalog excerpt)
catalog_ddl = """
  CREATE CATALOG IF NOT EXISTS iceberg WITH (
    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
    'uri' = 'http://<REST_HOST>:8181/catalog',
    'warehouse' = '<WAREHOUSE_NAME>',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://<MINIO_HOST>:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = '<S3_ACCESS_KEY>',
    's3.secret-access-key' = '<S3_SECRET_KEY>',
    'aws.region' = '<S3_REGION>'
  )
"""
```



## Troubleshooting
- Kafka client connection issues: check `KAFKA_ADVERTISED_LISTENERS` and `localhost:9092` (host) or `kafka-standalone:19092` (in-network)
- Flink missing jars: re-run `./dependencies.sh` then `./setup.sh` to copy jars into `/opt/flink/lib`
- REST catalog or MinIO unreachable: verify endpoints and container network reachability
- Port conflicts: Postgres `5433` on host; Flink UI `8081`; Kafka `9092`


## License
No license specified. Treat as internal/demo unless a license is added.


## Function Reference (Scripts)

test_cdc.py
- Purpose: seed/query Postgres and publish JSON to Kafka for the Flink job.
- Env vars: `POSTGRES_HOST` (default `localhost`), `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`).
- get_pg_connection(): opens a psycopg2 connection using env vars; returns a connection; exits on failure.
- query_pg(sql_query: str) -> list[tuple]: executes a SELECT; returns rows; on error prints a warning and returns `[]`.
- insert_pg(sql_insert: str) -> None: executes INSERT/DDL; commits; errors are logged as warnings.
- insert_test_rows(bulk: bool=False) -> tuple[list[str], list[str]]: inserts 1 or 100 rows into `commerce.account` and `commerce.product`; returns generated emails and product names.
- produce_json_to_kafka(emails: list[str], products: list[str]) -> list[tuple[int,int,str]]: publishes to `raw_json_account` and `raw_json_product`; returns a list of `(user_id, product_id, timestamp)` for the sent messages.
- main(): parses `--bulk`; prints pre-insert query results; inserts rows; produces JSON to Kafka.
Example
```bash
python3 test_cdc.py --bulk
```

setup.sh
- Purpose: bring up containers, verify health, copy Flink jars, create topics, start Connect, submit the PyFlink job.
- Key helpers: `log(msg)` prints section headers; `die(msg)` exits non‑zero; `create_topic(name)` creates a topic via `kafka-topics.sh` using the external bootstrap server.
- Flow: `docker compose up -d` → health checks (Kafka, Flink JM) → copy `/flink/plugins/*.jar` to `/opt/flink/lib` → restart JM → create `raw_json_*` topics → start Connect standalone with the source and sink JSONs → submit PyFlink job.
Example
```bash
DEBUG=1 ./setup.sh
# After it completes, Flink UI should be up on http://localhost:8081
```

dependencies.sh
- Purpose: resolve and stage Debezium, Iceberg, Hadoop (no Hive), and Flink connectors via Maven, then install into `kafka/plugins/` and `flink/plugins/`.
- Requirements: Java 17, Maven, zip/unzip.
- Behavior: cleans plugin dirs → `mvn clean generate-resources` → unzip Debezium plugin → copy jars → remove conflicting SLF4J bindings from `iceberg-aws-bundle` → install Flink and shared jars → sanity‑check required classes (Iceberg RESTCatalog, S3FileIO, KafkaSource, etc.).
Example
```bash
DEBUG=1 ./dependencies.sh   # verbose mode
```

flink/jobs/flink_json_to_iceberg.py
- Purpose: PyFlink streaming job that ingests raw JSON from Kafka into Iceberg tables with upsert semantics.
- Behavior: configures `pipeline.jars`; creates Iceberg catalog (REST + S3/MinIO); ensures DB `cdc`; creates `account_json` and `product_json` with `write.upsert.enabled=true`; defines temporary tables for topics `raw_json_account` and `raw_json_product`; parses fields via `JSON_VALUE`; runs two INSERT…SELECT streams; logs Flink job IDs.
- Notable APIs: `StreamExecutionEnvironment`, `StreamTableEnvironment.execute_sql`, SQL `JSON_VALUE`, JobClient `get_job_id`.
Example
```bash
docker exec CDC-Flink-JobManager /opt/flink/bin/flink run -d -py /opt/flink/usrlib/flink_json_to_iceberg.py
```

snapshot_mgmt.py
- Purpose: maintenance helper to expire Iceberg snapshots via Trino.
- Behavior: connects to Trino (`trino.dbapi.connect`), sets session `iceberg.expire_snapshots_min_retention`, lists base tables in `cdc`, executes `ALTER TABLE ... EXECUTE expire_snapshots` per table, then closes the connection.
Example
```bash
python3 snapshot_mgmt.py   # adjust host/port in the script first
```


## Reference Links

- Debezium PostgreSQL connector
  - https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- Kafka Connect (standalone worker + REST)
  - https://kafka.apache.org/documentation/#connect
  - REST API: https://docs.confluent.io/platform/current/connect/references/restapi.html
- Apache Iceberg
  - Kafka Connect sink: https://iceberg.apache.org/docs/latest/kafka-connect/
  - REST catalog spec: https://iceberg.apache.org/rest-catalog-spec/
  - AWS S3 FileIO: https://iceberg.apache.org/docs/latest/aws/
- Apache Flink
  - PyFlink Table API: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/
  - SQL JSON functions (`JSON_VALUE`): https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/functions/systemfunctions/#json-functions
  - Kafka SQL connector: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/kafka/
  - REST API (jobs): https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/rest_api/#jobs
- Trino Iceberg
  - https://trino.io/docs/current/connector/iceberg.html
- Libraries
  - kafka-python: https://kafka-python.readthedocs.io/en/master/
  - psycopg2: https://www.psycopg.org/docs/
  - Python `requests`: https://requests.readthedocs.io/
