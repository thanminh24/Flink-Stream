# Postgres → Iceberg CDC (Flink Distributed, Iceberg REST)

This repository demonstrates change data capture (CDC) from PostgreSQL into Apache Iceberg using Kafka + Debezium, with a PyFlink streaming job for JSON ingestion. The deployment is distributed: a Flink JobManager runs alongside Kafka/Connect on one machine, and one or more Flink TaskManagers run on separate machines.

Key update: The project has migrated from Hive Metastore to the Iceberg REST catalog. All Hive-related dependencies and configs are removed; both the Kafka Connect sink and the PyFlink job now talk to Iceberg via REST and write to S3/MinIO using Iceberg’s S3 FileIO.

## Repository Layout

Flink Job (JobManager side)
- Purpose: Runs Kafka, Debezium source + Iceberg sink (Kafka Connect), Flink JobManager, and the PyFlink job.
- Path: `Flink Job/`
- Notable:
  - `Flink Job/docker-compose.yaml`: Postgres, Kafka (+Connect), Flink JobManager.
  - `Flink Job/dependencies.sh`: Stages Debezium, Iceberg, Flink runtime jars into `kafka/plugins/` and `flink/plugins/`.
  - `Flink Job/setup.sh`: Boots containers, copies jars into `/opt/flink/lib`, creates topics, starts Connect, submits the PyFlink job.
  - `Flink Job/flink/jobs/flink_json_to_iceberg.py`: PyFlink streaming job (JSON → Iceberg v2, upsert) using Iceberg REST.
  - `Flink Job/kafka/config/*.json`: Kafka Connect source/sink configs (Iceberg sink uses REST catalog).
  - `Flink Job/postgres/`: Seeds a small `commerce` schema for CDC.

Flink Task (TaskManager side)
- Purpose: Starts a Flink TaskManager that joins the remote JobManager for distributed execution.
- Path: `Flink Task/`
- Notable:
  - `Flink Task/docker-compose.yaml`: TaskManager container (use on the remote host).
  - `Flink Task/dependencies.sh`: Stages the same Flink/Iceberg runtime jars for the TM container.
  - `Flink Task/setup.sh`: Starts the TM and copies jars into `/opt/flink/lib`.

Optional helpers (in `Flink Job/`)
- `Flink Job/test_cdc.py`: Inserts sample rows into Postgres and produces JSON to Kafka.
- `Flink Job/snapshot_mgmt.py`: Example for expiring Iceberg snapshots via Trino.

## Prerequisites
- Docker + Docker Compose v2
- Java 17 and Maven (to stage connectors/runtime jars)
- Python 3.10 (PyFlink and scripts)
- `zip` and `unzip` installed
- Iceberg REST catalog endpoint reachable, for example `http://<REST_HOST>:8181/catalog`
- S3 or MinIO endpoint reachable for the warehouse (e.g., `http://<S3_HOST>:9000`)

## Quick Start

1) Stage dependencies (both sides)
- On the JobManager host:
  - `cd "Flink Job" && ./dependencies.sh`
- On each TaskManager host:
  - `cd "Flink Task" && ./dependencies.sh`

2) Configure addresses
- Ensure the JobManager IP is set consistently:
  - `Flink Job/docker-compose.yaml:75` sets `JOB_MANAGER_RPC_ADDRESS`.
  - `Flink Job/flink/conf/flink-conf.yaml:4` sets `jobmanager.rpc.address`.
  - `Flink Task/docker-compose.yaml:10` sets `JOB_MANAGER_RPC_ADDRESS` for the TM.
  - `Flink Task/flink/conf/flink-conf.yaml:3` sets `jobmanager.rpc.address`.
- Set Kafka’s advertised listeners to your host IP:
  - `Flink Job/docker-compose.yaml:36` (`KAFKA_ADVERTISED_LISTENERS=PLAINTEXT_HOST://<HOST_IP>:9092,...`).
- Update Iceberg REST and S3 settings in both the PyFlink job and the Connect sink:
  - `Flink Job/flink/jobs/flink_json_to_iceberg.py:41` (`uri`), `:42` (`warehouse`), `:44-47` (S3 endpoint/creds).
  - `Flink Job/kafka/config/connect-iceberg-sink.json:19-27` (REST `uri`, `warehouse`, S3 configs).
- Update Kafka bootstrap in the PyFlink job if needed:
  - `Flink Job/flink/jobs/flink_json_to_iceberg.py:117,145`.

3) Start the JobManager side
```bash
cd "Flink Job"
./setup.sh
```
This starts Postgres, Kafka (+Connect), and Flink JobManager; copies jars to `/opt/flink/lib`; creates topics; starts the Debezium source and Iceberg sink; and submits the PyFlink job.

4) Start one or more TaskManagers (on remote hosts)
```bash
cd "Flink Task"
./setup.sh
```
This starts a TaskManager container, copies jars to `/opt/flink/lib`, and registers it with the JobManager.

5) Verify
- Kafka topics:
  - `docker exec CDC-Kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server <HOST_IP>:9092 --list`
- Connectors:
  - `curl -s http://<HOST_IP>:8083/connectors | jq`
  - `curl -s http://<HOST_IP>:8083/connectors/dbz-pg-source/status | jq`
  - `curl -s http://<HOST_IP>:8083/connectors/iceberg-sink/status | jq`
- Flink UI:
  - `http://<JOB_MANAGER_HOST>:8081`

6) Ingest sample data
```bash
cd "Flink Job"
KAFKA_BOOTSTRAP_SERVERS=<HOST_IP>:9092 python3 test_cdc.py
```
This inserts Postgres rows and produces JSON to `raw_json_*` topics that the PyFlink job upserts into Iceberg.


## PostgreSQL Source Connector (Debezium)
Minimal example (see `Flink Job/kafka/config/connect-postgres-source.json` for the actual config):
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

## Iceberg Sink Connector (Iceberg REST)
Minimal example (see `Flink Job/kafka/config/connect-iceberg-sink.json` for the actual config):
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
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.warehouse": "<WAREHOUSE_NAME>",
    "iceberg.catalog.s3.endpoint": "http://<MINIO_OR_S3_HOST>:9000",
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
  --data @"Flink Job/kafka/config/connect-postgres-source.json" \
  http://<HOST_IP>:8083/connectors
curl -X POST -H "Content-Type: application/json" \
  --data @"Flink Job/kafka/config/connect-iceberg-sink.json" \
  http://<HOST_IP>:8083/connectors
```


## Flink Streaming Ingest (JSON → Iceberg)
`Flink Job/flink/jobs/flink_json_to_iceberg.py` consumes `raw_json_account` and `raw_json_product` and upserts into `cdc.account_json` and `cdc.product_json` (Iceberg v2, `write.upsert.enabled=true`). It:
- Creates an Iceberg catalog (`iceberg`) via Iceberg REST + S3/MinIO FileIO
- Ensures database `cdc` exists and creates upsert tables
- Defines raw Kafka sources and parses fields with `JSON_VALUE(...)`
- Enables checkpointing (10s) and uses `/opt/flink/lib` jars configured via `pipeline.jars`

Flink UI: `http://<JOB_MANAGER_HOST>:8081`

Distributed TaskManager
- Use Flink 1.20.2 (Java 17) to match the JobManager image
- Ensure the TM has the same jars under `/opt/flink/lib` (from `Flink Task/dependencies.sh`)
- TM `flink/conf/flink-conf.yaml` must set `jobmanager.rpc.address: <JOB_MANAGER_HOST>` and have access to ports 6124/6125
- TM must reach Kafka (`<HOST_IP>:9092`), the Iceberg REST endpoint, and the S3/MinIO endpoint

Check job status (CLI and REST)
```bash
docker exec CDC-Flink-JobManager /opt/flink/bin/flink list -a
curl -s http://<JOB_MANAGER_HOST>:8081/jobs/overview | jq
curl -s http://<JOB_MANAGER_HOST>:8081/jobs/<jobId> | jq
```


## Usage
1) Insert test rows and push JSON to Kafka
```bash
cd "Flink Job"
KAFKA_BOOTSTRAP_SERVERS=<HOST_IP>:9092 python3 test_cdc.py
```
Expected output (abridged)
```
Querying Postgres (before insert)...
Postgres account: [...]
Postgres product: [...]
Inserting test data into PostgreSQL...
Pushed JSON to Kafka: {"user_id": 1234, ...}, {"product_id": 5678, ...}
```
2) Query with Trino (optional)
```sql
SELECT * FROM iceberg.cdc.account_json ORDER BY ts DESC LIMIT 10;
SELECT * FROM iceberg.cdc.product_json ORDER BY ts DESC LIMIT 10;
```


## Files You Must Update (Environment)
Update these to match your environment (IPs, endpoints, credentials). File:line references are included for convenience.
- `Flink Job/docker-compose.yaml:36` — `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT_HOST://<HOST_IP>:9092,...`
- `Flink Job/docker-compose.yaml:75` — `JOB_MANAGER_RPC_ADDRESS=<JOB_MANAGER_HOST>`
- `Flink Job/flink/conf/flink-conf.yaml:4` — `jobmanager.rpc.address: <JOB_MANAGER_HOST>`
- `Flink Job/flink/conf/flink-conf.yaml:1` — `blob.server.port: 6124` (ensure reachable)
- `Flink Job/flink/conf/flink-conf.yaml:8` — `query.server.port: 6125` (ensure reachable)
- `Flink Job/flink/jobs/flink_json_to_iceberg.py:41-49` — Iceberg REST `uri`, `warehouse`, S3 endpoint/creds
- `Flink Job/flink/jobs/flink_json_to_iceberg.py:117,145` — Kafka bootstrap for sources
- `Flink Job/kafka/config/connect-iceberg-sink.json:19-27` — Iceberg REST `uri`, `warehouse`, S3 configs
- `Flink Task/docker-compose.yaml:10` — `JOB_MANAGER_RPC_ADDRESS=<JOB_MANAGER_HOST>`
- `Flink Task/flink/conf/flink-conf.yaml:3` — `jobmanager.rpc.address: <JOB_MANAGER_HOST>`



## Troubleshooting
- Kafka client connection issues: check `KAFKA_ADVERTISED_LISTENERS` and `<HOST_IP>:9092` reachability.
- Flink missing jars: re-run the relevant `dependencies.sh`, then `setup.sh` to copy jars into `/opt/flink/lib`.
- Iceberg REST/MinIO unreachable: verify the REST `uri` and S3 endpoint credentials; ensure network reachability from both JM and TMs.
- Port conflicts: Postgres `5433` on host; Flink UI `8081`; Kafka `9092`.


## License
No license specified. Treat as internal/demo unless a license is added.


## Function Reference (Scripts)

Flink Job/test_cdc.py
- Purpose: seed/query Postgres and publish JSON to Kafka for the Flink job.
- Env vars: `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `KAFKA_BOOTSTRAP_SERVERS`.
- Example: `KAFKA_BOOTSTRAP_SERVERS=<HOST_IP>:9092 python3 test_cdc.py --bulk`

Flink Job/setup.sh
- Purpose: bring up containers, verify health, copy Flink jars, create topics, start Connect, submit the PyFlink job.
- Example: `DEBUG=1 ./setup.sh`

Flink Job/dependencies.sh and Flink Task/dependencies.sh
- Purpose: resolve and stage Debezium/Iceberg/Flink jars; verifies presence of `RESTCatalog`, S3 FileIO, Kafka clients, etc.
- Example: `DEBUG=1 ./dependencies.sh`

Flink Job/flink/jobs/flink_json_to_iceberg.py
- Purpose: PyFlink job to upsert JSON into Iceberg tables via REST catalog.
- Example: `docker exec CDC-Flink-JobManager /opt/flink/bin/flink run -d -py /opt/flink/usrlib/flink_json_to_iceberg.py`

Flink Job/snapshot_mgmt.py
- Purpose: expire Iceberg snapshots via Trino (optional maintenance).


## Reference Links

- Debezium PostgreSQL connector
  - https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- Kafka Connect (standalone worker + REST)
  - https://kafka.apache.org/documentation/#connect
  - REST API: https://docs.confluent.io/platform/current/connect/references/restapi.html
- Apache Iceberg
  - Kafka Connect sink: https://iceberg.apache.org/docs/latest/kafka-connect/
  - REST catalog: https://iceberg.apache.org/docs/latest/rest/
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
