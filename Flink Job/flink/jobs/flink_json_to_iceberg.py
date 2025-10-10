from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
import json
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup environment (local state with checkpoints)
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)  # Sequential processing
env.enable_checkpointing(10000)  # 10s interval for commits
logger.info("Initialized StreamExecutionEnvironment with parallelism 1 and checkpointing every 10s")

# Create table environment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
logger.info("Created StreamTableEnvironment in streaming mode")

# Set table configuration
table_config = t_env.get_config()
table_config.set("pipeline.jars", 
    "file:///opt/flink/lib/flink-connector-kafka-3.4.0-1.20.jar;"
    "file:///opt/flink/lib/iceberg-flink-runtime-1.20-1.9.2.jar;"
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar")
logger.info("Set pipeline.jars for Kafka and Iceberg connectors")

# Set state TTL for post-insert flushing
table_config.set("table.exec.state.ttl", "600s")  # 10 minutes
logger.info("Set state TTL to 600s")

# Create catalog with your specified S3 warehouse
print("Creating Iceberg catalog...")
catalog_ddl = """
    CREATE CATALOG IF NOT EXISTS iceberg WITH (
        'type' = 'iceberg',
        'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
        'uri' = 'http://10.17.26.218:8181/catalog',
        'warehouse' = 'demo',
        'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
        's3.endpoint' = 'http://10.17.26.218:9000',
        's3.path-style-access' = 'true',
        's3.access-key-id' = 'minioadmin',
        's3.secret-access-key' = 'minioadmin123',
        'aws.region' = 'us-east-1'
    )
"""
try:
    t_env.execute_sql(catalog_ddl).print()
    logger.info("Iceberg catalog created successfully")
except Exception as e:
    logger.error(f"Failed to create catalog: {e}")

# Use the catalog
t_env.use_catalog('iceberg')
logger.info("Switched to iceberg catalog")

# Create database
logger.info("Creating database if not exists...")
try:
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS cdc").print()
    logger.info("Database cdc created or already exists")
except Exception as e:
    logger.error(f"Failed to create database: {e}")

t_env.use_database('cdc')
logger.info("Switched to cdc database")

# Create tables
logger.info("Creating Iceberg tables...")
account_table_ddl = """
    CREATE TABLE IF NOT EXISTS account_json (
        user_id INT,
        email STRING,
        ts TIMESTAMP(3),
        PRIMARY KEY (user_id) NOT ENFORCED
    ) WITH (
        'format-version' = '2',
        'write.upsert.enabled' = 'true'
    )
"""
try:
    t_env.execute_sql(account_table_ddl).print()
    logger.info("Account table created or already exists")
except Exception as e:
    logger.error(f"Failed to create account table: {e}")

product_table_ddl = """
    CREATE TABLE IF NOT EXISTS product_json (
        product_id INT,
        product_name STRING,
        ts TIMESTAMP(3),
        PRIMARY KEY (product_id) NOT ENFORCED
    ) WITH (
        'format-version' = '2',
        'write.upsert.enabled' = 'true'
    )
"""
try:
    t_env.execute_sql(product_table_ddl).print()
    logger.info("Product table created or already exists")
except Exception as e:
    logger.error(f"Failed to create product table: {e}")

# Process topics using SQL approach
logger.info("Using SQL approach for data processing...")
kafka_source_account_ddl = """
    CREATE TEMPORARY TABLE kafka_account_source (
        raw_data STRING,
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'raw_json_account',
        'properties.bootstrap.servers' = '10.17.26.218:9092',
        'properties.group.id' = 'flink-json-account',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'raw'
    )
"""
t_env.execute_sql(kafka_source_account_ddl)
logger.info("Created kafka_account_source table")

process_account_sql = """
    INSERT INTO account_json
    SELECT 
        CAST(JSON_VALUE(raw_data, '$.user_id') AS INT) as user_id,
        JSON_VALUE(raw_data, '$.email') as email,
        CURRENT_TIMESTAMP as ts
    FROM kafka_account_source
    WHERE JSON_VALUE(raw_data, '$.user_id') IS NOT NULL
"""
account_job = t_env.execute_sql(process_account_sql)
logger.info(f"Account job started: {account_job.get_job_client().get_job_id()}")

kafka_source_product_ddl = """
    CREATE TEMPORARY TABLE kafka_product_source (
        raw_data STRING,
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'raw_json_product',
        'properties.bootstrap.servers' = '10.17.26.218:9092',
        'properties.group.id' = 'flink-json-product',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'raw'
    )
"""
t_env.execute_sql(kafka_source_product_ddl)
logger.info("Created kafka_product_source table")

process_product_sql = """
    INSERT INTO product_json
    SELECT 
        CAST(JSON_VALUE(raw_data, '$.product_id') AS INT) as product_id,
        JSON_VALUE(raw_data, '$.product_name') as product_name,
        CURRENT_TIMESTAMP as ts
    FROM kafka_product_source
    WHERE JSON_VALUE(raw_data, '$.product_id') IS NOT NULL
"""
product_job = t_env.execute_sql(process_product_sql)
logger.info(f"Product job started: {product_job.get_job_client().get_job_id()}")

logger.info("Jobs submitted successfully. Processing data...")
# Job runs continuously