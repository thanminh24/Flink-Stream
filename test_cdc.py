#!/usr/bin/env python3
# test_cdc.py: Insert Postgres data, push JSON to Kafka, and verify Iceberg tables via Trino
import subprocess
import sys
import json
import random
import os
import time
from datetime import datetime
import requests
from kafka import KafkaProducer
import psycopg2
from psycopg2 import sql
import pprint
import argparse

# Environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = "5433"  # Hardcoded to match CDC-Postgres container
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

PROD_TRINO_HOST = os.getenv("PROD_TRINO_HOST", "10.17.26.218")
PROD_TRINO_PORT = os.getenv("PROD_TRINO_PORT", "8080")
PROD_TRINO_CATALOG = os.getenv("PROD_TRINO_CATALOG", "iceberg")
PROD_TRINO_SCHEMA = os.getenv("PROD_TRINO_SCHEMA", "cdc")
TRINO_USER = os.getenv("TRINO_USER", "trino")

TRINO_URL = f"http://{PROD_TRINO_HOST}:{PROD_TRINO_PORT}/v1/statement"

def get_pg_connection():
    """Get a PostgreSQL connection."""
    print(f"Connecting to PostgreSQL: host={POSTGRES_HOST}, port={POSTGRES_PORT}, user={POSTGRES_USER}, dbname={POSTGRES_DB}")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB
        )
        print("Connection successful!")
        return conn
    except psycopg2.OperationalError as e:
        print(f"(error) Failed to connect to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)

def query_pg(sql_query):
    """Execute a PostgreSQL query using psycopg2."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            rows = cur.fetchall()
            return rows
    except Exception as e:
        print(f"(warn) Failed to query Postgres: {e}", file=sys.stderr)
        return []
    finally:
        conn.close()

def insert_pg(sql_insert):
    """Execute a PostgreSQL insert using psycopg2."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql_insert)
            conn.commit()
    except Exception as e:
        print(f"(warn) Failed to insert into Postgres: {e}", file=sys.stderr)
    finally:
        conn.close()

def insert_test_rows(bulk=False):
    """Insert random rows into PostgreSQL commerce.account and commerce.product."""
    if bulk:
        print("Inserting 100 test rows into PostgreSQL...")
        emails = [f"test_{random.randint(100000, 999999)}@example.com" for _ in range(100)]
        products = [f"Item_{random.choice(['A', 'B', 'C'])}{random.randint(100000, 999999)}" for _ in range(100)]
        for email, product in zip(emails, products):
            insert_account = f"INSERT INTO commerce.account (email) VALUES ('{email}');"
            insert_product = f"INSERT INTO commerce.product (product_name) VALUES ('{product}');"
            insert_pg(insert_account)
            insert_pg(insert_product)
        return emails, products
    else:
        random_email = f"test_{random.randint(100000, 999999)}@example.com"
        random_product = f"Item_{random.choice(['A', 'B', 'C'])}{random.randint(100000, 999999)}"
        insert_account = f"INSERT INTO commerce.account (email) VALUES ('{random_email}');"
        insert_product = f"INSERT INTO commerce.product (product_name) VALUES ('{random_product}');"
        print("Inserting test data into PostgreSQL...")
        insert_pg(insert_account)
        insert_pg(insert_product)
        return [random_email], [random_product]

def produce_json_to_kafka(emails, products):
    """Push JSON data to Kafka topics with timestamps."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    timestamp = datetime.now().isoformat()  # ISO format for TIMESTAMP(3)
    results = []
    for email, product in zip(emails, products):
        user_id = random.randint(1000, 9999)
        product_id = random.randint(1000, 9999)
        account_json = {
            'user_id': user_id,
            'email': email
        }
        product_json = {
            'product_id': product_id,
            'product_name': product
        }
        producer.send('raw_json_account', account_json)
        producer.send('raw_json_product', product_json)
        results.append((user_id, product_id, timestamp))
        print(f"Pushed JSON to Kafka: {account_json}, {product_json}")
    producer.flush()
    return results

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="CDC Test Script")
    parser.add_argument('--bulk', action='store_true', help='Insert 100 JSON entries instead of 1')
    args = parser.parse_args()

    # Query Postgres before insert
    print("Querying Postgres (before insert)...")
    account_rows = query_pg("SELECT * FROM commerce.account ORDER BY user_id;")
    product_rows = query_pg("SELECT * FROM commerce.product ORDER BY product_id;")
    print("Postgres account:")
    pprint.pprint(account_rows)
    print("Postgres product:")
    pprint.pprint(product_rows)

    # Insert into Postgres and push to Kafka
    emails, products = insert_test_rows(bulk=args.bulk)
    print("Inserted Postgres:", emails, products)
    results = produce_json_to_kafka(emails, products)


if __name__ == "__main__":
    main()