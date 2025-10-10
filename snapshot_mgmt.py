import trino

conn = trino.dbapi.connect(
    host='10.17.26.218', port=8089, user='trino', catalog='iceberg', schema='cdc'
)
cur = conn.cursor()

# Override min retention
cur.execute("SET SESSION iceberg.expire_snapshots_min_retention = '0s'")
cur.fetchall()

# List tables
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'cdc' AND table_type = 'BASE TABLE'")
tables = [row[0] for row in cur.fetchall()]

# Expire per table
for table in tables:
    cur.execute(f"ALTER TABLE cdc.{table} EXECUTE expire_snapshots(retention_threshold => '6h')")
    cur.fetchall()

conn.close()