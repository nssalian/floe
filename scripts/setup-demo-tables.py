#!/usr/bin/env python3
"""
Create demo Iceberg tables for Floe testing.
Configures Spark based on CATALOG_TYPE environment variable.

Supported catalog types: rest, nessie, polaris, hive, lakekeeper, gravitino
"""

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import random
import os

# Read catalog config from environment
CATALOG_TYPE = os.environ.get("CATALOG_TYPE", "rest").lower()
CATALOG_NAME = os.environ.get("CATALOG_NAME", "demo")
CATALOG_URI = os.environ.get("CATALOG_URI", "http://rest:8181")
CATALOG_WAREHOUSE = os.environ.get("CATALOG_WAREHOUSE", "s3://warehouse/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")

print(f"Configuring Spark for {CATALOG_TYPE} catalog...")

# Base builder
builder = SparkSession.builder \
    .appName("FloeDemo") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", CATALOG_WAREHOUSE) \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint", S3_ENDPOINT) \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id", S3_ACCESS_KEY) \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key", S3_SECRET_KEY) \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access", "true") \
    .config("spark.sql.defaultCatalog", CATALOG_NAME)

# Catalog-specific configuration
if CATALOG_TYPE == "rest":
    builder = builder \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", CATALOG_URI)

elif CATALOG_TYPE == "nessie":
    NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v1")
    NESSIE_REF = os.environ.get("NESSIE_REF", "main")
    # Nessie uses catalog-impl, not type - they are mutually exclusive
    # Note: tabulario/spark-iceberg has 'demo' catalog preconfigured with type=rest
    # so we configure nessie catalog fresh without type property
    builder = builder \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", NESSIE_URI) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.ref", NESSIE_REF)

elif CATALOG_TYPE == "polaris":
    # For Polaris, warehouse = catalog name, not S3 path
    # Polaris manages storage locations via catalog config
    POLARIS_CREDENTIAL = os.environ.get("POLARIS_CREDENTIAL", "root:secret")
    builder = builder \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", CATALOG_URI) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", CATALOG_NAME) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.credential", POLARIS_CREDENTIAL) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.scope", "PRINCIPAL_ROLE:ALL")

elif CATALOG_TYPE == "hive":
    HMS_URI = os.environ.get("HMS_URI", "thrift://hive-metastore:9083")
    builder = builder \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hive") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", HMS_URI)

elif CATALOG_TYPE == "lakekeeper":
    # Lakekeeper is a standard Iceberg REST catalog
    # URI should be http://lakekeeper:8181/catalog/<warehouse-name>
    builder = builder \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", CATALOG_URI)

elif CATALOG_TYPE == "gravitino":
    # Gravitino provides an Iceberg REST catalog interface
    # URI should be http://gravitino:9001/iceberg/<metalake>
    builder = builder \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", CATALOG_URI)

else:
    print(f"Unknown catalog type: {CATALOG_TYPE}, defaulting to REST")
    builder = builder \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", CATALOG_URI)

spark = builder.getOrCreate()

print(f"Setting up Floe demo tables in {CATALOG_NAME}...")

# Create namespace
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.test")

# Table 1: events - many small files for compaction
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.test.events")
spark.sql(f"""
    CREATE TABLE {CATALOG_NAME}.test.events (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        event_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(event_timestamp))
      TBLPROPERTIES ('gc.enabled' = 'true')
""")

event_types = ["click", "view", "purchase", "signup"]
for batch in range(10):
    data = []
    base_time = datetime.now() - timedelta(days=random.randint(0, 30))
    for i in range(100):
        data.append((
            f"evt_{batch}_{i}",
            random.choice(event_types),
            f"user_{random.randint(1, 100)}",
            base_time + timedelta(minutes=random.randint(0, 1440))
        ))
    df = spark.createDataFrame(data, ["event_id", "event_type", "user_id", "event_timestamp"])
    df.writeTo(f"{CATALOG_NAME}.test.events").append()
print(f"Created {CATALOG_NAME}.test.events (10 files, 1000 rows)")

# Table 2: users - with updates for delete files
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.test.users")
spark.sql(f"""
    CREATE TABLE {CATALOG_NAME}.test.users (
        user_id STRING,
        name STRING,
        status STRING,
        updated_at TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', 'gc.enabled' = 'true')
""")

users_data = [(f"user_{i}", f"User {i}", "active", datetime.now()) for i in range(200)]
spark.createDataFrame(users_data, ["user_id", "name", "status", "updated_at"]).writeTo(f"{CATALOG_NAME}.test.users").append()

spark.sql(f"""
    MERGE INTO {CATALOG_NAME}.test.users t
    USING (SELECT 'user_1' as user_id) s ON t.user_id = s.user_id
    WHEN MATCHED THEN UPDATE SET t.status = 'inactive', t.updated_at = current_timestamp()
""")
print(f"Created {CATALOG_NAME}.test.users (200 rows, with delete files)")

# Table 3: transactions - many snapshots
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.test.transactions")
spark.sql(f"""
    CREATE TABLE {CATALOG_NAME}.test.transactions (
        txn_id STRING,
        amount DOUBLE,
        txn_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(txn_timestamp))
""")

for day in range(10):
    txn_date = datetime.now() - timedelta(days=day)
    data = [(f"txn_{day}_{i}", round(random.uniform(10, 500), 2), txn_date) for i in range(50)]
    spark.createDataFrame(data, ["txn_id", "amount", "txn_timestamp"]).writeTo(f"{CATALOG_NAME}.test.transactions").append()
print(f"Created {CATALOG_NAME}.test.transactions (10 snapshots, 500 rows)")

# Table 4: scheduler_test - for testing scheduled operations (runs every minute)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.test.scheduler_test")
spark.sql(f"""
    CREATE TABLE {CATALOG_NAME}.test.scheduler_test (
        id STRING,
        value INT,
        ts TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('gc.enabled' = 'true')
""")

for batch in range(5):
    data = [(f"id_{batch}_{i}", random.randint(1, 100), datetime.now()) for i in range(20)]
    spark.createDataFrame(data, ["id", "value", "ts"]).writeTo(f"{CATALOG_NAME}.test.scheduler_test").append()
print(f"Created {CATALOG_NAME}.test.scheduler_test (5 files, 100 rows)")

# Table 5: orders - for individual policy testing
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.test.orders")
spark.sql(f"""
    CREATE TABLE {CATALOG_NAME}.test.orders (
        order_id STRING,
        customer_id STRING,
        product STRING,
        quantity INT,
        price DOUBLE,
        order_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(order_timestamp))
    TBLPROPERTIES ('format-version' = '2', 'gc.enabled' = 'true')
""")

products = ["laptop", "phone", "tablet", "headphones", "keyboard", "mouse"]
for batch in range(8):
    data = []
    base_time = datetime.now() - timedelta(days=random.randint(0, 14))
    for i in range(75):
        product = random.choice(products)
        data.append((
            f"ord_{batch}_{i}",
            f"cust_{random.randint(1, 50)}",
            product,
            random.randint(1, 5),
            round(random.uniform(20, 1500), 2),
            base_time + timedelta(minutes=random.randint(0, 1440))
        ))
    df = spark.createDataFrame(data, ["order_id", "customer_id", "product", "quantity", "price", "order_timestamp"])
    df.writeTo(f"{CATALOG_NAME}.test.orders").append()
print(f"Created {CATALOG_NAME}.test.orders (8 files, 600 rows)")

print(f"\nDemo tables ready in {CATALOG_NAME}:")
print(f"  {CATALOG_NAME}.test.events         - full-maintenance policy (all 4 ops)")
print(f"  {CATALOG_NAME}.test.users          - full-maintenance policy (all 4 ops)")
print(f"  {CATALOG_NAME}.test.transactions   - full-maintenance policy (all 4 ops)")
print(f"  {CATALOG_NAME}.test.scheduler_test - scheduler test (runs every minute)")
print(f"  {CATALOG_NAME}.test.orders         - individual policies (for isolated testing)")

spark.stop()
