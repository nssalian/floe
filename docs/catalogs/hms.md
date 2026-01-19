# Hive Metastore

The most widely-adopted catalog for Iceberg tables.

> **Note:** Hive Metastore 4.x is not yet compatible with Apache Iceberg due to Thrift API changes ([apache/iceberg#12878](https://github.com/apache/iceberg/issues/12878)). Use HMS 3.x (e.g., 3.1.3) for Iceberg workloads.

## Quick Start

```bash
make example-hms
```

Open http://localhost:9091 to access the Floe UI.

## Configuration

```bash
FLOE_CATALOG_TYPE=HIVE
FLOE_CATALOG_NAME=demo
FLOE_CATALOG_HIVE_URI=thrift://hive-metastore:9083
FLOE_CATALOG_WAREHOUSE=s3://warehouse/

# S3/MinIO storage (required for reading table data)
FLOE_CATALOG_S3_ENDPOINT=http://minio:9000
FLOE_CATALOG_S3_ACCESS_KEY_ID=admin
FLOE_CATALOG_S3_SECRET_ACCESS_KEY=password
FLOE_CATALOG_S3_REGION=us-east-1
```

## Options

| Variable | Required | Description |
|----------|----------|-------------|
| `FLOE_CATALOG_HIVE_URI` | Yes | HMS Thrift URI (e.g., `thrift://hive-metastore:9083`) |
| `FLOE_CATALOG_WAREHOUSE` | Yes | Default warehouse location (e.g., `s3://warehouse/`) |
| `FLOE_CATALOG_S3_ENDPOINT` | Yes* | S3/MinIO endpoint (*required for MinIO or custom S3) |
| `FLOE_CATALOG_S3_ACCESS_KEY_ID` | Yes | S3 access key |
| `FLOE_CATALOG_S3_SECRET_ACCESS_KEY` | Yes | S3 secret key |
| `FLOE_CATALOG_S3_REGION` | No | S3 region (default: `us-east-1`) |

## Engine Compatibility

Both Spark and Trino have full support for all Iceberg maintenance operations with Hive Metastore.

## Resources

- [Iceberg Hive Catalog](https://iceberg.apache.org/docs/latest/hive/)
- [Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-Metastore)
- [HMS image used for demo](https://github.com/criccomini/hive-metastore-standalone)
