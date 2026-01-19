# Project Nessie

Git-like version control for Iceberg tables with branching and tagging.

## Quick Start

```bash
make example-nessie
```

Open http://localhost:9091 to access the Floe UI.

## Configuration

```bash
FLOE_CATALOG_TYPE=NESSIE
FLOE_CATALOG_NAME=demo
FLOE_CATALOG_NESSIE_URI=http://nessie:19120/api/v1
FLOE_CATALOG_NESSIE_REF=main
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
| `FLOE_CATALOG_NESSIE_URI` | Yes | Nessie API endpoint (e.g., `http://nessie:19120/api/v1`) |
| `FLOE_CATALOG_NESSIE_REF` | No | Branch or tag (default: `main`) |
| `FLOE_CATALOG_NESSIE_TOKEN` | No | Bearer token for authentication |
| `FLOE_CATALOG_WAREHOUSE` | Yes | Warehouse location (e.g., `s3://warehouse/`) |
| `FLOE_CATALOG_S3_ENDPOINT` | Yes* | S3/MinIO endpoint (*required for MinIO or custom S3) |
| `FLOE_CATALOG_S3_ACCESS_KEY_ID` | Yes | S3 access key |
| `FLOE_CATALOG_S3_SECRET_ACCESS_KEY` | Yes | S3 secret key |
| `FLOE_CATALOG_S3_REGION` | No | S3 region (default: `us-east-1`) |

## Authentication

For authenticated Nessie servers, provide a Bearer token:

```bash
FLOE_CATALOG_NESSIE_TOKEN=your-jwt-token
```

## Engine Compatibility

Both Spark and Trino have full support for all Iceberg maintenance operations with Nessie.

## How It Works

Nessie stores pointers to Iceberg table metadata (metadata location, snapshot ID, schema ID) rather than the actual data. Table data remains in object storage (S3/MinIO).

**Implicit namespaces**: Nessie namespaces are derived from table paths and don't need to be explicitly created. 
When you create a table at `test.orders`, the `test` namespace is automatically recognized.

## Resources

- [Project Nessie](https://projectnessie.org/)
- [Nessie + Iceberg](https://projectnessie.org/guides/iceberg-rest/)
