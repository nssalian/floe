# DataHub

The Iceberg REST Catalog portion of the Datahub project.

> **Note:** DataHub support is early. No example setup is provided at this time.

## Configuration

```bash
FLOE_CATALOG_TYPE=DATAHUB
FLOE_CATALOG_NAME=demo
FLOE_CATALOG_DATAHUB_URI=http://datahub-gms:8080/iceberg/
FLOE_CATALOG_DATAHUB_TOKEN=<your-personal-access-token>
FLOE_CATALOG_WAREHOUSE=s3://warehouse/

# S3 storage
FLOE_CATALOG_S3_ENDPOINT=http://minio:9000
FLOE_CATALOG_S3_ACCESS_KEY_ID=admin
FLOE_CATALOG_S3_SECRET_ACCESS_KEY=password
FLOE_CATALOG_S3_REGION=us-east-1
```

## Options

| Variable | Required | Description |
|----------|----------|-------------|
| `FLOE_CATALOG_DATAHUB_URI` | Yes | DataHub GMS Iceberg endpoint (e.g., `http://datahub-gms:8080/iceberg/`) |
| `FLOE_CATALOG_DATAHUB_TOKEN` | Yes | DataHub Personal Access Token |
| `FLOE_CATALOG_WAREHOUSE` | Yes | Warehouse location (e.g., `s3://warehouse/`) |
| `FLOE_CATALOG_S3_ENDPOINT` | Yes* | S3 endpoint (*required for MinIO or custom S3) |
| `FLOE_CATALOG_S3_ACCESS_KEY_ID` | Yes | S3 access key |
| `FLOE_CATALOG_S3_SECRET_ACCESS_KEY` | Yes | S3 secret key |
| `FLOE_CATALOG_S3_REGION` | No | S3 region (default: `us-east-1`) |

## Authentication

DataHub requires a Personal Access Token (PAT):

1. Open DataHub UI
2. Go to Settings > Access Tokens
3. Generate a new token
4. Set `FLOE_CATALOG_DATAHUB_TOKEN`

## Engine Compatibility

Both Spark and Trino support DataHub via the standard Iceberg REST catalog protocol.

## Resources

- [DataHub Iceberg Catalog](https://docs.datahub.com/docs/iceberg-catalog)
- [DataHub GitHub](https://github.com/datahub-project/datahub)
