# REST Catalog

The Standard Iceberg REST Catalog provides a standard HTTP interface for catalog operations.

## Quick Start

```bash
make example-rest
```

## Configuration

```bash
FLOE_CATALOG_TYPE=REST
FLOE_CATALOG_NAME=demo
FLOE_CATALOG_URI=http://rest:8181
FLOE_CATALOG_WAREHOUSE=s3://warehouse/
```

## Options

| Variable | Required | Description |
|----------|----------|-------------|
| `FLOE_CATALOG_URI` | Yes | REST catalog endpoint |
| `FLOE_CATALOG_WAREHOUSE` | Yes | Warehouse location |
| `FLOE_CATALOG_NAME` | No | Catalog name (default: `demo`) |

## S3 Storage

```bash
FLOE_CATALOG_S3_ENDPOINT=http://minio:9000
FLOE_CATALOG_S3_ACCESS_KEY_ID=admin
FLOE_CATALOG_S3_SECRET_ACCESS_KEY=password
FLOE_CATALOG_S3_REGION=us-east-1
```

## Resources

- [Iceberg REST Catalog Spec](https://iceberg.apache.org/concepts/catalog/#decoupling-using-the-rest-catalog)
