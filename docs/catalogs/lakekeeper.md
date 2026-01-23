# Lakekeeper

Fast, secure Iceberg REST Catalog written in Rust.

## Quick Start

```bash
make example-lakekeeper
```

Open http://localhost:9091 to access the Floe UI.

## Configuration

```bash
FLOE_CATALOG_TYPE=LAKEKEEPER
FLOE_CATALOG_NAME=demo
FLOE_CATALOG_LAKEKEEPER_URI=http://lakekeeper:8181/catalog
FLOE_CATALOG_WAREHOUSE=demo

# S3/MinIO storage
FLOE_CATALOG_S3_ENDPOINT=http://minio:9000
FLOE_CATALOG_S3_ACCESS_KEY_ID=admin
FLOE_CATALOG_S3_SECRET_ACCESS_KEY=password
FLOE_CATALOG_S3_REGION=us-east-1
```

## Options

| Variable | Required | Description |
|----------|----------|-------------|
| `FLOE_CATALOG_LAKEKEEPER_URI` | Yes | Lakekeeper REST endpoint (e.g., `http://lakekeeper:8181/catalog`) |
| `FLOE_CATALOG_WAREHOUSE` | Yes | Warehouse name (e.g., `demo`) |
| `FLOE_CATALOG_LAKEKEEPER_CREDENTIAL` | No | OAuth2 credential in format `clientId:clientSecret` |
| `FLOE_CATALOG_LAKEKEEPER_OAUTH2_SERVER_URI` | No | OAuth2 token endpoint |
| `FLOE_CATALOG_LAKEKEEPER_SCOPE` | No | OAuth2 scope |
| `FLOE_CATALOG_S3_ENDPOINT` | Yes* | S3/MinIO endpoint (*required for MinIO or custom S3) |
| `FLOE_CATALOG_S3_ACCESS_KEY_ID` | Yes | S3 access key |
| `FLOE_CATALOG_S3_SECRET_ACCESS_KEY` | Yes | S3 secret key |
| `FLOE_CATALOG_S3_REGION` | No | S3 region (default: `us-east-1`) |

## Authentication

For authenticated Lakekeeper servers with OpenID Connect:

```bash
FLOE_CATALOG_LAKEKEEPER_CREDENTIAL=client-id:client-secret
FLOE_CATALOG_LAKEKEEPER_OAUTH2_SERVER_URI=https://idp.example.com/oauth/token
FLOE_CATALOG_LAKEKEEPER_SCOPE=catalog:read catalog:write
```

## Engine Compatibility

Both Spark and Trino support Lakekeeper via the standard Iceberg REST catalog protocol.

## Resources

- [Lakekeeper Documentation](https://docs.lakekeeper.io/)
- [Lakekeeper GitHub](https://github.com/lakekeeper/lakekeeper)
