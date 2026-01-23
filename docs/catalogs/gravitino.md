# Apache Gravitino

Federated metadata lake with Iceberg REST catalog interface.

## Quick Start

```bash
make example-gravitino
```

Open http://localhost:9091 to access the Floe UI.

## Configuration

```bash
FLOE_CATALOG_TYPE=GRAVITINO
FLOE_CATALOG_NAME=demo
FLOE_CATALOG_GRAVITINO_URI=http://gravitino:9001/iceberg/
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
| `FLOE_CATALOG_GRAVITINO_URI` | Yes | Gravitino Iceberg REST endpoint (e.g., `http://gravitino:9001/iceberg/`) |
| `FLOE_CATALOG_WAREHOUSE` | Yes | Catalog name (e.g., `demo`) |
| `FLOE_CATALOG_GRAVITINO_METALAKE` | No | Metalake name |
| `FLOE_CATALOG_GRAVITINO_CREDENTIAL` | No | OAuth2 credential in format `clientId:clientSecret` |
| `FLOE_CATALOG_GRAVITINO_OAUTH2_SERVER_URI` | No | OAuth2 token endpoint |
| `FLOE_CATALOG_S3_ENDPOINT` | Yes* | S3/MinIO endpoint (*required for MinIO or custom S3) |
| `FLOE_CATALOG_S3_ACCESS_KEY_ID` | Yes | S3 access key |
| `FLOE_CATALOG_S3_SECRET_ACCESS_KEY` | Yes | S3 secret key |
| `FLOE_CATALOG_S3_REGION` | No | S3 region (default: `us-east-1`) |

## Authentication

For authenticated Gravitino servers:

```bash
FLOE_CATALOG_GRAVITINO_CREDENTIAL=client-id:client-secret
FLOE_CATALOG_GRAVITINO_OAUTH2_SERVER_URI=https://idp.example.com/oauth/token
```

## Engine Compatibility

Both Spark and Trino support Gravitino via the standard Iceberg REST catalog protocol.

## Resources

- [Apache Gravitino](https://gravitino.apache.org/)
- [Gravitino Iceberg REST Service](https://gravitino.apache.org/docs/0.6.0-incubating/iceberg-rest-service/)
- [Gravitino GitHub](https://github.com/apache/gravitino)
