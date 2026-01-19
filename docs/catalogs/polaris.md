# Apache Polaris

Centralized access control across query engines.

> **Note:** Polaris 1.0+ includes a policy framework for storing maintenance rules (compaction, snapshot expiry, orphan removal). However, Polaris does not execute these operations. Floe provides the orchestration and execution layer, delegating to Spark or Trino.

## Quick Start

```bash
make example-polaris
```

This starts Polaris with MinIO storage and automatically creates the `demo` catalog.

## Configuration

```bash
FLOE_CATALOG_TYPE=POLARIS
FLOE_CATALOG_NAME=demo
FLOE_CATALOG_POLARIS_URI=http://polaris:8181/api/catalog
FLOE_CATALOG_POLARIS_CLIENT_ID=root
FLOE_CATALOG_POLARIS_CLIENT_SECRET=secret
FLOE_CATALOG_WAREHOUSE=demo
```

> **Note:** For Polaris, `FLOE_CATALOG_WAREHOUSE` should be the **catalog name** (e.g., `demo`), not an S3 path. Polaris manages storage locations internally via catalog configuration.

## Options

| Variable | Required | Description |
|----------|----------|-------------|
| `FLOE_CATALOG_POLARIS_URI` | Yes | Polaris REST API endpoint (e.g., `http://polaris:8181/api/catalog`) |
| `FLOE_CATALOG_POLARIS_CLIENT_ID` | Yes | OAuth2 client ID |
| `FLOE_CATALOG_POLARIS_CLIENT_SECRET` | Yes | OAuth2 client secret |
| `FLOE_CATALOG_POLARIS_PRINCIPAL_ROLE` | No | Principal role for scoped access (defaults to `ALL`) |

## Catalog Setup

Polaris requires catalog creation before use. 
The example docker-compose includes a `polaris-init` container that automatically:

1. Gets an OAuth2 token
2. Creates the `demo` catalog with S3 storage configuration
3. Creates an `admin` catalog role with `CATALOG_MANAGE_CONTENT` privilege
4. Assigns the role to the `service_admin` principal

For production, use the Polaris Management API or CLI to create catalogs.

## MinIO / S3-Compatible Storage

When using MinIO or other S3-compatible storage, the catalog must be created with the S3 endpoint configuration:

```json
{
  "catalog": {
    "name": "demo",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse/"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://warehouse/"],
      "endpoint": "http://minio:9000",
      "pathStyleAccess": true
    }
  }
}
```

> **Note:** MinIO does not require `roleArn` - Polaris will use the configured AWS credentials directly.

## Execution Engines

### Spark

Floe passes catalog properties to Spark jobs submitted via Livy. The OAuth2 credentials (`credential` and `scope`) are automatically included.

```bash
# Spark configuration (submitted via Livy)
FLOE_ENGINE_TYPE=SPARK
FLOE_LIVY_URL=http://livy:8998
```

### Trino

For Trino, configure the Iceberg connector with OAuth2 authentication:

```properties
# iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog
iceberg.rest-catalog.warehouse=demo
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=root:secret
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
```

The example automatically generates this configuration when `CATALOG_CREDENTIAL` is set.

```bash
# Trino engine configuration
FLOE_ENGINE_TYPE=TRINO
FLOE_TRINO_JDBC_URL=jdbc:trino://trino:8080
```

## Resources

- [Apache Polaris](https://polaris.apache.org/)
- [Polaris with MinIO](https://polaris.apache.org/releases/1.1.0/getting-started/minio/)
- [Polaris Documentation](https://polaris.apache.org/in-dev/unreleased/)
