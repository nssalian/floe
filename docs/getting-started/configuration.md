# Configuration

Floe is configured via environment variables prefixed with `FLOE_`.

## Quick Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `FLOE_ENGINE_TYPE` | `SPARK` or `TRINO` | `SPARK` |
| `FLOE_CATALOG_TYPE` | `REST`, `HIVE`, `NESSIE`, `POLARIS`, `LAKEKEEPER`, `GRAVITINO` | `REST` |
| `FLOE_CATALOG_NAME` | Catalog name | `demo` |
| `FLOE_CATALOG_WAREHOUSE` | Warehouse location | `s3://warehouse/` |
| `FLOE_STORE_TYPE` | `POSTGRES` or `MEMORY` | `MEMORY` |
| `FLOE_SCHEDULER_ENABLED` | Enable built-in scheduler | `true` |

## Catalog

```bash
FLOE_CATALOG_TYPE=REST
FLOE_CATALOG_NAME=demo
FLOE_CATALOG_URI=http://rest:8181
FLOE_CATALOG_WAREHOUSE=s3://warehouse/

# S3 storage
FLOE_CATALOG_S3_ENDPOINT=http://minio:9000
FLOE_CATALOG_S3_REGION=us-east-1
FLOE_CATALOG_S3_ACCESS_KEY_ID=admin
FLOE_CATALOG_S3_SECRET_ACCESS_KEY=password

# AWS SDK also needs these
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_REGION=us-east-1
```

### Catalog Types

**REST Catalog**

```bash
FLOE_CATALOG_TYPE=REST
FLOE_CATALOG_URI=http://rest:8181
```

**Hive Metastore**

```bash
FLOE_CATALOG_TYPE=HIVE
FLOE_CATALOG_HIVE_URI=thrift://hive-metastore:9083
```

**Nessie**

```bash
FLOE_CATALOG_TYPE=NESSIE
FLOE_CATALOG_NESSIE_URI=http://nessie:19120/api/v1
FLOE_CATALOG_NESSIE_REF=main
```

**Polaris**

```bash
FLOE_CATALOG_TYPE=POLARIS
FLOE_CATALOG_POLARIS_URI=http://polaris:8181/api/catalog
FLOE_CATALOG_POLARIS_CLIENT_ID=root
FLOE_CATALOG_POLARIS_CLIENT_SECRET=secret
```

**Lakekeeper**

```bash
FLOE_CATALOG_TYPE=LAKEKEEPER
FLOE_CATALOG_LAKEKEEPER_URI=http://lakekeeper:8181
FLOE_CATALOG_LAKEKEEPER_CREDENTIAL=clientId:clientSecret
FLOE_CATALOG_LAKEKEEPER_OAUTH2_SERVER_URI=https://idp.example.com/oauth/token
# Optional settings (defaults shown)
# FLOE_CATALOG_LAKEKEEPER_NESTED_NAMESPACE_ENABLED=true
# FLOE_CATALOG_LAKEKEEPER_VENDED_CREDENTIALS_ENABLED=true
```

**Gravitino**

```bash
FLOE_CATALOG_TYPE=GRAVITINO
FLOE_CATALOG_GRAVITINO_URI=http://gravitino:9001/iceberg/
FLOE_CATALOG_GRAVITINO_METALAKE=demo
# Optional OAuth2 settings
# FLOE_CATALOG_GRAVITINO_CREDENTIAL=clientId:clientSecret
# FLOE_CATALOG_GRAVITINO_OAUTH2_SERVER_URI=https://idp.example.com/oauth/token
# FLOE_CATALOG_GRAVITINO_VENDED_CREDENTIALS_ENABLED=true
```

## Engine

**Spark (via Livy)**

```bash
FLOE_ENGINE_TYPE=SPARK
FLOE_LIVY_URL=http://livy:8998
```

**Trino**

```bash
FLOE_ENGINE_TYPE=TRINO
FLOE_TRINO_JDBC_URL=jdbc:trino://trino:8080
```

## Store

**PostgreSQL**

```bash
FLOE_STORE_TYPE=POSTGRES
QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://postgres:5432/floe
QUARKUS_DATASOURCE_USERNAME=floe
QUARKUS_DATASOURCE_PASSWORD=floe
```

**In-Memory (Development)**

```bash
FLOE_STORE_TYPE=MEMORY
```

## Scheduler

```bash
FLOE_SCHEDULER_ENABLED=true
```

For external schedulers (Airflow, Dagster), disable the built-in scheduler and trigger via API:

```bash
FLOE_SCHEDULER_ENABLED=false
```
