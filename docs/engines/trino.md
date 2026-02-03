# Trino Engine

Trino executes Iceberg maintenance using built-in table procedures via JDBC.

## Operation Support

All Iceberg maintenance operations are supported across all catalog types
(REST, Hive, Nessie, Polaris, Lakekeeper, Gravitino).

## Configuration

**Property notation:**
```properties
floe.engine-type=TRINO
floe.trino.jdbc-url=jdbc:trino://trino:8080
floe.trino.user=floe
floe.trino.password=
floe.trino.catalog=demo
floe.trino.schema=test
floe.trino.query-timeout-seconds=3600
```

**Environment variable notation:**
```bash
FLOE_ENGINE_TYPE=TRINO
FLOE_TRINO_JDBC_URL=jdbc:trino://trino:8080
FLOE_TRINO_USER=floe
FLOE_TRINO_PASSWORD=
FLOE_TRINO_CATALOG=demo
FLOE_TRINO_SCHEMA=test
FLOE_TRINO_QUERY_TIMEOUT_SECONDS=3600
```

## Options

| Property | Environment Variable | Required | Default | Description |
|----------|---------------------|----------|---------|-------------|
| `floe.trino.jdbc-url` | `FLOE_TRINO_JDBC_URL` | Yes | - | JDBC connection URL |
| `floe.trino.user` | `FLOE_TRINO_USER` | Yes | - | Trino user |
| `floe.trino.password` | `FLOE_TRINO_PASSWORD` | No | - | Trino password |
| `floe.trino.catalog` | `FLOE_TRINO_CATALOG` | Yes | `demo` | Iceberg catalog name in Trino |
| `floe.trino.schema` | `FLOE_TRINO_SCHEMA` | No | `test` | Default schema |
| `floe.trino.query-timeout-seconds` | `FLOE_TRINO_QUERY_TIMEOUT_SECONDS` | No | `3600` | Query timeout |


## Trino Catalog Setup

Ensure your Trino Iceberg connector matches Floe's catalog:

```properties
# trino/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://rest-catalog:8181
iceberg.rest-catalog.warehouse=s3://warehouse/
```

## Monitoring

### Trino UI

Trino provides a web UI (default: `http://trino:8080/ui`) to monitor query execution, including Floe maintenance operations.

## Resources

- [Trino](https://trino.io/)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [Trino Iceberg Procedures](https://trino.io/docs/current/connector/iceberg.html#procedures)
- [Iceberg Table Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
