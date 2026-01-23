# Catalogs

Floe connects to Iceberg catalogs to discover tables and execute maintenance.

## Supported Catalogs

| Catalog                          | Type |
|----------------------------------|------|
| [REST Catalog](rest.md)          | `REST` |
| [Project Nessie](nessie.md)      | `NESSIE` |
| [Apache Polaris](polaris.md)     | `POLARIS` |
| [Hive Metastore](hms.md)         | `HIVE` |
| [Lakekeeper](lakekeeper.md)      | `LAKEKEEPER` |
| [Apache Gravitino](gravitino.md) | `GRAVITINO` |

## Quick Start with Examples

Use make commands to quickly run Floe with any catalog:

```bash
# REST Catalog
make example-rest
# Nessie
make example-nessie
# Apache Polaris
make example-polaris
# Hive Metastore
make example-hms
# Lakekeeper
make example-lakekeeper
# Apache Gravitino
make example-gravitino
```
Same as above, but using Trino as the execution engine:

```bash
# REST Catalog with Trino
make example-rest-trino
# Nessie with Trino
make example-nessie-trino
# Apache Polaris with Trino
make example-polaris-trino
# Hive Metastore with Trino
make example-hms-trino
# Lakekeeper with Trino
make example-lakekeeper-trino
# Apache Gravitino with Trino
make example-gravitino-trino
```

Each example starts at **http://localhost:9091** with all dependencies included.

Stop an example:

```bash
make clean
```

## Resources

- [Iceberg Table Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)