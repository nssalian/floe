# Floe

Floe is a policy-based table maintenance system for Apache Iceberg. 
It continuously assesses table health and automatically triggers maintenance 
operations based on configurable conditions.

- **Compact small files** for better query performance
- **Expire old snapshots** to reclaim storage
- **Remove orphan files** not referenced by any snapshot
- **Optimize manifests** for faster query planning

![High Level Architecture](assets/high_level.png)

## Quick Start

```bash
git clone https://github.com/nssalian/floe.git
cd floe
make start
```

Open http://localhost:9091

## Documentation

| Section | Description |
|---------|-------------|
| [Policies](policies.md) | Policy configuration guide and API reference |
| [Configuration](getting-started/configuration.md) | Configure catalogs, engines, storage |
| [Architecture](architecture/overview.md) | System design and extension points |

## Catalogs

| Catalog                              | Description |
|--------------------------------------|-------------|
| [REST](catalogs/rest.md)             | Iceberg REST Catalog |
| [Nessie](catalogs/nessie.md)         | Git-like versioning |
| [Polaris](catalogs/polaris.md)       | Multi-engine access control |
| [Hive](catalogs/hms.md)              | Hive Metastore |
| [Lakekeeper](catalogs/lakekeeper.md) | Open-source Iceberg catalog |
| [Gravitino](catalogs/gravitino.md)   | Unified metadata lake |
| [DataHub](catalogs/datahub.md)       | DataHub catalog integration |

## Engines

| Engine | Description |
|--------|-------------|
| [Spark](engines/spark.md) | Apache Spark (via Livy) |
| [Trino](engines/trino.md) | Trino SQL |

## API

API documentation (once service is up and running): [Swagger UI](http://localhost:9091/api)

