# Floe

Floe is a policy-based table maintenance system for Apache Iceberg.

Floe automates table maintenance through declarative policies:

- **Compact small files** for better query performance
- **Expire old snapshots** to reclaim storage
- **Remove orphan files** not referenced by any snapshot
- **Optimize manifests** for faster query planning

Floe orchestrates maintenance operations that wrap around [Iceberg's maintenance procedures](https://iceberg.apache.org/docs/latest/maintenance/).

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
| [Configuration](getting-started/configuration.md) | Configure catalogs, engines, storage |
| [Architecture](architecture/overview.md) | System design and extension points |

## Catalogs

| Catalog                        | Description |
|--------------------------------|-------------|
| [REST](catalogs/rest.md)       | Iceberg REST Catalog |
| [Nessie](catalogs/nessie.md)   | Git-like versioning |
| [Polaris](catalogs/polaris.md) | Multi-engine access control |
| [Hive](catalogs/hms.md)        | Hive Metastore |

## Engines

| Engine | Description |
|--------|-------------|
| [Spark](engines/spark.md) | Apache Spark (via Livy) |
| [Trino](engines/trino.md) | Trino SQL |

## API

API documentation (once service is up and running): [Swagger UI](http://localhost:9091/api)

