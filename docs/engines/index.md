# Execution Engines

Floe delegates maintenance operations to execution engines that interact with Iceberg tables.

## Supported Engines

| Engine | Type |
|--------|------|
| [Trino](trino.md) | `TRINO` |
| [Apache Spark](spark.md) | `SPARK` |

## Configuration

```properties
floe.engine-type=TRINO
```

## Operation Support

Both Trino and Spark support all maintenance operations (compaction, snapshot expiration, orphan cleanup, manifest rewriting) across all supported catalogs.

Each maintenance operation maps to engine-specific commands:

| Operation | Trino | Spark |
|-----------|-------|-------|
| Rewrite Data Files | `ALTER TABLE ... EXECUTE optimize` | `RewriteDataFilesSparkAction` |
| Expire Snapshots | `ALTER TABLE ... EXECUTE expire_snapshots` | `ExpireSnapshotsSparkAction` |
| Orphan Cleanup | `ALTER TABLE ... EXECUTE remove_orphan_files` | `DeleteOrphanFilesSparkAction` |
| Rewrite Manifests | `ALTER TABLE ... EXECUTE optimize_manifests` | `RewriteManifestsSparkAction` |
