# Policies

Policies define maintenance operations for Iceberg tables matching a pattern.

## Policy Structure

```json
{
  "name": "daily-maintenance",
  "description": "Daily maintenance for analytics tables",
  "tablePattern": "catalog.analytics.*",
  "enabled": true,
  "priority": 100,
  "tags": {"env": "prod"},

  "rewriteDataFiles": { ... },
  "rewriteDataFilesSchedule": { ... },

  "expireSnapshots": { ... },
  "expireSnapshotsSchedule": { ... },

  "orphanCleanup": { ... },
  "orphanCleanupSchedule": { ... },

  "rewriteManifests": { ... },
  "rewriteManifestsSchedule": { ... }
}
```

## Core Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique policy name |
| `description` | string | No | Policy description |
| `tablePattern` | string | Yes | Table pattern (e.g., `catalog.db.*`) |
| `enabled` | boolean | No | Enable/disable policy (default: true) |
| `priority` | integer | No | Higher priority wins when multiple policies match |
| `tags` | object | No | Key-value metadata |

---

## rewriteDataFiles

Compacts small files into larger files. Maps to Iceberg's [RewriteDataFiles](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/actions/RewriteDataFiles.html).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | string | `BINPACK` | `BINPACK`, `SORT`, or `ZORDER` |
| `sortOrder` | string[] | - | Columns to sort by (when strategy=SORT) |
| `zOrderColumns` | string[] | - | Columns for z-ordering (when strategy=ZORDER) |
| `targetFileSizeBytes` | long | table default | Target output file size |
| `maxFileGroupSizeBytes` | long | 100GB | Max bytes per file group |
| `maxConcurrentFileGroupRewrites` | integer | 5 | Max parallel file group rewrites |
| `partialProgressEnabled` | boolean | false | Commit after each file group |
| `partialProgressMaxCommits` | integer | 10 | Max partial commits |
| `partialProgressMaxFailedCommits` | integer | - | Max failed commits before abort |
| `filter` | string | - | Iceberg expression to filter files |
| `rewriteJobOrder` | string | `NONE` | `NONE`, `BYTES_ASC`, `BYTES_DESC`, `FILES_ASC`, `FILES_DESC` |
| `useStartingSequenceNumber` | boolean | true | Use starting sequence number for new files |
| `removeDanglingDeletes` | boolean | false | Remove dangling delete files |
| `outputSpecId` | integer | current | Output partition spec ID |

**Example:**

```json
{
  "rewriteDataFiles": {
    "strategy": "BINPACK",
    "targetFileSizeBytes": 134217728,
    "maxConcurrentFileGroupRewrites": 5
  }
}
```

---

## expireSnapshots

Removes old snapshots and associated data files. Maps to Iceberg's [ExpireSnapshots](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/actions/ExpireSnapshots.html).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retainLast` | integer | 5 | Minimum snapshots to retain |
| `maxSnapshotAge` | string | `P7D` | Max snapshot age (ISO-8601 duration) |
| `cleanExpiredMetadata` | boolean | false | Clean unused partition specs and schemas |
| `expireSnapshotId` | long | - | Expire a specific snapshot by ID (Spark only) |

**Example:**

```json
{
  "expireSnapshots": {
    "retainLast": 5,
    "maxSnapshotAge": "P7D",
    "cleanExpiredMetadata": false
  }
}
```

**Note:** `expireSnapshotId` is not supported by Trino.

---

## orphanCleanup

Removes files not referenced by any snapshot. Maps to Iceberg's [DeleteOrphanFiles](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/actions/DeleteOrphanFiles.html).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retentionPeriodInDays` | integer | 3 | Delete files older than N days |
| `location` | string | table root | Location to scan for orphans (Spark only) |
| `prefixMismatchMode` | string | `ERROR` | `ERROR`, `IGNORE`, or `DELETE` (Spark only) |
| `equalSchemes` | object | - | Map of equivalent URI schemes (Spark only) |
| `equalAuthorities` | object | - | Map of equivalent URI authorities (Spark only) |

**Example:**

```json
{
  "orphanCleanup": {
    "retentionPeriodInDays": 3
  }
}
```

**Note:** `location`, `prefixMismatchMode`, `equalSchemes`, and `equalAuthorities` are not supported by Trino.

---

## rewriteManifests

Rewrites manifest files for better query planning. Maps to Iceberg's [RewriteManifests](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/actions/RewriteManifests.html).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `specId` | integer | current | Partition spec ID to rewrite |
| `stagingLocation` | string | metadata location | Staging location for new manifests |
| `sortBy` | string[] | spec order | Partition fields to sort manifests by |
| `rewriteIf` | object | all | Filter which manifests to rewrite |

### rewriteIf (ManifestFilter)

Filter manifests based on ManifestFile schema fields. All fields are optional.

| Field | Type | Description |
|-------|------|-------------|
| `path` | string | Manifest file path |
| `length` | long | Manifest file size in bytes |
| `specId` | integer | Partition spec ID |
| `content` | string | `DATA` or `DELETES` |
| `sequenceNumber` | long | Sequence number of commit that added manifest |
| `minSequenceNumber` | long | Lowest sequence number of any live file |
| `snapshotId` | long | Snapshot that added the manifest |
| `addedFilesCount` | integer | Number of added files |
| `existingFilesCount` | integer | Number of existing files |
| `deletedFilesCount` | integer | Number of deleted files |
| `addedRowsCount` | long | Number of added rows |
| `existingRowsCount` | long | Number of existing rows |
| `deletedRowsCount` | long | Number of deleted rows |
| `firstRowId` | long | Starting row ID for new rows |
| `keyMetadata` | string | Encryption key metadata (hex) |
| `partitionSummaries` | array | Partition field summaries |

#### partitionSummaries (PartitionFieldSummary)

| Field | Type | Description |
|-------|------|-------------|
| `containsNull` | boolean | Whether partition contains null values |
| `containsNan` | boolean | Whether partition contains NaN values |
| `lowerBound` | string | Lower bound (hex encoded) |
| `upperBound` | string | Upper bound (hex encoded) |

**Example:**

```json
{
  "rewriteManifests": {
    "specId": 0,
    "rewriteIf": {
      "content": "DATA",
      "addedFilesCount": 0
    }
  }
}
```

**Note:** `rewriteIf`, `specId`, `stagingLocation`, and `sortBy` are not supported by Trino.

---

## Schedule

Each operation can have its own schedule.

| Field | Type | Description |
|-------|------|-------------|
| `cronExpression` | string | Cron expression (e.g., `0 2 * * *`) |
| `interval` | integer | Run every N days (alternative to cron) |
| `windowStart` | string | Earliest start time (`HH:mm`) |
| `windowEnd` | string | Latest start time (`HH:mm`) |
| `allowedDays` | string | Comma-separated days (e.g., `MONDAY,FRIDAY`) |
| `timeout` | integer | Operation timeout in hours |
| `priority` | integer | Schedule priority |
| `enabled` | boolean | Enable/disable schedule |

**Example:**

```json
{
  "rewriteDataFilesSchedule": {
    "cronExpression": "0 2 * * *",
    "windowStart": "02:00",
    "windowEnd": "06:00",
    "enabled": true
  }
}
```

---

## Engine Support

Not all fields are supported by all engines.

| Field | Spark | Trino |
|-------|-------|-------|
| **rewriteDataFiles** | | |
| All fields | Yes | No (only `targetFileSizeBytes`, `filter`) |
| **expireSnapshots** | | |
| `retainLast`, `maxSnapshotAge`, `cleanExpiredMetadata` | Yes | Yes |
| `expireSnapshotId` | Yes | No |
| **orphanCleanup** | | |
| `retentionPeriodInDays` | Yes | Yes |
| `location`, `prefixMismatchMode`, `equalSchemes`, `equalAuthorities` | Yes | No |
| **rewriteManifests** | | |
| All fields | Yes | No (Trino takes no parameters) |
