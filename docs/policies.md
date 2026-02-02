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
  "rewriteManifestsSchedule": { ... },

  "healthThresholds": { ... }
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

## healthThresholds

Optional overrides for health issue thresholds used by auto-mode and recommendations. All fields are optional; unspecified fields use system defaults.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `smallFileSizeBytes` | long | 104857600 | Size below which a file is considered "small" (100 MB) |
| `smallFilePercentWarning` | double | 20.0 | Warning threshold for small file percentage |
| `smallFilePercentCritical` | double | 50.0 | Critical threshold for small file percentage |
| `largeFileSizeBytes` | long | 1073741824 | Size above which a file is considered "large" (1 GB) |
| `largeFilePercentWarning` | double | 20.0 | Warning threshold for large file percentage |
| `largeFilePercentCritical` | double | 50.0 | Critical threshold for large file percentage |
| `fileCountWarning` | int | 10000 | Warning threshold for total file count |
| `fileCountCritical` | int | 50000 | Critical threshold for total file count |
| `snapshotCountWarning` | int | 100 | Warning threshold for snapshot count |
| `snapshotCountCritical` | int | 500 | Critical threshold for snapshot count |
| `snapshotAgeWarningDays` | int | 7 | Warning threshold for oldest snapshot age (days) |
| `snapshotAgeCriticalDays` | int | 30 | Critical threshold for oldest snapshot age (days) |
| `deleteFileCountWarning` | int | 100 | Warning threshold for delete file count |
| `deleteFileCountCritical` | int | 500 | Critical threshold for delete file count |
| `deleteFileRatioWarning` | double | 0.10 | Warning threshold for delete file ratio (deleteFiles/dataFiles) |
| `deleteFileRatioCritical` | double | 0.25 | Critical threshold for delete file ratio |
| `manifestCountWarning` | int | 100 | Warning threshold for manifest count |
| `manifestCountCritical` | int | 500 | Critical threshold for manifest count |
| `manifestSizeWarningBytes` | long | 104857600 | Warning threshold for total manifest size (100 MB) |
| `manifestSizeCriticalBytes` | long | 524288000 | Critical threshold for total manifest size (500 MB) |
| `partitionCountWarning` | int | 5000 | Warning threshold for partition count |
| `partitionCountCritical` | int | 10000 | Critical threshold for partition count |
| `partitionSkewWarning` | double | 3.0 | Warning threshold for partition skew (max/avg files per partition) |
| `partitionSkewCritical` | double | 10.0 | Critical threshold for partition skew |
| `staleMetadataWarningDays` | int | 7 | Warning threshold for days since last write |
| `staleMetadataCriticalDays` | int | 30 | Critical threshold for days since last write |

**Example - Custom thresholds for high-volume tables:**

```json
{
  "healthThresholds": {
    "smallFilePercentWarning": 15.0,
    "smallFilePercentCritical": 40.0,
    "snapshotCountWarning": 50,
    "snapshotCountCritical": 200,
    "deleteFileRatioWarning": 0.05,
    "deleteFileRatioCritical": 0.15,
    "fileCountWarning": 20000,
    "fileCountCritical": 100000
  }
}
```

**Preset configurations:**

- **defaults()** - Standard thresholds suitable for most tables
- **strict()** - Tighter thresholds for high-performance tables requiring aggressive maintenance
- **relaxed()** - Looser thresholds for archival or low-priority tables

---

## triggerConditions

Signal-based triggering conditions that gate when operations actually execute. By default, conditions use OR logic (any condition met triggers). If `triggerLogic` is set to `AND`, all configured conditions must be met. If `triggerConditions` is not set (null), operations run whenever the schedule is due (backward-compatible cron behavior).

This enables data-driven maintenance: instead of running compaction every day regardless of need, Floe evaluates table health and only triggers when conditions warrant action.

| Field | Type | Description |
|-------|------|-------------|
| `smallFilePercentageAbove` | double | Trigger when small file % exceeds value |
| `smallFileCountAbove` | long | Trigger when small file count exceeds value |
| `totalFileSizeAboveBytes` | long | Trigger when total data size exceeds value |
| `deleteFileCountAbove` | integer | Trigger when delete file count exceeds value |
| `deleteFileRatioAbove` | double | Trigger when delete file ratio exceeds value |
| `snapshotCountAbove` | integer | Trigger when snapshot count exceeds value |
| `snapshotAgeAboveDays` | integer | Trigger when oldest snapshot age exceeds value |
| `partitionCountAbove` | integer | Trigger when partition count exceeds value |
| `partitionSkewAbove` | double | Trigger when partition skew exceeds value |
| `manifestSizeAboveBytes` | long | Trigger when total manifest size exceeds value |
| `minIntervalMinutes` | integer | Minimum time between operations (global) |
| `perOperationMinIntervalMinutes` | object | Per-operation minimum intervals |
| `criticalPipeline` | boolean | Force trigger when max delay exceeded |
| `criticalPipelineMaxDelayMinutes` | integer | Max delay before forcing (for critical pipelines) |
| `triggerLogic` | string | `OR` (default) or `AND` |

**Example - Signal-based compaction:**

```json
{
  "triggerConditions": {
    "smallFilePercentageAbove": 20,
    "smallFileCountAbove": 100,
    "deleteFileCountAbove": 50,
    "minIntervalMinutes": 60,
    "perOperationMinIntervalMinutes": {
      "REWRITE_DATA_FILES": 120,
      "EXPIRE_SNAPSHOTS": 1440
    },
    "triggerLogic": "OR"
  }
}
```

**Example - Critical pipeline with forced execution:**

```json
{
  "triggerConditions": {
    "smallFilePercentageAbove": 30,
    "criticalPipeline": true,
    "criticalPipelineMaxDelayMinutes": 360,
    "triggerLogic": "AND"
  }
}
```

When `criticalPipeline` is true and the time since last operation exceeds `criticalPipelineMaxDelayMinutes`, the operation is forced even if no conditions are met. This ensures critical tables are never left unmaintained for too long.

**Behavior notes:**
- `null` triggerConditions = always trigger when schedule is due (cron-based)
- `triggerLogic` = `OR` (default) or `AND` (all conditions required)
- Any condition met = operation triggers when `triggerLogic` is `OR`
- `minIntervalMinutes` prevents rapid re-execution even if conditions are met
- Health data is required to evaluate conditions; if unavailable, conditions are not evaluated

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
