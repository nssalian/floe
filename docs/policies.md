# Policies

Policies define automated maintenance for Iceberg tables via pattern matching. Each policy specifies which tables to maintain, what operations to run, and when to execute them.

## Policy Structure

```json
{
  "name": "policy-name",
  "tablePattern": "catalog.namespace.*",
  "enabled": true,
  "priority": 100,

  "rewriteDataFiles": { ... },
  "rewriteDataFilesSchedule": { ... },

  "expireSnapshots": { ... },
  "expireSnapshotsSchedule": { ... },

  "orphanCleanup": { ... },
  "orphanCleanupSchedule": { ... },

  "rewriteManifests": { ... },
  "rewriteManifestsSchedule": { ... },

  "healthThresholds": { ... },
  "triggerConditions": { ... }
}
```

## Core Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique policy identifier |
| `description` | string | No | Policy description |
| `tablePattern` | string | Yes | Pattern to match tables (e.g., `catalog.db.*`, `/regex/`, `*`) |
| `enabled` | boolean | No | Enable/disable (default: true) |
| `priority` | integer | No | Higher wins when multiple policies match |
| `tags` | object | No | Metadata key-value pairs |

### Table Patterns

| Pattern | Matches |
|---------|---------|
| `*` | All tables |
| `catalog.*.*` | All tables in catalog |
| `catalog.namespace.*` | All tables in namespace |
| `catalog.namespace.table` | Specific table |
| `catalog.prod_*.*` | Glob patterns (`*` = wildcard, `?` = single char) |
| `/catalog\.prod_\d+\..*/` | Regex (enclosed in `/`) |

**Pattern Resolution:**
- Priority: Higher `priority` value wins
- Tiebreaker: More specific pattern wins (exact > glob > wildcard)
- Effective priority = `priority × 1000 + specificity_score`

---

## Operations

### rewriteDataFiles (Compaction)

Compacts small files. Maps to [RewriteDataFiles](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/actions/RewriteDataFiles.html).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | string | `BINPACK` | `BINPACK`, `SORT`, or `ZORDER` |
| `sortOrder` | string[] | - | Sort columns (SORT strategy) |
| `zOrderColumns` | string[] | - | Z-order columns (ZORDER strategy) |
| `targetFileSizeBytes` | long | table default | Target file size (e.g., 134217728 = 128 MB) |
| `maxFileGroupSizeBytes` | long | 100 GB | Max bytes per file group |
| `maxConcurrentFileGroupRewrites` | integer | 5 | Parallelism level |
| `partialProgressEnabled` | boolean | false | Commit after each file group |
| `partialProgressMaxCommits` | integer | 10 | Max partial commits |
| `partialProgressMaxFailedCommits` | integer | - | Max failures before abort |
| `filter` | string | - | Iceberg expression filter |
| `rewriteJobOrder` | string | `NONE` | `NONE`, `BYTES_ASC`, `BYTES_DESC`, `FILES_ASC`, `FILES_DESC` |
| `useStartingSequenceNumber` | boolean | true | Use starting sequence numbers |
| `removeDanglingDeletes` | boolean | false | Remove dangling deletes |
| `outputSpecId` | integer | current | Output partition spec ID |

---

### expireSnapshots

Removes old snapshots. Maps to [ExpireSnapshots](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/actions/ExpireSnapshots.html).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retainLast` | integer | 5 | Min snapshots to keep |
| `maxSnapshotAge` | string | `P7D` | Max age (ISO-8601: `P7D` = 7 days) |
| `cleanExpiredMetadata` | boolean | false | Clean unused specs/schemas |
| `expireSnapshotId` | long | - | Expire specific snapshot (Spark only) |

---

### orphanCleanup

Removes unreferenced files. Maps to [DeleteOrphanFiles](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/actions/DeleteOrphanFiles.html).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retentionPeriodInDays` | integer | 3 | Delete files older than N days |
| `location` | string | table root | Scan location (Spark only) |
| `prefixMismatchMode` | string | `ERROR` | `ERROR`, `IGNORE`, `DELETE` (Spark only) |
| `equalSchemes` | object | - | URI scheme equivalence map (Spark only) |
| `equalAuthorities` | object | - | URI authority equivalence map (Spark only) |

---

### rewriteManifests

Optimizes manifest files. Maps to [RewriteManifests](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/actions/RewriteManifests.html).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `specId` | integer | current | Partition spec ID |
| `stagingLocation` | string | metadata location | Staging directory |
| `sortBy` | string[] | spec order | Sort by partition fields |
| `rewriteIf` | object | all | Manifest filter (see below) |

#### rewriteIf Filter Fields

| Field | Type | Description |
|-------|------|-------------|
| `path` | string | Manifest file path |
| `length` | long | File size in bytes |
| `specId` | integer | Partition spec ID |
| `content` | string | `DATA` or `DELETES` |
| `sequenceNumber` | long | Commit sequence number |
| `minSequenceNumber` | long | Lowest sequence number |
| `snapshotId` | long | Snapshot ID |
| `addedFilesCount` | integer | Number of added files |
| `existingFilesCount` | integer | Number of existing files |
| `deletedFilesCount` | integer | Number of deleted files |
| `addedRowsCount` | long | Added rows |
| `existingRowsCount` | long | Existing rows |
| `deletedRowsCount` | long | Deleted rows |
| `partitionSummaries` | array | Partition summaries (containsNull, containsNan, lowerBound, upperBound) |

---

## Scheduling

Each operation has independent schedule configuration.

| Field | Type | Description |
|-------|------|-------------|
| `cronExpression` | string | Cron expression (e.g., `0 2 * * *`) |
| `interval` | integer | Interval in days (e.g., `1` = daily, `7` = weekly) |
| `windowStart` | string | Start time `HH:mm` (e.g., `09:00`) |
| `windowEnd` | string | End time `HH:mm` (e.g., `17:00`) |
| `allowedDays` | string | Comma-separated days (e.g., `MONDAY,FRIDAY`) |
| `timeout` | integer | Timeout in hours (e.g., `2` = 2 hours) |
| `priority` | integer | Schedule priority |
| `enabled` | boolean | Enable/disable |

**Interval vs Cron:**
- Use `interval` for periodic execution (every N hours/days)
- Use `cronExpression` for specific times (daily at 2am, weekly on Sunday)

---

## triggerConditions (Optional)

Gates operations based on table health. If null, operations run whenever scheduled. If set, conditions must be met (OR/AND logic).

| Field | Type | Description |
|-------|------|-------------|
| `smallFilePercentageAbove` | double | Trigger when small file % exceeds value |
| `smallFileCountAbove` | long | Trigger when small file count exceeds |
| `totalFileSizeAboveBytes` | long | Trigger when total size exceeds |
| `deleteFileCountAbove` | integer | Trigger when delete files exceed |
| `deleteFileRatioAbove` | double | Trigger when delete ratio exceeds |
| `snapshotCountAbove` | integer | Trigger when snapshot count exceeds |
| `snapshotAgeAboveDays` | integer | Trigger when oldest snapshot age exceeds |
| `partitionCountAbove` | integer | Trigger when partition count exceeds |
| `partitionSkewAbove` | double | Trigger when partition skew exceeds |
| `manifestSizeAboveBytes` | long | Trigger when manifest size exceeds |
| `minIntervalMinutes` | integer | Min time between operations (global) |
| `perOperationMinIntervalMinutes` | object | Per-operation intervals (map: OperationType → minutes) |
| `criticalPipeline` | boolean | Force execution if max delay exceeded |
| `criticalPipelineMaxDelayMinutes` | integer | Max delay before forcing |
| `triggerLogic` | string | `OR` (any condition) or `AND` (all conditions) |

---

## healthThresholds (Optional)

Custom health assessment thresholds. If null, uses system defaults.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `smallFileSizeBytes` | long | 104857600 | Size defining "small" file (100 MB) |
| `smallFilePercentWarning` | double | 20.0 | Warning threshold % |
| `smallFilePercentCritical` | double | 50.0 | Critical threshold % |
| `largeFileSizeBytes` | long | 1073741824 | Size defining "large" file (1 GB) |
| `largeFilePercentWarning` | double | 20.0 | Warning threshold % |
| `largeFilePercentCritical` | double | 50.0 | Critical threshold % |
| `fileCountWarning` | int | 10000 | Warning file count |
| `fileCountCritical` | int | 50000 | Critical file count |
| `snapshotCountWarning` | int | 100 | Warning snapshot count |
| `snapshotCountCritical` | int | 500 | Critical snapshot count |
| `snapshotAgeWarningDays` | int | 7 | Warning age (days) |
| `snapshotAgeCriticalDays` | int | 30 | Critical age (days) |
| `deleteFileCountWarning` | int | 100 | Warning delete file count |
| `deleteFileCountCritical` | int | 500 | Critical delete file count |
| `deleteFileRatioWarning` | double | 0.10 | Warning ratio (deletes/data) |
| `deleteFileRatioCritical` | double | 0.25 | Critical ratio |
| `manifestCountWarning` | int | 100 | Warning manifest count |
| `manifestCountCritical` | int | 500 | Critical manifest count |
| `manifestSizeWarningBytes` | long | 104857600 | Warning size (100 MB) |
| `manifestSizeCriticalBytes` | long | 524288000 | Critical size (500 MB) |
| `partitionCountWarning` | int | 5000 | Warning partition count |
| `partitionCountCritical` | int | 10000 | Critical partition count |
| `partitionSkewWarning` | double | 3.0 | Warning skew (max/avg) |
| `partitionSkewCritical` | double | 10.0 | Critical skew |
| `staleMetadataWarningDays` | int | 7 | Warning staleness (days) |
| `staleMetadataCriticalDays` | int | 30 | Critical staleness (days) |

---

## Examples

### Basic Compaction

```json
{
  "name": "events-compaction",
  "tablePattern": "prod.events.*",
  "priority": 100,
  "rewriteDataFiles": {
    "strategy": "BINPACK",
    "targetFileSizeBytes": 134217728
  },
  "rewriteDataFilesSchedule": {
    "interval": "PT6H",
    "enabled": true
  }
}
```

### Snapshot Expiration with Triggers

```json
{
  "name": "snapshot-cleanup",
  "tablePattern": "prod.*.*",
  "priority": 50,
  "expireSnapshots": {
    "retainLast": 10,
    "maxSnapshotAge": "P7D"
  },
  "expireSnapshotsSchedule": {
    "cronExpression": "0 2 * * *",
    "enabled": true
  },
  "triggerConditions": {
    "snapshotCountAbove": 50,
    "minIntervalMinutes": 1440
  }
}
```

### Multi-Operation with Time Windows

```json
{
  "name": "full-maintenance",
  "tablePattern": "prod.analytics.*",
  "priority": 100,
  "rewriteDataFiles": {
    "strategy": "BINPACK",
    "targetFileSizeBytes": 268435456,
    "maxConcurrentFileGroupRewrites": 10
  },
  "rewriteDataFilesSchedule": {
    "interval": "PT2H",
    "windowStart": "09:00",
    "windowEnd": "17:00",
    "allowedDays": "MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY",
    "enabled": true
  },
  "expireSnapshots": {
    "retainLast": 10,
    "maxSnapshotAge": "P7D"
  },
  "expireSnapshotsSchedule": {
    "cronExpression": "0 2 * * SUN",
    "enabled": true
  },
  "orphanCleanup": {
    "retentionPeriodInDays": 3
  },
  "orphanCleanupSchedule": {
    "interval": "P7D",
    "enabled": true
  }
}
```

### Critical Pipeline (Guaranteed Execution)

```json
{
  "name": "critical-orders",
  "tablePattern": "prod.orders.*",
  "priority": 200,
  "rewriteDataFiles": {
    "strategy": "BINPACK"
  },
  "rewriteDataFilesSchedule": {
    "interval": "PT1H",
    "enabled": true
  },
  "triggerConditions": {
    "smallFilePercentageAbove": 30,
    "criticalPipeline": true,
    "criticalPipelineMaxDelayMinutes": 360
  }
}
```

---

## Engine Compatibility

| Feature | Spark | Trino |
|---------|-------|-------|
| **rewriteDataFiles** | Full | Partial (`targetFileSizeBytes`, `filter` only) |
| **expireSnapshots** | Full | Partial (no `expireSnapshotId`) |
| **orphanCleanup** | Full | Basic only (no `location`, `prefixMismatchMode`, etc.) |
| **rewriteManifests** | Full | No parameters |

---

## See Also

- [Configuration](getting-started/configuration.md)
- [Architecture](architecture/overview.md)
- [Iceberg Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
