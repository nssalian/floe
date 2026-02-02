# Advanced Example (REST Catalog)

This example uses the REST catalog and enables advanced features (health persistence, signal-based triggers, and scheduler budgets).

## Start

```bash
make example-advanced
```

By default, this runs demo **tables only** and then creates the
`advanced-maintenance` policy. Demo policies are skipped.
Access:
- Floe UI: http://localhost:9091
- Floe API: http://localhost:9091/api
- Livy: http://localhost:8998
- Trino (optional): http://localhost:8085

## What This Enables

- Health report persistence and history
- Maintenance debt score prioritization (auto-mode)
- Condition-based triggering (`triggerConditions`)
- Budget limits and throttling/backoff

## Sample Policy (Health + Triggers)

```bash
curl -s -X POST "http://localhost:9091/api/v1/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "advanced-maintenance",
    "tablePattern": "demo.test.*",
    "priority": 100,
    "rewriteDataFiles": {"strategy": "BINPACK", "targetFileSizeBytes": 134217728},
    "expireSnapshots": {"retainLast": 10, "maxSnapshotAge": "P7D"},
    "orphanCleanup": {"retentionPeriodInDays": 3},
    "rewriteManifests": {},
    "healthThresholds": {
      "smallFilePercentWarning": 20.0,
      "smallFilePercentCritical": 50.0,
      "deleteFileCountWarning": 100,
      "deleteFileCountCritical": 500,
      "snapshotCountWarning": 50,
      "snapshotCountCritical": 200
    },
    "triggerConditions": {
      "smallFilePercentageAbove": 20.0,
      "deleteFileCountAbove": 50,
      "minIntervalMinutes": 60,
      "criticalPipeline": true,
      "criticalPipelineMaxDelayMinutes": 360
    }
  }'
```

## Health History

```bash
curl http://localhost:9091/api/v1/tables/test/events/health
curl http://localhost:9091/api/v1/tables/test/events/health/history?limit=10
```

## Trigger Status

```bash
curl http://localhost:9091/api/v1/tables/test/events/trigger-status
```

## Stop

```bash
make clean
```
