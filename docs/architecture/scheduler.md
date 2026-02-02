# Scheduler

The Floe scheduler automatically triggers maintenance operations based on policy schedules.

## How It Works

Every minute, the scheduler:

1. **Acquires lock** - Gets distributed lock (if enabled) to prevent duplicate runs
2. **Lists policies** - Fetches all enabled policies
3. **Checks schedules** - For each policy, checks each operation's schedule
4. **Finds tables** - Lists tables matching the policy pattern
5. **Triggers maintenance** - Runs due operations on matching tables
6. **Records execution** - Stores completion time to calculate next run

The scheduler also applies:

- Budget limits (tables/operations/bytes per hour)
- Backoff after repeated failures
- Throttling after repeated zero-change runs
- Auto-mode prioritization based on maintenance debt score

## Configuration

```properties
# Enable/disable the scheduler
floe.scheduler.enabled=true

# Enable/disable condition-based triggering (default: true)
floe.scheduler.condition-based-triggering-enabled=true

# Enable distributed locking for multi-replica deployments for future use
floe.scheduler.distributed-lock-enabled=true

# Budget limits (0 = unlimited)
floe.scheduler.max-tables-per-poll=0
floe.scheduler.max-operations-per-poll=0
floe.scheduler.max-bytes-per-hour=0

# Backoff and throttling
floe.scheduler.failure-backoff-threshold=3
floe.scheduler.failure-backoff-hours=6
floe.scheduler.zero-change-threshold=5
floe.scheduler.zero-change-frequency-reduction-percent=50
floe.scheduler.zero-change-min-interval-hours=6
```

## Schedule Configuration

Each operation in a policy can have its own schedule:

```json
{
  "rewriteDataFilesSchedule": {
    "cronExpression": "0 2 * * *",
    "windowStart": "02:00",
    "windowEnd": "06:00",
    "allowedDays": "MONDAY,WEDNESDAY,FRIDAY",
    "timeout": 4,
    "enabled": true
  }
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `cronExpression` | string | Cron schedule (e.g., `0 2 * * *` = 2 AM daily) |
| `interval` | int | Alternative: run every N days |
| `windowStart` | string | Earliest start time (HH:mm) |
| `windowEnd` | string | Latest start time (HH:mm) |
| `allowedDays` | string | Comma-separated days (e.g., `MONDAY,FRIDAY`) |
| `timeout` | int | Timeout in hours |
| `enabled` | boolean | Enable/disable this schedule |

## Distributed Locking

For deployments with multiple Floe replicas, enable distributed locking to ensure only one replica runs the scheduler:

```properties
floe.scheduler.distributed-lock-enabled=true
```

### How It Works

1. Before each poll cycle, scheduler attempts to acquire the `floe-scheduler` lock
2. Uses PostgreSQL advisory locks (fast, no table overhead)
3. If lock is held by another replica, poll is skipped
4. Lock auto-releases on connection close (crash-safe)

### Requirements

- PostgreSQL storage backend (`floe.store.type=POSTGRES`)
- Multiple replicas sharing the same database

### Single-Replica Mode

When `distributed-lock-enabled=false` (default):

- Uses local `AtomicBoolean` for concurrency within JVM
- Suitable for single-replica deployments
- No external dependencies

## Execution Tracking

The scheduler tracks when each operation last ran for each table to prevent duplicate executions.

### Execution Record

```
Policy: daily-compaction
Operation: REWRITE_DATA_FILES
Table: warehouse.analytics.events
Last Run: 2024-01-15T02:00:00Z
Next Run: 2024-01-16T02:00:00Z
```

### Calculation

1. Check if operation is due: `now >= nextRun`
2. If cron: parse expression to find next occurrence
3. If interval: `lastRun + interval`
4. Apply maintenance window constraints

## Manual Trigger

The scheduler handles automatic execution. To trigger maintenance manually, use the API:

```bash
curl -X POST http://localhost:9091/api/v1/maintenance/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "warehouse",
    "namespace": "analytics",
    "table": "events"
  }'
```

## Trigger Status

Check whether operations would trigger for a table:

```bash
curl http://localhost:9091/api/v1/tables/{namespace}/{table}/trigger-status
```

Returns per-operation status including conditions met/unmet and next eligible time.
