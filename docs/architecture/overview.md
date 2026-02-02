# Architecture

Floe is a modular platform for Apache Iceberg table maintenance. Every core component can be swapped to integrate with your infrastructure.

## System Architecture

![Floe Architecture](../assets/architecture.png){width="600"}

## Extension Points

External integrations are pluggable with configuration:

| Component | Interface | Implementations |
|-----------|-----------|-----------------|
| **Catalog** | `CatalogClient` | Iceberg REST, Hive, Nessie, Polaris, Lakekeeper, Gravitino |
| **Engine** | `ExecutionEngine` | Spark, Trino |
| **Store** | `PolicyStore`, `OperationStore` | PostgreSQL, Memory |

## For External Scheduler Integration

Disable the built-in scheduler and trigger maintenance via API (requires a matching policy):

![For external schedulers](../assets/external-scheduler.svg){width="600"}

```bash
# Disable built-in scheduler
# The demos enable the scheduler by default
FLOE_SCHEDULER_ENABLED=false

# Trigger from external orchestrator
curl -X POST http://floe:9091/api/v1/maintenance/trigger \
  -d '{"catalog": "demo", "namespace": "db", "table": "events"}'
```

## Data Flow

```
    1. Policy created (patterns can cover many tables)
           |
           v
    2. Scheduler triggers (auto-mode) or API call
           |
           v
    3. Table health assessed (metrics from Iceberg metadata)
           |
           v
    4. Health report persisted in TableHealthStore (if enabled)
           |
           v
    5. Orchestrator validates matching policy and table
           |
           v
    6. MaintenancePlanner selects operations from health issues
           |
           v
    7. TriggerEvaluator gates operations (min interval / signals / critical deadline)
           |
           v
    8. MaintenanceDebtScore prioritizes tables (auto-mode)
           |
           v
    9. Engine executes maintenance (Spark or Trino)
           |
           +--> Spark: rewriteDataFiles(), expireSnapshots(),
           |           deleteOrphanFiles(), rewriteManifests()
           |
           +--> Trino: ALTER TABLE ... EXECUTE optimize,
           |           expire_snapshots, remove_orphan_files
           |
           v
    10. Operation recorded in Store
           |
           v
    11. Event emitted (if logging is enabled)
```
### Prioritization (Auto-Mode)

- The scheduler computes a maintenance debt score per table using health issues and recent
  operation outcomes (failure streaks, zero-change runs).
- Higher debt scores are prioritized when budgets (tables/operations/bytes) are enforced.
- Throttling and backoff can defer tables after repeated zero-change runs or failures.

### Policy Requirement

Manual or scheduled triggers require a matching policy. Use table patterns (e.g., `demo.test.*`)
to avoid per-table policies.


## Learn More

- [Scheduler](scheduler.md) - Cron schedules and distributed locking
- [Engines](../engines/index.md) - Spark and Trino configuration
- [Catalogs](../catalogs/index.md) - Catalog setup
- [Monitoring](../operations/monitoring.md) - Metrics and health checks
