# Monitoring

Floe exposes Prometheus metrics and health endpoints.

## Health Checks

```bash
curl http://localhost:9091/health
curl http://localhost:9091/health/live
curl http://localhost:9091/health/ready
```

## Metrics

Prometheus-compatible metrics at `/metrics`:

```bash
curl http://localhost:9091/metrics
```

### Custom Floe Metrics

| Metric | Description |
|--------|-------------|
| `floe_maintenance_triggers_total` | Maintenance operations triggered |
| `floe_maintenance_success_total` | Successful operations |
| `floe_maintenance_failures_total` | Failed operations |
| `floe_operation_duration_seconds` | Operation duration by type |
| `floe_policies_created_total` | Policies created |
| `floe_policies_updated_total` | Policies updated |
| `floe_policies_deleted_total` | Policies deleted |
| `floe_policy_matches_total` | Policy matches |
| `floe_active_policies` | Current active policies (gauge) |
| `floe_running_operations` | Currently running operations (gauge) |

### JVM & HTTP Metrics

Standard Micrometer metrics are also available:
- `jvm_memory_*` - JVM memory
- `jvm_gc_*` - Garbage collection
