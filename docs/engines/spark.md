# Apache Spark Engine

Apache Spark for Iceberg maintenance operations, submitted via Apache Livy REST API.

## Operation Support

Spark has full support for all Iceberg maintenance operations across all supported catalog types (REST, Hive, Nessie, Polaris, Lakekeeper, Gravitino).

Spark's `SparkActions` API provides the most comprehensive maintenance capabilities. Floe submits Spark jobs through Apache Livy, 
which provides a REST interface to Spark clusters.

## Configuration

```bash
FLOE_ENGINE_TYPE=SPARK
FLOE_LIVY_URL=http://livy:8998
```

## Options

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `floe.livy.url` | Yes | - | Livy REST API URL |
| `floe.livy.job.jar` | No | `local:///opt/floe/floe-maintenance-job.jar` | Path to maintenance job JAR |
| `floe.livy.job.main-class` | No | `com.floe.spark.job.MaintenanceJob` | Main class |
| `floe.livy.driver-memory` | No | `2g` | Spark driver memory |
| `floe.livy.executor-memory` | No | `2g` | Spark executor memory |

## Job Lifecycle

1. Floe submits batch job to Livy
2. Livy creates Spark application
3. Job executes maintenance operation
4. Floe polls for completion
5. Results returned to Floe

## Monitoring

### Livy UI

Access at `http://livy:8998/ui` to see:

- Active sessions
- Batch job history
- Logs

## Resources

- [Apache Livy](https://livy.apache.org/)
- [Iceberg Spark Procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Iceberg Table Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Spark Actions API](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/spark/actions/SparkActions.html)
