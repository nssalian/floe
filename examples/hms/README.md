# Hive Metastore Example

[Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-Metastore) is the most widely-adopted catalog for Iceberg tables. Compatible with Spark, Trino, Presto, Hive, and Flink.

## Services

| Service | Port | URL                                    |
|---------|------|----------------------------------------|
| Floe | 9091 | http://localhost:9091                  |
| Hive Metastore | 9083 | thrift://localhost:9083                |
| MinIO Console | 9001 | http://localhost:9001 (admin/password) |
| Livy | 8998 | http://localhost:8998                  |
| Trino | 8085 | http://localhost:8085 (with `admin`)    |
