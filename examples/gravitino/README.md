# Gravitino Example

[Apache Gravitino](https://gravitino.apache.org/) Iceberg REST Server provides a standalone implementation of the Iceberg REST Catalog specification with JDBC-backed catalog storage.

## Services

| Service | Port | URL |
|---------|------|-----|
| Floe | 9091 | http://localhost:9091 |
| Gravitino Iceberg REST | 9001 | http://localhost:9001 |
| MinIO Console | 19001 | http://localhost:19001 (admin/password) |
| Livy | 8998 | http://localhost:8998 |
| Trino | 8085 | http://localhost:8085 (with `-trino` target) |
