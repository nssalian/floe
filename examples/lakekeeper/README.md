# Lakekeeper Example

[Lakekeeper](https://lakekeeper.io/) is a fast, secure Apache Iceberg REST Catalog written in Rust. It provides full Iceberg REST Catalog specification compliance with OAuth2 authentication and credential vending support.

## Services

| Service | Port | URL |
|---------|------|-----|
| Floe | 9091 | http://localhost:9091 |
| Lakekeeper | 8181 | http://localhost:8181 |
| MinIO Console | 9001 | http://localhost:9001 (admin/password) |
| Livy | 8998 | http://localhost:8998 |
| Trino | 8085 | http://localhost:8085 (with `-trino` target) |
