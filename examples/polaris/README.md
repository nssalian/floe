# Polaris Example

[Apache Polaris](https://polaris.apache.org/) provides centralized access control across query engines with OAuth2 authentication and fine-grained permissions.

## Services

| Service | Port | URL |
|---------|------|-----|
| Floe | 9091 | http://localhost:9091 |
| Polaris | 8181 | http://localhost:8181 |
| MinIO Console | 9001 | http://localhost:9001 (admin/password) |
| Livy | 8998 | http://localhost:8998 |
| Trino | 8085 | http://localhost:8085 (with `-trino` target) |

## Notes

- The `polaris-init` container automatically creates the `demo` catalog with OAuth2 credentials (`root:secret`)
- For Polaris, `FLOE_CATALOG_WAREHOUSE` is the catalog name (`demo`), not an S3 path
