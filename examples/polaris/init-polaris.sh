#!/bin/sh
set -e

echo "Waiting for Polaris to be ready..."
sleep 5

echo "Getting OAuth token..."
TOKEN=$(curl -s -X POST "http://polaris:8181/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials" \
  -d "client_id=root" \
  -d "client_secret=secret" \
  -d "scope=PRINCIPAL_ROLE:ALL" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "$TOKEN" ]; then
  echo "ERROR: Failed to get OAuth token"
  exit 1
fi
echo "Got token: ${TOKEN:0:20}..."

echo "Creating catalog 'demo'..."
# INTERNAL catalog with MinIO S3 endpoint (no roleArn needed for MinIO)
# endpoint = external (from client perspective), endpointInternal = from Polaris server
curl -s -X POST "http://polaris:8181/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalog":{"name":"demo","type":"INTERNAL","properties":{"default-base-location":"s3://warehouse/"},"storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://warehouse/"],"endpoint":"http://minio:9000","endpointInternal":"http://minio:9000","pathStyleAccess":true}}}'
echo ""

echo "Creating admin catalog role..."
curl -s -X POST "http://polaris:8181/api/management/v1/catalogs/demo/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"admin"}}'
echo ""

echo "Granting CATALOG_MANAGE_CONTENT privilege..."
curl -s -X PUT "http://polaris:8181/api/management/v1/catalogs/demo/catalog-roles/admin/grants" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'
echo ""

echo "Assigning admin role to service_admin principal..."
curl -s -X PUT "http://polaris:8181/api/management/v1/principal-roles/service_admin/catalog-roles/demo" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"admin"}}'
echo ""

echo "Polaris catalog 'demo' configured successfully!"
