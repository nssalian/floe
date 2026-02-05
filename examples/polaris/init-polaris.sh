#!/bin/sh
#
# Copyright 2026 The Floe Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
