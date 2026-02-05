#!/bin/bash
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

# Setup Polaris catalog and permissions for demo
# This script should be run after Polaris starts but before creating tables

set -e

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-secret}"
CATALOG_NAME="${CATALOG_NAME:-demo}"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_BUCKET="${S3_BUCKET:-warehouse}"

echo "Configuring Polaris catalog at $POLARIS_URL..."

# Get OAuth token
TOKEN=$(curl -s -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials" \
  -d "client_id=$CLIENT_ID" \
  -d "client_secret=$CLIENT_SECRET" \
  -d "scope=PRINCIPAL_ROLE:ALL" | jq -r '.access_token')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "Failed to get OAuth token"
  exit 1
fi

echo "Got OAuth token"

# Check if catalog exists
CATALOG_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $TOKEN" \
  "$POLARIS_URL/api/management/v1/catalogs/$CATALOG_NAME")

if [ "$CATALOG_EXISTS" = "200" ]; then
  echo "Catalog '$CATALOG_NAME' already exists"
else
  echo "Creating catalog '$CATALOG_NAME'..."
  curl -s -X POST "$POLARIS_URL/api/management/v1/catalogs" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"catalog\": {
        \"name\": \"$CATALOG_NAME\",
        \"type\": \"INTERNAL\",
        \"properties\": {
          \"default-base-location\": \"s3://$S3_BUCKET/\"
        },
        \"storageConfigInfo\": {
          \"storageType\": \"S3\",
          \"allowedLocations\": [\"s3://$S3_BUCKET/\"]
        }
      }
    }" | jq .
fi

# Create admin catalog role if not exists
echo "Creating catalog role 'admin'..."
curl -s -X POST "$POLARIS_URL/api/management/v1/catalogs/$CATALOG_NAME/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "admin"}}' 2>/dev/null || true

# Grant full catalog privileges
echo "Granting catalog privileges..."
curl -s -X PUT "$POLARIS_URL/api/management/v1/catalogs/$CATALOG_NAME/catalog-roles/admin/grants" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' 2>/dev/null || true

# Assign to service_admin principal role
echo "Assigning role to service_admin..."
curl -s -X PUT "$POLARIS_URL/api/management/v1/principal-roles/service_admin/catalog-roles/$CATALOG_NAME" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "admin"}}' 2>/dev/null || true

echo "Polaris catalog '$CATALOG_NAME' configured successfully"
