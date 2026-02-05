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

#
# Trino entrypoint script for dynamic catalog configuration.
#
# Generates Iceberg catalog properties from environment variables.
# The catalog name is determined by CATALOG_NAME env var (default: demo).
#
# Environment Variables:
#   CATALOG_NAME          - Name of the catalog (default: demo)
#   CATALOG_TYPE          - Iceberg catalog type: rest, nessie, hive_metastore
#   CATALOG_URI           - REST catalog URI
#   CATALOG_WAREHOUSE     - Warehouse location (s3://bucket/path)
#   NESSIE_URI            - Nessie API endpoint
#   NESSIE_REF            - Nessie ref/branch (default: main)
#   HMS_URI               - Hive Metastore Thrift URI
#   S3_ENDPOINT           - S3/MinIO endpoint
#   S3_PATH_STYLE         - Use path-style access (true/false)
#   S3_REGION             - S3 region
#   AWS_ACCESS_KEY_ID     - S3 access key
#   AWS_SECRET_ACCESS_KEY - S3 secret key
#
set -e

CATALOG_NAME="${CATALOG_NAME:-demo}"
OUTPUT_FILE="/etc/trino/catalog/${CATALOG_NAME}.properties"

echo "Generating Trino catalog: ${CATALOG_NAME}"
echo "  Type: ${CATALOG_TYPE:-rest}"

# Set defaults
CATALOG_TYPE="${CATALOG_TYPE:-rest}"
CATALOG_URI="${CATALOG_URI:-}"
CATALOG_WAREHOUSE="${CATALOG_WAREHOUSE:-}"
NESSIE_URI="${NESSIE_URI:-}"
NESSIE_REF="${NESSIE_REF:-main}"
HMS_URI="${HMS_URI:-}"
S3_ENDPOINT="${S3_ENDPOINT:-}"
S3_PATH_STYLE="${S3_PATH_STYLE:-true}"
S3_REGION="${S3_REGION:-us-east-1}"

# Start with common config
cat > "$OUTPUT_FILE" <<EOF
connector.name=iceberg
iceberg.catalog.type=${CATALOG_TYPE}
EOF

# Add catalog-type-specific properties
case "$CATALOG_TYPE" in
    rest)
        cat >> "$OUTPUT_FILE" <<EOF
iceberg.rest-catalog.uri=${CATALOG_URI}
iceberg.rest-catalog.warehouse=${CATALOG_WAREHOUSE}
EOF
        # Add OAuth2 settings if credential is provided (e.g., for Polaris)
        if [ -n "${CATALOG_CREDENTIAL:-}" ]; then
            echo "  OAuth2: enabled"
            cat >> "$OUTPUT_FILE" <<EOF
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=${CATALOG_CREDENTIAL}
EOF
            if [ -n "${CATALOG_SCOPE:-}" ]; then
                echo "iceberg.rest-catalog.oauth2.scope=${CATALOG_SCOPE}" >> "$OUTPUT_FILE"
            fi
        fi
        ;;
    nessie)
        cat >> "$OUTPUT_FILE" <<EOF
iceberg.nessie-catalog.uri=${NESSIE_URI}
iceberg.nessie-catalog.ref=${NESSIE_REF}
iceberg.nessie-catalog.default-warehouse-dir=${CATALOG_WAREHOUSE}
EOF
        ;;
    hive_metastore)
        cat >> "$OUTPUT_FILE" <<EOF
hive.metastore.uri=${HMS_URI}
EOF
        ;;
    *)
        echo "Warning: Unknown catalog type: ${CATALOG_TYPE}"
        ;;
esac

# Add S3 and Iceberg settings (common to all)
cat >> "$OUTPUT_FILE" <<EOF
fs.native-s3.enabled=true
s3.endpoint=${S3_ENDPOINT}
s3.path-style-access=${S3_PATH_STYLE}
s3.region=${S3_REGION}
s3.aws-access-key=${AWS_ACCESS_KEY_ID}
s3.aws-secret-key=${AWS_SECRET_ACCESS_KEY}
iceberg.remove-orphan-files.min-retention=1d
iceberg.expire-snapshots.min-retention=1d
EOF

echo "Generated catalog file: $OUTPUT_FILE"
cat "$OUTPUT_FILE"

# Start Trino
echo "Starting Trino..."
exec /usr/lib/trino/bin/run-trino
