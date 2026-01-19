#!/bin/bash
# Usage:
#   ./scripts/setup-full-maintenance-policy.sh
#   ./scripts/setup-full-maintenance-policy.sh <table_pattern>
#
# Examples:
#   ./scripts/setup-full-maintenance-policy.sh                    # Uses demo.test.events
#   ./scripts/setup-full-maintenance-policy.sh "demo.test.*"      # All tables in demo.test
#   ./scripts/setup-full-maintenance-policy.sh "demo.*.users"     # Users table in any namespace

set -e

API="${FLOE_API:-http://localhost:9091/api/}"
TABLE_PATTERN="${1:-demo.test.events}"

echo "Creating full maintenance policy for: ${TABLE_PATTERN}"
echo "API: ${API}"
echo ""

# Delete existing policy if present
echo "Removing existing full-maintenance policy (if any)..."
curl -s -X DELETE "${API}/policies/full-maintenance" > /dev/null 2>&1 || true

# Create comprehensive policy with all 4 operations
echo "Creating full-maintenance policy..."
curl -s -X POST "${API}/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "full-maintenance",
    "description": "Full maintenance policy with all 4 operations - for engine testing",
    "enabled": true,
    "tablePattern": "'"${TABLE_PATTERN}"'",
    "priority": 100,

    "rewriteDataFiles": {
      "strategy": "BINPACK",
      "targetFileSizeBytes": 134217728,
      "maxConcurrentFileGroupRewrites": 5
    },
    "rewriteDataFilesSchedule": {
      "cronExpression": "0 0 1 * * ?",
      "enabled": true
    },

    "expireSnapshots": {
      "retainLast": 5,
      "maxSnapshotAge": "P7D",
      "cleanExpiredMetadata": false
    },
    "expireSnapshotsSchedule": {
      "cronExpression": "0 0 2 * * ?",
      "enabled": true
    },

    "orphanCleanup": {
      "retentionPeriodInDays": 3
    },
    "orphanCleanupSchedule": {
      "cronExpression": "0 0 3 * * ?",
      "enabled": true
    },

    "rewriteManifests": {
    },
    "rewriteManifestsSchedule": {
      "cronExpression": "0 0 4 * * ?",
      "enabled": true
    }
  }' | jq .

echo ""
echo "Policy created. To trigger maintenance:"
echo ""
echo "  # Trigger all operations on the table"
echo "  curl -X POST ${API}/maintenance/trigger \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"catalog\": \"demo\", \"namespace\": \"test\", \"table\": \"events\"}'"
echo ""
echo "  # Or trigger specific operation types"
echo "  curl -X POST ${API}/maintenance/trigger \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"catalog\": \"demo\", \"namespace\": \"test\", \"table\": \"events\", \"operationType\": \"REWRITE_MANIFESTS\"}'"
echo ""
echo "To switch engines, set in application.properties or environment:"
echo "  FLOE_ENGINE_TYPE=TRINO   # or SPARK"
echo ""
echo "Check operations: curl ${API}/operations | jq"
