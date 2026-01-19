#!/bin/bash
# Creating demo policies for Floe testing
# - Full maintenance policies for events, users, transactions (manual trigger only)
# - Scheduler test policy for scheduler_test table (cron every minute)
# - Individual policies for orders table (manual trigger only)
#
# Run after: make demo-setup

set -e

API="${FLOE_API:-http://localhost:9091/api/v1}"

echo "Creating demo policies..."
echo "API: http://localhost:9091/api/"
echo ""

# Clean up existing policies
echo "Removing existing policies..."
for policy_name in full-maintenance-events full-maintenance-users full-maintenance-transactions \
                   scheduler-test orders-compaction orders-expire orders-orphan orders-manifests; do
  # Get policy ID by name and delete
  policy_id=$(curl -s "${API}/policies" | jq -r ".policies[] | select(.name == \"${policy_name}\") | .id" 2>/dev/null)
  if [ -n "$policy_id" ] && [ "$policy_id" != "null" ]; then
    curl -s -X DELETE "${API}/policies/${policy_id}" > /dev/null 2>&1 || true
    echo "  Deleted: ${policy_name}"
  fi
done

# Full maintenance policies for events, users, transactions (NO cron - manual trigger only)
FULL_MAINTENANCE_TABLES="events users transactions"

for table in $FULL_MAINTENANCE_TABLES; do
  echo "Creating full-maintenance-${table} policy (manual trigger)..."
  curl -s -X POST "${API}/policies" \
    -H "Content-Type: application/json" \
    -d '{
      "name": "full-maintenance-'"${table}"'",
      "description": "Full maintenance for '"${table}"' table - all 4 operations (manual trigger)",
      "enabled": true,
      "tablePattern": "demo.test.'"${table}"'",
      "priority": 100,

      "rewriteDataFiles": {
        "strategy": "BINPACK",
        "targetFileSizeBytes": 1048576
      },

      "expireSnapshots": {
        "retainLast": 3,
        "maxSnapshotAge": "P1D",
        "cleanExpiredMetadata": false
      },

      "orphanCleanup": {
        "retentionPeriodInDays": 1
      },

      "rewriteManifests": {
      }
    }' | jq -r '.name // .error'
done

# Scheduler test policy - runs every minute (ONLY policy with cron)
echo ""
echo "Creating scheduler-test policy (cron: every minute)..."
curl -s -X POST "${API}/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "scheduler-test",
    "description": "Scheduler test - compaction runs every minute via cron",
    "enabled": true,
    "tablePattern": "demo.test.scheduler_test",
    "priority": 100,
    "rewriteDataFiles": {
      "strategy": "BINPACK",
      "targetFileSizeBytes": 1048576
    },
    "rewriteDataFilesSchedule": {
      "cronExpression": "0 * * * * ?",
      "enabled": true
    }
  }' | jq -r '.name // .error'

# Individual policies for orders table (NO cron - manual trigger only)
echo ""
echo "Creating individual policies for orders table (manual trigger)..."

# Policy 1: Compaction
echo "  - orders-compaction"
curl -s -X POST "${API}/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders-compaction",
    "description": "Compact small files in orders table (manual trigger)",
    "enabled": true,
    "tablePattern": "demo.test.orders",
    "priority": 100,
    "rewriteDataFiles": {
      "strategy": "BINPACK",
      "targetFileSizeBytes": 1048576
    }
  }' | jq -r '.name // .error'

# Policy 2: Expire snapshots
echo "  - orders-expire"
curl -s -X POST "${API}/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders-expire",
    "description": "Expire old snapshots from orders table (manual trigger)",
    "enabled": true,
    "tablePattern": "demo.test.orders",
    "priority": 90,
    "expireSnapshots": {
      "retainLast": 3,
      "maxSnapshotAge": "P1D"
    }
  }' | jq -r '.name // .error'

# Policy 3: Orphan cleanup
echo "  - orders-orphan"
curl -s -X POST "${API}/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders-orphan",
    "description": "Remove orphan files from orders table (manual trigger)",
    "enabled": true,
    "tablePattern": "demo.test.orders",
    "priority": 80,
    "orphanCleanup": {
      "retentionPeriodInDays": 1
    }
  }' | jq -r '.name // .error'

# Policy 4: Rewrite manifests
echo "  - orders-manifests"
curl -s -X POST "${API}/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders-manifests",
    "description": "Rewrite manifests for orders table (manual trigger)",
    "enabled": true,
    "tablePattern": "demo.test.orders",
    "priority": 70,
    "rewriteManifests": {
    }
  }' | jq -r '.name // .error'

echo ""
echo "Policies created:"
curl -s "${API}/policies" | jq -r '.policies[] | "  \(.name) -> \(.tablePattern)"'
echo ""
echo "Scheduled policies (cron):"
echo "  - scheduler-test: runs every minute on demo.test.scheduler_test"
echo ""
echo "Manual trigger examples:"
echo "  # Full maintenance on events"
echo "  curl -X POST ${API}/maintenance/trigger -H 'Content-Type: application/json' -d '{\"catalog\":\"demo\",\"namespace\":\"test\",\"table\":\"events\"}'"
echo ""
echo "  # Specific policy on orders"
echo "  curl -X POST ${API}/maintenance/trigger -H 'Content-Type: application/json' -d '{\"catalog\":\"demo\",\"namespace\":\"test\",\"table\":\"orders\",\"policyName\":\"orders-compaction\"}'"
