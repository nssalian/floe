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

# Creating demo policies for Floe testing
# - Full maintenance policies for events, users, transactions (manual trigger only)
# - Scheduler test policy for scheduler_test table (cron every minute)
# - Individual policies for orders table (manual trigger only)
#
# Optional mode:
#   ./scripts/setup-demo-policies.sh --full-maintenance "demo.test.*"
#
# Run after: make demo-setup

set -e

API="${FLOE_API:-http://localhost:9091/api/v1}"

FULL_MAINTENANCE_PATTERN=""
if [ "$1" = "--full-maintenance" ]; then
  FULL_MAINTENANCE_PATTERN="$2"
fi

echo "API: ${API}"
echo ""

full_maintenance_payload() {
  local name="$1"
  local description="$2"
  local table_pattern="$3"
  cat <<EOF
{
  "name": "${name}",
  "description": "${description}",
  "enabled": true,
  "tablePattern": "${table_pattern}",
  "priority": 100,

  "rewriteDataFiles": {
    "strategy": "BINPACK",
    "targetFileSizeBytes": 33554432,
    "maxConcurrentFileGroupRewrites": 5
  },

  "expireSnapshots": {
    "retainLast": 1,
    "maxSnapshotAge": "P1D",
    "cleanExpiredMetadata": false
  },

  "orphanCleanup": {
    "retentionPeriodInDays": 1
  },

  "rewriteManifests": {
  },

  "healthThresholds": {
    "smallFilePercentWarning": 20.0,
    "smallFilePercentCritical": 50.0,
    "snapshotCountWarning": 20,
    "snapshotCountCritical": 50,
    "deleteFileRatioWarning": 0.02,
    "deleteFileRatioCritical": 0.05
  }
}
EOF
}

if [ -n "${FULL_MAINTENANCE_PATTERN}" ]; then
  echo "Creating full maintenance policy for: ${FULL_MAINTENANCE_PATTERN}"
  echo ""
  echo "Removing existing full-maintenance policy (if any)..."
  curl -s -X DELETE "${API}/policies/full-maintenance" > /dev/null 2>&1 || true
  echo "Creating full-maintenance policy..."
  full_maintenance_payload \
    "full-maintenance" \
    "Aggressive full maintenance policy for demo visibility" \
    "${FULL_MAINTENANCE_PATTERN}" \
    | curl -s -X POST "${API}/policies" -H "Content-Type: application/json" -d @- \
    | jq -r '.name // .error'
  echo ""
  echo "Policy created. To trigger maintenance:"
  echo ""
  echo "  curl -X POST ${API}/maintenance/trigger \\"
  echo "    -H 'Content-Type: application/json' \\"
  echo "    -d '{\"catalog\": \"demo\", \"namespace\": \"test\", \"table\": \"events\"}'"
  exit 0
fi

echo "Creating demo policies..."
echo ""

create_policy() {
  local payload="$1"
  local response status body name error
  response=$(
    printf "%s" "$payload" \
      | curl -sS -w '\n__HTTP_STATUS__:%{http_code}' -X POST "${API}/policies" \
        -H "Content-Type: application/json" -d @-
  )
  status=$(printf "%s" "$response" | sed -n 's/.*__HTTP_STATUS__://p' | tail -1)
  body=$(printf "%s" "$response" | sed '$d')
  name=$(printf "%s" "$body" | jq -r '.name // empty' 2>/dev/null)
  error=$(printf "%s" "$body" | jq -r '.error // empty' 2>/dev/null)
  if [ -z "$status" ]; then
    status="000"
  fi
  if [ "$status" -lt 200 ] || [ "$status" -ge 300 ]; then
    echo "Policy creation failed (HTTP ${status}). Response:"
    echo "$body"
    return 1
  fi
  if [ -n "$name" ]; then
    echo "$name"
    return 0
  fi
  if [ -n "$error" ]; then
    echo "$error"
  else
    echo "Policy creation failed. Response:"
    echo "$body"
  fi
  return 1
}

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
  create_policy "$(
    full_maintenance_payload \
      "full-maintenance-${table}" \
      "Full maintenance for ${table} table - all 4 operations (manual trigger)" \
      "demo.test.${table}"
  )"
done

# Scheduler test policy - runs every minute (ONLY policy with cron)
echo ""
echo "Creating scheduler-test policy (cron: every minute)..."
create_policy '{
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
  }'

# Individual policies for orders table (NO cron - manual trigger only)
echo ""
echo "Creating individual policies for orders table (manual trigger)..."

# Policy 1: Compaction
echo "  - orders-compaction"
create_policy '{
    "name": "orders-compaction",
    "description": "Compact small files in orders table (manual trigger)",
    "enabled": true,
    "tablePattern": "demo.test.orders",
    "priority": 100,
    "rewriteDataFiles": {
      "strategy": "BINPACK",
      "targetFileSizeBytes": 1048576
    }
  }'

# Policy 2: Expire snapshots
echo "  - orders-expire"
create_policy '{
    "name": "orders-expire",
    "description": "Expire old snapshots from orders table (manual trigger)",
    "enabled": true,
    "tablePattern": "demo.test.orders",
    "priority": 90,
    "expireSnapshots": {
      "retainLast": 3,
      "maxSnapshotAge": "P1D"
    }
  }'

# Policy 3: Orphan cleanup
echo "  - orders-orphan"
create_policy '{
    "name": "orders-orphan",
    "description": "Remove orphan files from orders table (manual trigger)",
    "enabled": true,
    "tablePattern": "demo.test.orders",
    "priority": 80,
    "orphanCleanup": {
      "retentionPeriodInDays": 1
    }
  }'

# Policy 4: Rewrite manifests
echo "  - orders-manifests"
create_policy '{
    "name": "orders-manifests",
    "description": "Rewrite manifests for orders table (manual trigger)",
    "enabled": true,
    "tablePattern": "demo.test.orders",
    "priority": 70,
    "rewriteManifests": {
    }
  }'

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
