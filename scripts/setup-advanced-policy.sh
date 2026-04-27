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

# Usage: ./scripts/setup-advanced-policy.sh

set -euo pipefail

API="${FLOE_API:-http://localhost:9091/api/v1}"
NAME="advanced-maintenance"

exists() {
  python3 - <<'PY'
import json, os, sys, urllib.request
api = os.environ.get("API")
name = os.environ.get("NAME")
url = f"{api}/policies?limit=1000&offset=0"
try:
    with urllib.request.urlopen(url) as resp:
        data = json.load(resp)
    for p in data.get("policies", []):
        if p.get("name") == name:
            print("yes")
            sys.exit(0)
except Exception:
    pass
print("no")
PY
}

if [ "$(exists)" = "yes" ]; then
  echo "Policy '${NAME}' already exists; skipping."
  exit 0
fi

curl -s -X POST "${API}/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "advanced-maintenance",
    "tablePattern": "demo.test.*",
    "priority": 100,
    "rewriteDataFiles": {"strategy": "BINPACK", "targetFileSizeBytes": 134217728},
    "expireSnapshots": {"retainLast": 10, "maxSnapshotAge": "P7D"},
    "orphanCleanup": {"retentionPeriodInDays": 3},
    "rewriteManifests": {},
    "healthThresholds": {
      "smallFilePercentWarning": 20.0,
      "smallFilePercentCritical": 50.0,
      "deleteFileCountWarning": 100,
      "deleteFileCountCritical": 500,
      "snapshotCountWarning": 50,
      "snapshotCountCritical": 200
    },
    "triggerConditions": {
      "smallFilePercentageAbove": 20.0,
      "deleteFileCountAbove": 50,
      "minIntervalMinutes": 60,
      "criticalPipeline": true,
      "criticalPipelineMaxDelayMinutes": 360
    }
  }' >/dev/null

echo "Created policy '${NAME}'."
