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

# Usage: ./scripts/seed-health-history.sh <namespace> [table] [count]

set -euo pipefail

API="${FLOE_API:-http://localhost:9091/api}"
NAMESPACE="${1:-test}"
TABLE="${2:-}"
COUNT="${3:-5}"

seed_table() {
  local table="$1"
  echo "Seeding health history for ${NAMESPACE}.${table} (${COUNT} samples)"
  for i in $(seq 1 "$COUNT"); do
    curl -s -X GET "${API}/tables/${NAMESPACE}/${table}/health" > /dev/null
    sleep 1
  done
}

if [ -n "${TABLE}" ]; then
  seed_table "${TABLE}"
  echo "Done."
  exit 0
fi

echo "Discovering tables in ${NAMESPACE}..."
TABLES_JSON="$(curl -s "${API}/tables/namespaces/${NAMESPACE}?limit=1000")"
TABLES="$(python3 - <<'PY'
import json, os, sys
try:
    payload = json.loads(os.environ.get("TABLES_JSON", "{}"))
    tables = payload.get("tables", [])
    names = [t.get("name") for t in tables if t.get("name")]
    print("\n".join(names))
except Exception:
    pass
PY
)"

if [ -z "${TABLES}" ]; then
  echo "Failed to discover tables via API, falling back to demo list."
  TABLES="events users transactions scheduler_test orders"
fi

for table in ${TABLES}; do
  seed_table "${table}"
done

echo "Done."
