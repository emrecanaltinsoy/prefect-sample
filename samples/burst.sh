#!/usr/bin/env bash
# Send N pipeline requests quickly, to see the work pool queue and the --limit 10 cap.
# Usage: ./samples/burst.sh [count] [base_url]
set -euo pipefail

COUNT="${1:-25}"
BASE_URL="${2:-http://localhost:8080}"
DIR="$(cd "$(dirname "$0")" && pwd)"

for i in $(seq 1 "$COUNT"); do
  curl -s -X POST "$BASE_URL/pipelines" \
       -H "Content-Type: application/json" \
       -d @"$DIR/create-pipeline.json" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['flow_run_id'], d['state'])"
done
