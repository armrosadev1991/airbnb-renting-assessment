#!/usr/bin/env bash
# Run one SQL statement on a SQL warehouse through the Statement Execution API and print the result.
# Used by `just bootstrap` because, on Free Edition, only SQL `CREATE CATALOG` picks up the
# workspace's Default Storage (the REST `catalogs create` call fails without a storage root).
#
# Usage: run_sql.sh <profile> <warehouse-name> "<statement>"
set -euo pipefail

profile="$1"
warehouse_name="$2"
statement="$3"

warehouse_id=$(databricks warehouses list --profile "$profile" -o json |
  python3 -I -c 'import json,sys; n=sys.argv[1]; print(next(w["id"] for w in json.load(sys.stdin) if w["name"] == n))' "$warehouse_name")

body=$(python3 -I -c 'import json,sys; print(json.dumps({"warehouse_id": sys.argv[1], "statement": sys.argv[2], "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"}))' "$warehouse_id" "$statement")
response=$(databricks api post /api/2.0/sql/statements --json "$body" --profile "$profile")
statement_id=$(echo "$response" | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["statement_id"])')

# A stopped serverless warehouse needs ~1 min to start, so poll instead of failing on PENDING.
for _ in $(seq 1 36); do
  state=$(echo "$response" | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["status"]["state"])')
  case "$state" in SUCCEEDED | FAILED | CANCELED | CLOSED) break ;; esac
  sleep 5
  response=$(databricks api get "/api/2.0/sql/statements/$statement_id" --profile "$profile")
done

RESPONSE="$response" python3 -I - <<'PY'
import json, os, sys

d = json.loads(os.environ["RESPONSE"])
status = d["status"]
if status["state"] != "SUCCEEDED":
    sys.exit(f"SQL {status['state']}: {status.get('error', {}).get('message', '')}")
for row in (d.get("result") or {}).get("data_array") or []:
    print(" | ".join("" if v is None else str(v) for v in row))
PY
