#!/usr/bin/env bash
# End-to-end smoke test against a running stack:
#   docker compose up -d --build && tests/smoke_test.sh
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

TIMEOUT="${SMOKE_TIMEOUT:-300}"
psql_q() { docker compose exec -T postgres psql -U "$PG_ADMIN_USER" -d "$PG_DB" -Atc "$1"; }

echo "==> Waiting up to ${TIMEOUT}s for aggregated rows in PostgreSQL ..."
deadline=$((SECONDS + TIMEOUT))
until [ "$(psql_q 'SELECT count(*) FROM field_metrics_1m' 2>/dev/null || echo 0)" -gt 0 ]; do
  [ $SECONDS -lt $deadline ] || { echo "FAIL: no rows landed"; docker compose logs --tail=50 flink-job jobmanager; exit 1; }
  sleep 5
done

echo "==> Flink job state"
state=$(docker compose exec -T jobmanager curl -fsS http://localhost:8081/jobs/overview \
  | python3 -c 'import json,sys; print([j["state"] for j in json.load(sys.stdin)["jobs"] if j["name"]=="agristream-aggregate"][0])')
[ "$state" = "RUNNING" ] || { echo "FAIL: job state is $state"; exit 1; }

echo "==> No duplicate windows (primary key guarantees this)"
dups=$(psql_q "SELECT count(*) FROM (SELECT field_id, window_start FROM field_metrics_1m GROUP BY 1,2 HAVING count(*)>1) d")
[ "$dups" = "0" ] || { echo "FAIL: duplicates found"; exit 1; }

echo "==> Per-window volume sanity check (complete windows only)"
psql_q "SELECT window_start, sum(reading_count) FROM field_metrics_1m
        WHERE window_start < (now() AT TIME ZONE 'UTC') - interval '2 minutes'
        GROUP BY 1 ORDER BY 1 DESC LIMIT 3"

echo "PASS"
