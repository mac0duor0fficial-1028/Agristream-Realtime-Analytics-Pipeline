#!/usr/bin/env bash
# Daily purge of old aggregates. Runs as the least-privilege retention role.
set -euo pipefail
: "${PGHOST:?}" "${PGUSER:?}" "${PGPASSWORD:?}" "${PGDATABASE:?}"
DAYS="${RETENTION_DAYS:-30}"

while true; do
  echo "[retention] purging field_metrics_1m older than ${DAYS} days"
  psql -v ON_ERROR_STOP=1 -v days="${DAYS}" <<'SQL' \
    || echo "[retention] purge failed; will retry next cycle"
DELETE FROM field_metrics_1m
WHERE window_start < (now() AT TIME ZONE 'UTC') - (:days * interval '1 day');
SQL
  sleep 86400
done
