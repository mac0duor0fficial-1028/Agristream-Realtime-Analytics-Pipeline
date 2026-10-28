#!/bin/bash
# Creates least-privilege roles. Credentials come from container env vars.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v dbname="$POSTGRES_DB" \
     -v flink_user="$FLINK_DB_USER"         -v flink_pw="$FLINK_DB_PASSWORD" \
     -v grafana_user="$GRAFANA_DB_USER"     -v grafana_pw="$GRAFANA_DB_PASSWORD" \
     -v retention_user="$RETENTION_DB_USER" -v retention_pw="$RETENTION_DB_PASSWORD" <<'EOSQL'

CREATE ROLE :"flink_user"     LOGIN PASSWORD :'flink_pw';
CREATE ROLE :"grafana_user"   LOGIN PASSWORD :'grafana_pw';
CREATE ROLE :"retention_user" LOGIN PASSWORD :'retention_pw';

REVOKE ALL ON DATABASE :"dbname" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"dbname" TO :"flink_user", :"grafana_user", :"retention_user";
GRANT USAGE ON SCHEMA public TO :"flink_user", :"grafana_user", :"retention_user";

-- Flink sink: upsert only (no DELETE, no DDL)
GRANT SELECT, INSERT, UPDATE ON field_metrics_1m TO :"flink_user";
-- Dashboards: read only
GRANT SELECT ON field_metrics_1m TO :"grafana_user";
-- Retention job: read + purge
GRANT SELECT, DELETE ON field_metrics_1m TO :"retention_user";
EOSQL
