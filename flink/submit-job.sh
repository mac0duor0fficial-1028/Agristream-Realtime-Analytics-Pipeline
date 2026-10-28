#!/usr/bin/env bash
# One-shot job submitter. Idempotent: does nothing if the job is already running.
set -euo pipefail

JM="${JOB_MANAGER:-jobmanager:8081}"
JOB_NAME="${PIPELINE_NAME:-agristream-aggregate}"

log() { echo "[submit-job] $*"; }

log "Waiting for JobManager REST API at ${JM} ..."
until curl -fsS "http://${JM}/overview" >/dev/null 2>&1; do sleep 3; done

log "Waiting for at least one TaskManager ..."
until [ "$(curl -fsS "http://${JM}/overview" | python3 -c 'import json,sys; print(json.load(sys.stdin)["taskmanagers"])')" -ge 1 ]; do
  sleep 3
done

already_running=$(curl -fsS "http://${JM}/jobs/overview" | JOB_NAME="${JOB_NAME}" python3 -c '
import json, os, sys
name = os.environ["JOB_NAME"]
live = {"CREATED", "RUNNING", "RESTARTING", "INITIALIZING"}
jobs = json.load(sys.stdin)["jobs"]
print("yes" if any(j["name"] == name and j["state"] in live for j in jobs) else "no")
')

if [ "${already_running}" = "yes" ]; then
  log "Job '${JOB_NAME}' is already running - nothing to do."
  exit 0
fi

log "Submitting '${JOB_NAME}' ..."
exec flink run -m "${JM}" -d -py /opt/flink/jobs/aggregate.py
