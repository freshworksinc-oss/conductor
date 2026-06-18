#!/usr/bin/env bash
# Staging smoke: start one workflow and print workflow id for Jaeger lookup.
# Usage: ./smoke-tracing.sh <conductor_base_url> <workflow_name> [version]
set -euo pipefail

BASE_URL="${1:?Conductor base URL required, e.g. http://localhost:8080}"
WORKFLOW_NAME="${2:?Workflow name required}"
VERSION="${3:-1}"

RESPONSE="$(curl -sf -X POST "${BASE_URL}/api/workflow" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"${WORKFLOW_NAME}\",\"version\":${VERSION},\"input\":{},\"correlationId\":\"smoke-tracing-$(date +%s)\"}")"

# API returns workflow id as plain text or JSON string depending on version
WORKFLOW_ID="$(echo "${RESPONSE}" | tr -d '"')"

echo "Started workflow: ${WORKFLOW_ID}"
echo "Jaeger: search workflow.id=${WORKFLOW_ID}"
echo "Expected spans (v1): workflow.start, workflow.enqueue_decider, workflow.decide, task.enqueue"
echo "On completion: workflow.decide should include workflow.status=COMPLETED (or FAILED)"
