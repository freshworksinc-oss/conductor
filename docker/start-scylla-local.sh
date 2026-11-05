#!/usr/bin/env bash
# Start Conductor locally on Scylla persistence (profile: scylla-local).
#
#   ./docker/start-scylla-local.sh          # start infra + Conductor server (foreground, Ctrl+C to stop server)
#   ./docker/start-scylla-local.sh infra    # start infra only (Scylla, Redis, Postgres)
#   ./docker/start-scylla-local.sh stop     # stop server + infra containers
#
# Infra:  conductor-scylla   :9042  (keyspace "conductor", created by Conductor on startup)
#         conductor-redis    :6379  (queues + locks)
#         conductor-postgres :6432  (TenantMetadataDAO only)
# View:   cqlsh localhost 9042 -k conductor
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCYLLA=conductor-scylla
REDIS=conductor-redis
POSTGRES=conductor-postgres

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

command -v podman >/dev/null || die "podman not found"

exists()  { podman container exists "$1"; }
running() { [ "$(podman inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]; }

wait_for() { # name, check-command...
  local name=$1; shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then log "$name is ready"; return 0; fi
    sleep 2
  done
  die "$name did not become ready in time"
}

ensure_machine() {
  if ! podman info >/dev/null 2>&1; then
    log "Starting podman machine"
    podman machine start
  fi
}

start_container() { # name, run-args...
  local name=$1; shift
  if running "$name"; then
    log "$name already running"
  elif exists "$name"; then
    log "Starting $name"
    podman start "$name" >/dev/null
  else
    log "Creating $name"
    podman run -d --name "$name" "$@" >/dev/null
  fi
}

start_infra() {
  ensure_machine

  start_container "$SCYLLA" -p 9042:9042 docker.io/scylladb/scylla:latest \
    --smp 1 --memory 1G --overprovisioned 1 --developer-mode 1
  start_container "$REDIS" -p 6379:6379 docker.io/library/redis:6.2.3
  start_container "$POSTGRES" -p 6432:5432 \
    -e POSTGRES_USER=conductor -e POSTGRES_PASSWORD=conductor -e POSTGRES_DB=conductor \
    docker.io/library/postgres:15

  wait_for "Redis"    podman exec "$REDIS" redis-cli ping
  wait_for "Postgres" podman exec "$POSTGRES" pg_isready -U conductor -d conductor
  wait_for "Scylla"   podman exec "$SCYLLA" cqlsh -e "SELECT now() FROM system.local"

  if ! podman exec "$POSTGRES" psql -U conductor -d conductor -tAc \
      "SELECT 1 FROM information_schema.tables WHERE table_name='meta_workflow_def'" | grep -q 1; then
    log "WARNING: Postgres has no meta_* tables (TenantMetadataDAO reads them)."
    log "         Run Conductor once with the 'local' (postgres) profile to create the schema."
  fi
}

stop_all() {
  log "Stopping Conductor server"
  pkill -f 'GradleWrapperMain.*bootRun' 2>/dev/null || true
  pkill -f 'com.netflix.conductor.Conductor' 2>/dev/null || true
  for c in "$SCYLLA" "$REDIS" "$POSTGRES"; do
    if running "$c"; then log "Stopping $c"; podman stop "$c" >/dev/null; fi
  done
}

start_server() {
  if lsof -nP -iTCP:8080 -sTCP:LISTEN >/dev/null 2>&1; then
    die "port 8080 is already in use (run '$0 stop' or free it first)"
  fi
  log "Starting Conductor (profile scylla-local) on http://localhost:8080"
  cd "$REPO_ROOT"
  SPRING_PROFILES_ACTIVE=scylla-local exec ./gradlew :conductor-server:bootRun --no-daemon
}

case "${1:-all}" in
  all)   start_infra; start_server ;;
  infra) start_infra ;;
  stop)  stop_all ;;
  *)     die "usage: $0 [all|infra|stop]" ;;
esac
