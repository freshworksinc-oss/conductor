# Workflow start tracing (v1)

Manual OpenTelemetry tracing for the **workflow start** path: `POST /api/workflow` through async decider execution. Disabled by default for OSS deployments.

## What is traced (v1)

When `conductor.tracing.manual.enabled=true`:

| Span | Where |
|------|--------|
| `workflow.start` | `WorkflowServiceImpl.startWorkflow` (API/service) |
| `workflow.enqueue_decider` | `ExecutionDAOFacade.createWorkflow` |
| `workflow.decide` | `WorkflowExecutorOps` (sync start and sweeper) |
| `task.enqueue` | `WorkflowExecutorOps.addTaskToQueue` |

Common attributes: `workflow.id`, `workflow.name`, `workflow.version`, `correlation.id`, and task fields on enqueue.

On `workflow.decide`, when the workflow reaches a terminal state (`COMPLETED`, `FAILED`, `TERMINATED`, etc.), the span is tagged with `workflow.status` so completion is visible in Jaeger without a separate span.

**Not in v1 scope:** `task.poll`, `task.update`, metadata, or search business spans (even if `@BusinessTrace` is present elsewhere, the aspect allowlist ignores them).

Infrastructure spans (HTTP, JDBC, Redis) come from the [OpenTelemetry Java agent](https://github.com/open-telemetry/opentelemetry-java-instrumentation) when attached.

## Internal variable: `_trace_context`

W3C trace context (`traceparent`, `tracestate`, etc.) is stored in workflow runtime variables under `_trace_context` when a workflow is started with tracing enabled.

- Persisted with the workflow in the database (not in queue payloads).
- Restored on later `workflow.decide` / system-task execution so async sweeper spans stay correlated.
- Intended for Conductor internals only; do not use it in workflow business logic.
- May appear in workflow GET responses until filtered in a future release.

## Configuration

### Application

```properties
# Default: false (no-op). Enable only in pilot/staging/prod when ready.
conductor.tracing.manual.enabled=false
```

Local development: `server/src/main/resources/application-local.properties` sets this to `true`.

### JVM (OpenTelemetry agent)

Tracing export requires the agent (see `application-local.properties` header comment). Example:

```bash
java \
  -javaagent:/path/to/opentelemetry-javaagent.jar \
  -Dotel.javaagent.experimental.sdk.enabled=true \
  -Dotel.service.name=conductor-server \
  -Dotel.traces.exporter=otlp \
  -Dotel.exporter.otlp.traces.endpoint=http://localhost:4318/v1/traces \
  -Dotel.metrics.exporter=none \
  -Dotel.logs.exporter=none \
  -Dotel.traces.sampler=parentbased_traceidratio \
  -Dotel.traces.sampler.arg=0.1 \
  -jar server/build/libs/conductor-server-*-boot.jar
```

Use `parentbased_always_on` in staging; use ratio sampling in production.

### Docker observability stack

```bash
docker compose -f docker/docker-compose-postgres-redis-observability.yaml up -d
```

Collector config: `docker/observability/otel-collector/config.yaml`.

## Background noise (agent vs manual tracing)

The OTel Java agent creates many traces that are **not** tied to how many workflows you started.

| Trace / span | Source | Frequency | Part of v1 manual tracing? |
|--------------|--------|-----------|----------------------------|
| `workflow.start`, `workflow.decide`, … | `conductor.tracing.manual.enabled` | Per start / decide | Yes |
| `WorkflowReconciler.pollAndSweep` | Agent on `@Scheduled` reconciler | ~every 500ms per server (`conductor.sweep-frequency.millis`) | No |
| `SELECT conductor` (JDBC) | Agent under sweeper or API | Many per tick | No |

Two workflow starts can still produce **hundreds** of `pollAndSweep` roots in Jaeger over a few minutes. That is expected Conductor behavior; tracing makes it visible.

### What is ideal for production?

**Primary (recommended for every prod deploy): JVM trace sampling**

```text
-Dotel.traces.sampler=parentbased_traceidratio
-Dotel.traces.sampler.arg=0.1
```

- Cuts volume and cost while keeping full traces for a sample of requests.
- Still allows debugging real workflow runs when sampled.

**Already in collector: drop orphan root JDBC** (`filter/drop_orphan_jdbc_roots`)

- Removes stray root DB client spans that are not under an HTTP trace.
- Keep this enabled.

**Drop reconciler scheduler spans** (enabled in local `docker/observability/otel-collector/config.yaml`)

- Collector drops spans named `WorkflowReconciler.pollAndSweep`. **Restart `otel-collector`** after editing the config.
- **Also disable at the agent** (recommended): `docker/observability/conductor-local-otel.properties` sets `otel.instrumentation.spring-scheduling.enabled=false`. Pass `-Dotel.javaagent.configuration-file=...` when starting Conductor so sweeper ticks are not instrumented at all.

**Optional in prod** — use JVM sampling first; keep collector + agent settings if Jaeger is still noisy.
- **Safe for v1 workflow tracing:** `workflow.decide` restores `_trace_context` from the start request, so business spans should remain under the `workflow.start` trace. Sweeper ticks are mostly infrastructure noise.
- **Trade-off:** you lose visibility into reconciler latency/DB churn as standalone traces (metrics/logs still cover sweeper health).

**Not ideal as the only fix:** dropping scheduler roots without sampling — you would still export every unsampled HTTP and business trace at 100%, and you lose sweeper-only diagnostics entirely.

### How to use Jaeger day to day

- Search by **`workflow.id`** or span name **`workflow.start`** / **`workflow.decide`**.
- Do not use the default “all traces” list as your workflow dashboard — it will be dominated by `pollAndSweep` when sampling is off (local dev).

### Local dev vs prod

| Environment | Suggestion |
|-------------|------------|
| Local / staging | `parentbased_always_on` or low sampling; leave reconciler filter **off** while learning the UI |
| Production pilot | **10% sampling** + JDBC orphan filter; add reconciler filter only if still too noisy |

## Staging smoke test

1. Start Postgres, Redis, OTel collector, Jaeger (compose file above).
2. Run Conductor with agent + `conductor.tracing.manual.enabled=true`.
3. Register a pilot workflow definition and run:

   ```bash
   docker/observability/smoke-tracing.sh http://localhost:8080 <workflow_name>
   ```

4. Open Jaeger UI (compose exposes the UI port) and search by `workflow.id` from the script output.
5. Confirm spans: `workflow.start` → `workflow.enqueue_decider` → `workflow.decide` → `task.enqueue` (if tasks were scheduled). On a finishing workflow, the final `workflow.decide` should have `workflow.status=COMPLETED` or `FAILED`.
6. After a sweeper cycle, confirm another `workflow.decide` is linked to the same trace (not an unrelated root).
7. Set `conductor.tracing.manual.enabled=false`, restart, start a new workflow — no `_trace_context` on new runs.

## Troubleshooting: no traces in Jaeger

### 1. Check Conductor logs for export errors

If you see `Failed to export spans` / `Broken pipe` / `Connection reset` to `localhost:4329`, spans **never reach** the collector (filters are not the cause).

```bash
docker compose -f docker/docker-compose-postgres-redis-observability.yaml ps
docker logs conductor-otel-collector --tail 50
```

Collector must be **Up**. Restart if needed:

```bash
docker compose -f docker/docker-compose-postgres-redis-observability.yaml up -d jaeger otel-collector
docker compose -f docker/docker-compose-postgres-redis-observability.yaml restart otel-collector
```

### 2. Confirm Conductor export endpoint

| Endpoint | Path |
|----------|------|
| **Via collector (filtered)** | `http://localhost:4329/v1/traces` |
| **Direct to Jaeger (bypass collector)** | `http://localhost:4319/v1/traces` |

Use **4329** when `otel-collector` is healthy. If the collector is down, switch temporarily to **4319** to confirm tracing works:

```text
-Dotel.exporter.otlp.traces.endpoint=http://localhost:4319/v1/traces
```

### 3. Generate a trace

```bash
curl -X POST http://localhost:8080/api/workflow -H 'Content-Type: application/json' \
  -d '{"name":"<your_workflow>","version":1,"input":{}}'
```

In Jaeger: Service `conductor-server-local`, lookback **Last hour**, search span name `workflow.start`.

### 4. Manual tracing flag

`conductor.tracing.manual.enabled=true` requires `--spring.profiles.active=local` (or explicit property). Agent alone still exports HTTP spans without it.

### 5. Empty Jaeger after fixing collector

Old export failures dropped batches — trigger a **new** workflow after the collector is healthy.

## Rollback

Set `conductor.tracing.manual.enabled=false` and reload/restart. No code deploy required if config is external. Existing workflows may still contain `_trace_context` in variables; new starts will not inject it.

## Production checklist

- [ ] Flag `false` by default; enabled only on pilot environment
- [ ] OTel agent version pinned; OTLP endpoint and sampling configured
- [ ] JVM trace sampling configured (e.g. 10% prod); JDBC orphan filter enabled in collector
- [ ] Reconciler scheduler filter only if needed after sampling (optional)
- [ ] Staging smoke passed
- [ ] On-call runbook: find workflow by `workflow.id` in Jaeger
- [ ] Lead sign-off on `_trace_context` in workflow persistence

## Next phases (not v1)

- `task.update` with stored context restore
- Worker `traceparent` on poll/update
- Sub-workflow and terminal workflow spans
- Optional hide `_trace_context` from workflow API responses
