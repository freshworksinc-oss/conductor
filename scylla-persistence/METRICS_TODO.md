# Scylla metrics: to-do

Tracking list for the queue/workflow/task metrics work on `scylla-queue-metrics`
(branched from `production-new` at `194ee5e75`). Tick items as they land and note the commit.

## Done

- [x] `ca0e3a792`: real `workflow_running`, `task_in_progress`, `task_queue_depth` on Scylla
      (`workflow_pending` table, `task_def_limit` for every IN_PROGRESS task) and the new
      `task_queue_depth_by_domain`.
- [x] `ea43fc36d`: `WorkflowMonitor` no longer loads every definition (port of
      conductor-oss/conductor#1145, plus a Scylla projection that parses only `ownerApp`).
      Measured: ~1.5–29 KB heap per parsed definition vs ~115 B per metric record.
- [x] `ea43fc36d`: `getInProgressTaskCount` queried only for task defs whose
      `task_in_progress` is published.
- [x] `ea43fc36d`: `ScyllaQueueDepthMonitor` switched only by
      `conductor.scylla.queue-metrics-enabled`.

## Decisions needed

- [ ] **Tenant label source:** the task's `domain` (what the auth proxy forces with
      `"*" -> <tenant>`), or the workflow owner's `tenant_id`, when both exist?
- [ ] **Label name** for the new metrics: `tenant`, or reuse `ownerApp`?
- [ ] **Skip zero-value series?** Fewer series, but idle queues then show no data instead of 0.
- [ ] **Single pod:** a separate 1-replica deployment (recommended) or a Redis lock on the
      existing pods?

## 1. Config (do first, needed before going to a single pod)

- [ ] Split `conductor.scylla.queue-metrics-enabled` in two:
  - `conductor.scylla.queue-metrics-enabled`: bookkeeping writes to `workflow_pending` /
    `task_def_limit`. Must stay **on in every pod** (the pod that updates a workflow or task
    does the write).
  - `conductor.scylla.queue-depth-monitor.enabled`: the publisher. **On only on the metrics pod.**
- [ ] Optional: own schedule for the Scylla monitor, falling back to the current one, e.g.
      `${conductor.scylla.queue-metrics.delay:${conductor.workflow-monitor.stats.delay:60000}}`.

## 2. Single metrics pod

- [ ] Deployment `conductor-core-scylla-metrics`, `replicas: 1`, no ingress (no API traffic).
- [ ] On it: `conductor.workflow-monitor.enabled=true`, the queue-depth monitor on.
- [ ] On all other pods: `conductor.workflow-monitor.enabled=false`, the queue-depth monitor off,
      bookkeeping writes **on**.
- [ ] Event metrics (counters/timers) stay on every pod; they are summed across pods.
- [ ] Dashboards: filter gauges on the metrics pod; no more `max()` across pods.

## 3. Tenant metrics (gauges, metrics pod only)

- [ ] `tenant` column on `task_def_limit` (added at startup to existing tables, like
      `tenant_id`), written in `addTaskToLimit`. Rows from before show `unknown` until the
      task's next update.
- [ ] `task_in_progress_by_tenant{tenant}`, plus `{tenant, taskType}` for pairs with something
      in progress. One paged scan of `task_def_limit` per run, grouped in memory, instead of one
      `COUNT` per task def.
- [ ] `task_queue_depth_by_tenant{tenant}`: sum of the domain queues per tenant.
- [ ] `workflow_running_by_tenant{tenant}`: sum per tenant of the counts `WorkflowMonitor`
      already fetches. Needed because the platform drops `workflowName`, so per-workflow series
      collide.
- [ ] **New property (not implemented yet; nothing in the repo defines it):**
      `conductor.workflow-monitor.per-workflow-metrics-enabled`
  - `true` (default): `workflow_running{workflowName, ownerApp}` as today, so upstream
    behaviour is unchanged.
  - `false` (our environments): the per-workflow gauges are not built; only
    `workflow_running_by_tenant{tenant}` is published.
  - Why: today the only switch is `conductor.workflow-monitor.enabled`, which also stops
    `task_queue_depth` / `task_in_progress`. The platform drops `workflowName`, so the
    per-workflow series collide and their gauges only cost heap.
  - Where: `WorkflowMonitor` (`@Value`, defaulting to `true`), the entry in
    `core/src/main/resources/META-INF/additional-spring-configuration-metadata.json`,
    and a `WorkflowMonitorTest` case for each value.
  - Depends on: the tenant label decision above.
- [ ] Tests: unit specs, plus a real-Scylla spec for the tenant column and the scan.

## 4. Event metrics by tenant (core, all pods)

- [x] Switch `conductor.metrics.event-metrics.enabled` (default `true`). When `false`, `Monitors.getCounter` / `getTimer` /
      `distributionSummary` return shared no-op meters: nothing registered, exported or held per
      tag combination, and event meters registered earlier are removed. Gauges are unaffected. The
      no-op timer still runs wrapped code (`JedisProxy` Redis calls). Set it `true` on the serving
      core pods and `false` on the metrics pod.
- [ ] Drop percentiles on `task_execution` / `task_queue_wait`. Measured ~21.6 KB heap and 8
      series per timer with percentiles, vs 3 series without; dashboards already use `_sum` /
      `_count` / `_max`. Quantile series would disappear.

- [ ] `task_completed_by_tenant{tenant, status}` counter (completed / failed / timed out).
- [ ] `task_queue_wait_by_tenant{tenant}` timer **without percentiles** (count/sum/max only).
- [ ] Do **not** add `tenant` to the existing percentile timers (`task_execution`,
      `task_queue_wait`): each would create ~8 series × tenant × taskType × pods.
- [ ] Already by tenant via `domain`: `task_poll_count`, `task_poll_error`. Document them.

## 5. Cardinality cleanup

- [ ] Drop the `NO_DOMAIN` series from `task_queue_depth_by_domain` (it repeats
      `task_queue_depth`, ~1 series per task def per pod).
- [ ] Clean up unused or test task definitions (e.g. `mig_*`). No code change; shrinks every
      task metric. Sandbox has ~920 task defs.
- [ ] Re-check series counts after the single-pod move. Sandbox baseline: `task_queue_depth`
      4,591, `task_queue_depth_by_domain` 2,486, `task_in_progress` 137, `workflow_running` 82.

## 6. Correctness / resilience of the current metrics

- [ ] `WorkflowMonitor`: a `try/catch` per item, so one failed `COUNT` doesn't drop every gauge
      for that run.
- [ ] `getPendingWorkflowCount` throws `UnsupportedOperationException` when
      `queue-metrics-enabled=false`, which breaks the whole `WorkflowMonitor` run. Return 0 or
      skip instead.
- [ ] Tombstones under `COUNT` / scans on `workflow_pending` and `task_def_limit`: every finished
      workflow or task leaves one. Watch Scylla tombstone warnings; consider bucketing the
      partition key or a tighter `tombstone_gc` / `gc_grace_seconds`.
- [ ] Race: the insert/delete on `workflow_pending` runs after the version-checked update but is
      a plain write, so two pods can reorder it and leave a row that is counted forever.
      Consider `USING TIMESTAMP` from the workflow's update time, or a periodic reconcile.
- [ ] Backfill `workflow_pending` for running workflows created before tracking, or migrated
      from Postgres. They are only counted after their next update, and migrated WAIT tasks
      may never update.
- [ ] `task_in_progress` for async system tasks is queried by `taskType`, but rows are stored
      by `taskDefName`. They differ if a system task's definition has another name.

## 7. Memory (same output)

- [ ] Remove the unused `SimpleMeterRegistry` from `Monitors` once a real registry is added
      (about one copy of every meter).
- [ ] Lighter gauge key in `Monitors` (today `name + Arrays.toString(tags)`, built on every call).
- [ ] Optional: store `ownerApp` in a new column of the Scylla definition index, to skip reading
      the latest definitions on each refresh. Don't put it into the existing `name/version`
      value: pods still on old code would skip those entries.

## 8. Housekeeping

- [ ] Four spotless-only files are uncommitted (`CacheableEventHandlerDAO`, `CachingConfig`,
      `ScyllaEventHandlerDAO`, `ScyllaRateLimitingDAO`). Commit them separately or revert.
- [ ] Pin `scylladb/scylla:latest` in `ScyllaExecutionStatsSpec` (dependency-pinning rule in
      `AGENTS.md`).
- [ ] Remove the leftover duplicate Javadoc above `isInTaskDefLimit` in `ScyllaExecutionDAO`
      (~line 1176).
- [ ] `ScyllaSpec` on this branch is still the original version, so the specs built on it
      (`ScyllaMetadataDAOSpec`, `ScyllaExecutionDAOSpec`, …) may not run. Bring over the fix.
- [ ] conductor-oss/conductor#1145 is still open upstream. When it merges, reconcile with our
      port (same API, so it should be a small merge).
- [ ] Run a Snyk dependency scan (`snyk test --all-projects`); it could not run in the session.

## 9. Sandbox / dashboards

- [ ] Confirm the sandbox image includes `ea43fc36d`.
- [ ] `conductor.workflow-monitor.enabled` is `false` in sandbox, so `workflow_running`,
      `task_in_progress` and `task_queue_depth` won't appear there until it's re-enabled (or
      the metrics pod exists).
- [ ] Update dashboards: the platform drops `workflowName`; filter Scylla pods with
      `pod=~"conductor-core-scylla-.*"`; switch to the `*_by_tenant` metrics when they land.
- [ ] After the single-pod move, update the PromQL in dashboards and alerts from `max()` to
      plain selectors on the metrics pod.
