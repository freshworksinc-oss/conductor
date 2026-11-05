# pg-mirror

Keeps a Postgres Conductor's tables up to date with what a **non-Postgres** Conductor (Scylla) writes,
so that after a cutover to Scylla we can roll back to Postgres. This module is the **capture side**:
for every write the Conductor makes, it builds the equivalent Postgres statements and pushes them to a
Redis list. A separate consumer (in the data-migrator) applies them to Postgres.

Off by default. With `conductor.pg-mirror.enabled=false` nothing in the Conductor changes.

```
Conductor ──► ExecutionDAO (Scylla) ──► real write, untouched
                  │ wrapped from outside (Spring), Scylla module unchanged
                  └─ after the write succeeds: build SQL ─► RPUSH  pg-mirror:<stack>:sql  (Redis list)

consumer (migrator): list ─► Postgres journal (durable) ─► apply to the Postgres Conductor's tables
```

## What it guarantees the Conductor

- The DAO call runs first and its result and exceptions are returned **as they are**.
- Any error in the mirror (building SQL, Redis down, Redis slow) is caught, counted and logged. It never
  reaches the caller.
- A push costs at most `timeout-ms` (default 200 ms). After `breaker-failures` failed pushes in a row the
  pushes are skipped for `breaker-open-ms`, so a dead Redis costs nothing per write.
- While the list is longer than `max-queue-length` (nobody is applying it) pushes are skipped.
- A skipped or failed push loses that message. **That is accepted and must be repaired by a
  reconciliation** (compare fingerprints of the running workflows on both sides and re-queue the
  differences). A mirror without it can be silently wrong.

## Configuration (`conductor.pg-mirror.*`)

| Property | Default | |
|---|---|---|
| `enabled` | `false` | also requires `conductor.db.type` not `postgres` |
| `queue-key` | `pg-mirror:<conductor.app.stack>:sql` | a Redis list no Conductor key uses |
| `redis.host` / `port` / `username` / `password` / `ssl` | localhost / 6379 / – / – / false | the same Redis as the Conductor is fine |
| `timeout-ms` | 200 | connect and read timeout of a push |
| `breaker-failures` / `breaker-open-ms` | 5 / 10000 | circuit breaker |
| `max-queue-length` | 2000000 | pause pushing above this |
| `poll-data-flush-ms` | 10000 | poll data is buffered; the latest poll per queue + domain goes out as one message every this many ms |

Example for the Scylla Conductor:

```properties
conductor.pg-mirror.enabled=true
conductor.pg-mirror.redis.host=${REDIS_HOST}
conductor.pg-mirror.redis.port=${REDIS_PORT}
conductor.pg-mirror.redis.username=${REDIS_USER_NAME}
conductor.pg-mirror.redis.password=${REDIS_PASSWORD}
```

## Operations and the Postgres tables each one writes

Every table `PostgresExecutionDAO` writes for an operation is written here too.

| Conductor call | Tables | Notes |
|---|---|---|
| `createWorkflow`, `updateWorkflow` | `workflow`, `workflow_def_to_workflow`, `workflow_pending` | JSON without tasks; pending row only while not terminal |
| `createTasks`, `updateTask` | `task`, `workflow_to_task`, `task_scheduled`, `task_in_progress` | one message per task; `task_in_progress` row while not terminal, `in_progress_status` kept only for tasks whose definition has a concurrency limit |
| `removeTask` | `task_scheduled`, `workflow_to_task`, `task_in_progress`, `task` | built from the task as it was before |
| `removeWorkflow` | `workflow_def_to_workflow`, `workflow`, `workflow_pending` + the four above for every task | built from the workflow as it was before |
| `removeFromPendingWorkflow` | `workflow_pending` | a no-op on Scylla, but Postgres has the table |
| `addEventExecution` (only if added), `updateEventExecution`, `removeEventExecution` | `event_execution` | |
| `createTaskDef`, `updateTaskDef`, `removeTaskDef` | `meta_task_def` | upsert by name |
| `createWorkflowDef`, `updateWorkflowDef`, `removeWorkflowDef` | `meta_workflow_def` | upsert by (name, version), then `latest_version` of the name is set to its highest version (two statements, each placeholder used once) |
| `addEventHandler`, `updateEventHandler`, `removeEventHandler` | `meta_event_handler` | delete + insert in one message (the table has no unique name) |
| `updateLastPollData` | `poll_data` | **buffered**: one message per flush interval with the latest poll of each queue + domain; an older poll never overwrites a newer one |

`createTasks` mirrors every task the Scylla DAO returns: that is what Scylla stored. (The Postgres DAO
skips tasks that are already scheduled; the Scylla DAO does not.)

**Not covered yet:** `poll_data`, the definition tables (`meta_task_def`, `meta_workflow_def`,
`meta_event_handler`). They change rarely and are better synced by a timer diff than per write.

## Message format

One JSON object per list element (`RPUSH`), see `PgMirrorMessage`:

```json
{ "v": 1, "id": "uuid", "op": "updateTask", "workflowId": "…", "ts": 1760000000000,
  "gated": true,
  "statements": [ { "sql": "INSERT INTO task … WHERE … <= $3::bigint", "args": ["task-id", "{…json…}", 1760000004000] } ] }
```

## Applier contract (what the consumer must do)

1. Run a message's statements **in one transaction, in order**.
2. Placeholders are `$1…$n`, each used once, in ascending order; `args` are JSON scalars (string, number,
   boolean, null), one per placeholder.
3. If `gated` is true the first statement is a guard (an upsert whose `WHERE` compares the stored
   `updatedTime`/`updateTime` with the incoming one). **If it affects 0 rows the message is stale: skip the
   rest and commit nothing.** This makes messages idempotent and safe out of order.
4. Messages of one workflow should be applied in list order; different workflows may be applied in
   parallel (hash by `workflowId`).
5. A message that fails must go to a dead-letter table with an alert, and must not block the others.
6. Keep a checkpoint of what has been applied (a sequence number in the journal table).

`PgMirrorTestApplier` (test sources) is the reference implementation.

## Tests

```
./gradlew :conductor-pg-mirror:test
```

- `PgMirrorEquivalenceTest` is the proof that the rows are right: it runs operations through the real
  `PostgresExecutionDAO` on database A, applies the statements the interceptor emits for the same calls
  to an empty database B, and requires every table to match after each step. It also checks that a stale
  message is skipped and a repeated one changes nothing. It needs two Postgres databases with the
  Conductor's schema (all `postgres-persistence` migrations) and is skipped unless they are given:

  ```
  PG_MIRROR_A_URL='jdbc:postgresql://127.0.0.1:5432/mirror_a?user=postgres'
  PG_MIRROR_B_URL='jdbc:postgresql://127.0.0.1:5432/mirror_b?user=postgres'
  ```
- `PgMirrorExecutionDaoInterceptorTest`: results and exceptions pass through; mirror errors never reach the
  caller; the pre-reads for removals.
- `RedisPgMirrorPublisherTest`: one `RPUSH` per call, failures, circuit breaker, backlog cap.
- `PgMirrorConfigurationTest`: the Spring wiring with the real Jedis client against a fake Redis,
  including a dead Redis.

## Things to know

- `workflow_def_to_workflow.date_str` is `yyyyMMdd` in the JVM's default time zone, as in the Postgres
  DAO. Run the Conductors in the same zone (UTC).
- The queue key must not match the Conductor's own Redis prefixes. The default `pg-mirror:…` does not.
- Workflows that existed before the mirror was switched on are not in the queue: copy them with the
  migrator's reverse state copy / reconciliation.
