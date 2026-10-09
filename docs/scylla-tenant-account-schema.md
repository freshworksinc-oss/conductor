# Scylla schema: tenant and account identity for sharding

Status: **draft for review** · 2026-10-06 · module `scylla-persistence` (`conductor.db.type=scylla`)

Builds on D1–D7 in *Conductor persistence schema → Schema decision*. This draft adds the account dimension,
fixes where both identifiers come from, defines the shard ID logic, and gives the full DDL. Decisions are tracked in
[§11](#11-decisions); 1–4 and 7 were resolved on 2026-10-06.

---

## 1. Goals

| # | Goal |
|---|---|
| G1 | Every row is owned by exactly one tenant, and the database refuses a query that doesn't name it. |
| G2 | Account is a stored attribute of every workflow, so access can be scoped per account and an account's data can be found without a scan. |
| G3 | Data can later move at three granularities without a schema change: a **tenant** to its own keyspace or cluster, an **account** to its own keyspace or cluster, and a **large workflow** spread over several partitions. |
| G4 | Journeys and Postgres data migrate by **adding columns**, with no rewrite of the JSON payloads. |
| G5 | Keep the shape of the Journeys DAO, so the port stays mostly "bind one more value". |

## 2. Facts this design rests on

- **Today, nothing in Scylla records a tenant.** Rows are keyed by IDs, by `shard_id = parseInt(correlationId)`, or by fixed strings
  (`'task_defs'`, `'handlers'`, `'workflow_def_version_index'`). Proxy-style correlation IDs (`"t1: ORDER-1"`) currently fail with a 500.
- **In Journeys, `shard_id` is the account ID.** Clients send `correlationId = accountId`, there is a
  `GET /accounts/{accountId}/workflows/{id}` endpoint, and workers return `outputData.shardId` on task update.
- **The Freshworks proxy already supplies both values:** headers `x-tenant-id` / `x-account-id`; the body field
  `input._tenantContext = {tenantID, accountID}` (with `accountID = tenantID` when no account is sent); and a `correlationId` prefix
  `"<tenant>: "` or `"<tenant>: <account>: "`.
- **`workflow_lookup` is written in `createTasks`, not `createWorkflow`, and is deleted in `removeTaskLookup`.** Both are bugs, fixed below.

## 3. Identity model

### 3.1 Fields

| Field | Type | Required | Meaning |
|---|---|---|---|
| `tenant_id` | `text` | yes, never empty | Owning tenant (`ownerApp`). The unit of isolation and placement. |
| `account_id` | `text` | yes; the sentinel `'-1'` means "no account" | Account inside the tenant. The unit of access scoping and of account-level moves. |
| `shard_id` | `int` | yes | Partition bucket within one workflow: `base_shard + bucket` (see [§3.5](#35-shard-id)). Assigned once at creation, then always read from `workflow_lookup` / `task_lookup` (or a trusted client hint, §3.5) and never re-derived from request data. |

`'-1'` matches the convention `StatusChangePublisher` already uses for a missing account. A sentinel is needed
because a partition-key component can't be null.

### 3.2 Rules

1. **Set once, at workflow creation.** Every task, sub-workflow and retry/rerun copy inherits it. Nothing changes it later.
2. **Never derived from `correlationId` or `shard_id`** after creation. (Migration is the only place that parses them, and only once.)
3. **The columns are authoritative.** The DAO copies `tenant_id` and `account_id` from the row into `WorkflowModel` and `TaskModel` on read.
   The JSON payload may also carry them, but it is never trusted over the columns, so migrated payloads don't need rewriting.
4. **Tenant goes in every partition key. Account is a column everywhere except `workflows_by_account`.**

### 3.3 Resolution at creation

| Entry point | `tenant_id` | `account_id` |
|---|---|---|
| `POST /api/workflow` (REST/gRPC) | `X-Tenant-ID` → `input._tenantContext.tenantID` → `conductor.tenant.default` | `X-Account-ID` → `input._tenantContext.accountID` (ignored when it equals the tenant) → `'-1'` |
| Sub-workflow, `START_WORKFLOW` task | parent workflow | parent workflow |
| Event handler `start_workflow` action | the event handler's tenant | `'-1'` for now (event→tenant/account mapping deferred; events aren't in use, but the tables carry `tenant_id` already) |
| Any task | its workflow | its workflow |

`conductor.tenant.default` is set per deployment: `default` on the shared cluster, `sip`, `journeys`.

### 3.4 Runtime context

`TenantContext(tenantId, accountId)` is a thread-local holder in `core`:

- **API threads:** a `OncePerRequestFilter` in `rest` sets it from the headers, and clears it in `finally`.
- **Async threads** (decider, sweeper, system-task workers, event processor, `conductor-async`): an ID comes from Redis, the
  lookup resolves the tenant, and the work runs inside `TenantContext.runAs(tenant, account, () -> ...)`.
- **Metadata calls** (`getTaskDef(name)`, `getWorkflowDef(name, v)`) read the tenant from the context, so the DAO interfaces don't change.
  Calling one with no tenant set is a programming error and throws.

### 3.5 Shard ID

```
shard_id = base_shard + bucket
```

| Part | Rule |
|---|---|
| `base_shard` | Chosen once at workflow creation by the deployment's strategy (`conductor.scylla.shard-strategy`): **`account`**: numeric `account_id` (Journeys; it equals the `shardId` their clients already hold and send back), falling back to `0` when the account is `'-1'` or not numeric. **`zero`** (default; shared and `sip`): `0`. |
| `bucket` | `floor(task.seq / shardSize)` when spill is enabled (`conductor.scylla.shard-size > 0`), else `0`. The workflow row is always at `base_shard`. **Ships disabled**; enable it only if the Phase 3 load test shows oversized partitions. |

Why not hash the workflow ID or the account? `workflow_id` is already in the partition key and spreads data evenly, so a
derived shard adds no distribution. The shard is only useful for (a) keeping Journeys' `shardId` contract and (b) splitting one
very large workflow. A shard value only has to be unique within its workflow, so `base + bucket` never collides, even when the
base is an account number.

Where the shard is stored and read:

- `workflow_lookup.shard_id` = `base_shard`; `workflows.total_partitions` (static, on the base partition) = number of buckets in use.
- `task_lookup.shard_id` = the task's exact shard.
- Loading a workflow with tasks reads shards `base_shard .. base_shard + total_partitions - 1` (in parallel when there is more than one).
- **Client hint (Journeys):** when a request carries `shardId` (`outputData.shardId` on task update,
  `/accounts/{acc}/workflows/{id}`), the DAO reads `(tenant, workflow_id, shardId)` directly and skips the lookup. This stays
  tenant-safe because `tenant_id` is in the partition key, and account-safe because the `account_id` static column comes back in
  the same read. If nothing is found there, it falls back to the lookup.

## 4. Placement hierarchy

```
cluster                       per region / dedicated tenant              (ops decision)
 └─ keyspace                  conductor_shared | conductor_sip | conductor_journeys | conductor_<promoted>
     └─ tenant_id             first component of every partition key     (isolation, tenant moves)
         ├─ account_id        workflows_by_account partition + column    (account scoping, account moves)
         └─ workflow_id       partition of workflows                     (even data distribution)
             └─ shard_id      0..n buckets of one workflow               (very large workflows)
```

**Why `account_id` is not in the `workflows` partition key:**
- `workflow_id` already distributes data evenly, so adding the account adds no spread.
- It would force every read to know the account, and async paths only have an ID.

The account dimension instead gets its own index table (`workflows_by_account`). That gives targeted account operations
and keeps the hot read path to a single lookup.

## 5. Keyspace

```sql
CREATE KEYSPACE IF NOT EXISTS conductor_shared
  WITH replication = {'class': 'NetworkTopologyStrategy', '<dc>': 3}
  AND tablets = {'enabled': true};
-- conductor_sip, conductor_journeys: identical DDL, own replication, own CQL role.
-- Local dev: {'datacenter1': 1}. Scylla 2026.x with tablets rejects SimpleStrategy.
```

## 6. Tables (DDL)

All tables are identical in every keyspace. "Change vs Journeys" is the delta from the current module.

### 6.1 Execution

```sql
-- Workflow row + its task rows in one partition (one read loads everything).
CREATE TABLE workflows (
  tenant_id         text,
  workflow_id       uuid,
  shard_id          int,
  entity            text,          -- 'workflow' | 'task'
  task_id           text,          -- '' for the workflow row
  account_id        text STATIC,   -- NEW: once per partition
  total_tasks       int  STATIC,
  total_partitions  int  STATIC,
  payload           text,          -- WorkflowModel / TaskModel JSON (unchanged format)
  version           int,           -- optimistic lock for UPDATE ... IF version = ?
  PRIMARY KEY ((tenant_id, workflow_id, shard_id), entity, task_id)
);
-- Change vs Journeys: tenant_id added to the partition key; account_id static column.

-- ID-only entry point for API calls and async workers. Written at createWorkflow.
CREATE TABLE workflow_lookup (
  workflow_id    uuid PRIMARY KEY,
  tenant_id      text,
  account_id     text,
  shard_id       int,
  workflow_type  text,             -- lets removeWorkflow clean up the index tables without loading the payload
  correlation_id text,
  created_time   timestamp
);
-- Change: + tenant_id, account_id, workflow_type, correlation_id, created_time.

CREATE TABLE task_lookup (
  task_id      uuid PRIMARY KEY,
  tenant_id    text,
  workflow_id  uuid,
  shard_id     int
);
-- Change: + tenant_id. One read gives workflow_id and shard_id (today: two SELECTs).
```

### 6.2 Query tables (new)

```sql
-- GET /workflow/{name}/correlated/{correlationId}. Replaces the full-table ALLOW FILTERING scan.
CREATE TABLE workflows_by_correlation (
  tenant_id      text,
  correlation_id text,             -- stored as received (proxy prefix included), see decision 2
  workflow_type  text,
  workflow_id    uuid,
  account_id     text,
  PRIMARY KEY ((tenant_id, correlation_id), workflow_type, workflow_id)
);

-- Account listing, account deletion/offboarding, account moves. The only account-keyed table.
CREATE TABLE workflows_by_account (
  tenant_id      text,
  account_id     text,
  time_bucket    text,             -- 'YYYY-MM' (proposed); granularity fixed per keyspace
  created_time   timestamp,
  workflow_id    uuid,
  workflow_type  text,
  correlation_id text,
  PRIMARY KEY ((tenant_id, account_id, time_bucket), created_time, workflow_id)
) WITH CLUSTERING ORDER BY (created_time DESC, workflow_id ASC);

-- Running workflows per type. Replaces the Postgres workflow_pending table (COUNT(*) / SKIP LOCKED).
CREATE TABLE workflow_pending (
  tenant_id        text,
  workflow_type    text,
  bucket           int,            -- abs(hash(workflow_id)) % N, N = 16 (proposed)
  workflow_id      uuid,
  workflow_version int,
  PRIMARY KEY ((tenant_id, workflow_type, bucket), workflow_id)
);
```

Only immutable fields go into the query tables. Status is never copied there, so a status change costs no index writes.

### 6.3 Task bookkeeping

```sql
CREATE TABLE task_in_progress_v2 (
  tenant_id          text,
  task_def_name      text,
  task_id            uuid,
  workflow_id        uuid,
  in_progress_status boolean,
  PRIMARY KEY ((tenant_id, task_def_name, task_id))
);
-- Change: + tenant_id. task_in_progress (v1) is dropped.

-- Concurrency limit per task def; the partition is the set of running tasks.
CREATE TABLE task_def_limit (
  tenant_id     text,
  task_def_name text,
  task_id       uuid,
  workflow_id   uuid,
  PRIMARY KEY ((tenant_id, task_def_name), task_id)
);

-- Replaces the stubbed ScyllaPollDataDAO and the proxy's polldata_access_filter.
CREATE TABLE poll_data (
  tenant_id      text,
  queue_name     text,
  domain         text,             -- '' when no domain
  worker_id      text,
  last_poll_time bigint,
  PRIMARY KEY ((tenant_id, queue_name), domain)
);
```

### 6.4 Metadata (tenant-level, no account)

```sql
CREATE TABLE workflow_definitions (
  tenant_id           text,
  workflow_def_name   text,
  version             int,
  workflow_definition text,
  latest_version      int STATIC,  -- NEW: getLatestWorkflowDef without a scan
  PRIMARY KEY ((tenant_id, workflow_def_name), version)
) WITH CLUSTERING ORDER BY (version DESC);

-- Was one global partition 'workflow_def_version_index'; now one partition per tenant.
CREATE TABLE workflow_defs_index (
  tenant_id                 text,
  workflow_def_name_version text,  -- '<name>/<version>'
  workflow_def_index_value  text,
  PRIMARY KEY ((tenant_id), workflow_def_name_version)
);

-- Was partition 'task_defs'.
CREATE TABLE task_definitions (
  tenant_id       text,
  task_def_name   text,
  task_definition text,
  PRIMARY KEY ((tenant_id), task_def_name)
);

-- Was partition 'handlers'.
CREATE TABLE event_handlers (
  tenant_id          text,
  event_handler_name text,
  event_handler      text,
  PRIMARY KEY ((tenant_id), event_handler_name)
);

CREATE TABLE event_executions (
  tenant_id          text,
  message_id         text,
  event_handler_name text,
  event_execution_id text,
  payload            text,
  PRIMARY KEY ((tenant_id, message_id, event_handler_name), event_execution_id)
);  -- written USING TTL when eventExecutionPersistenceTtl > 0
```

### 6.5 Schema versioning

```sql
CREATE TABLE schema_migrations (
  id          int PRIMARY KEY,     -- V1, V2, ... applied in order at startup
  description text,
  applied_at  timestamp
);
```

This replaces the `CREATE TABLE IF NOT EXISTS` calls in `ScyllaBaseDAO.init()` with versioned CQL scripts.

**Count:** 15 tables: 14 data tables plus `schema_migrations`. `task_in_progress` (v1) is removed.

## 7. Access patterns

Every statement binds `tenant_id` first, except the two lookup reads, which are keyed by ID alone and **return** the tenant.

| Operation | Statements | Tenant from |
|---|---|---|
| Register task / workflow def | `INSERT task_definitions`; `INSERT workflow_definitions IF NOT EXISTS` + `UPDATE ... SET latest_version` + `INSERT workflow_defs_index` | context (header) |
| Get def (start, scheduling) | `SELECT ... WHERE tenant_id=? AND workflow_def_name=? [AND version=?]` | context |
| **Create workflow** | one UNLOGGED batch on the workflow partition (`INSERT workflows` entity='workflow', `account_id` static), then `INSERT workflow_lookup`, `INSERT workflows_by_correlation`, `INSERT workflows_by_account`, `INSERT workflow_pending` | context → stored |
| Create tasks | batch: `INSERT workflows` (entity='task') + `total_tasks`; per task `INSERT task_lookup`, `INSERT task_in_progress_v2` | the workflow |
| Get workflow by ID | `SELECT workflow_lookup` → check → `SELECT * FROM workflows WHERE tenant_id=? AND workflow_id=? AND shard_id=?` (each bucket when `total_partitions > 1`) | lookup |
| Get workflow / update task **with a `shardId` hint** (Journeys) | `SELECT ... FROM workflows WHERE tenant_id=<ctx> AND workflow_id=? AND shard_id=<hint>`; account checked against the static `account_id`; on a miss, fall back to the lookup | context + hint |
| Get / update task by ID (poll, worker update) | `SELECT task_lookup` → check → read/write the task row | lookup |
| Update workflow | `UPDATE workflows ... IF version=?` | the workflow |
| Correlated lookup | `SELECT workflow_id FROM workflows_by_correlation WHERE tenant_id=? AND correlation_id=? AND workflow_type=?` → load each | context |
| Account listing | `SELECT ... FROM workflows_by_account WHERE tenant_id=? AND account_id=? AND time_bucket IN (...)` | context |
| Pending/running by type | `SELECT ... FROM workflow_pending WHERE tenant_id=? AND workflow_type=? AND bucket IN (0..N-1)` | context |
| Workflow reaches a terminal state | `DELETE workflow_pending` row | the workflow |
| Remove workflow | delete the partition, each `task_lookup`, `workflows_by_correlation` / `workflows_by_account` / `workflow_pending` rows (keys from `workflow_lookup`), **then** `workflow_lookup` | lookup |
| Remove a single task | delete the task row + `task_lookup` only (no longer touches `workflow_lookup`) | lookup |

### 7.1 Enforcement

At the lookup, which is the single place every ID-only call passes through:

```
row = SELECT ... FROM workflow_lookup WHERE workflow_id = ?
if row == null                                              -> not found
if ctx.tenant != null && ctx.tenant != row.tenant_id        -> not found   (never 403: don't confirm the ID exists)
if ctx.account != null && ctx.account != row.account_id     -> not found   (decision 3: on every ID endpoint)
TenantContext.runAs(row.tenant_id, row.account_id, ...)
```

This replaces the proxy's `workflow_access_filter` and `account_workflow_filter`, and covers every ID-based endpoint:
get, delete, rerun, retry, restart, task get, and task update.

### 7.2 Cache keys

`CacheableMetadataDAO` / `CacheableEventHandlerDAO` currently cache by name only. They must key on `tenant_id + ':' + name`,
or one tenant will be served another tenant's cached definition.

## 8. Write cost per workflow (vs today)

| | Today | Proposed |
|---|---|---|
| Create workflow | 1 insert | 1 insert + 4 index/lookup inserts (lookup moved here from `createTasks`) |
| Per task | 1 lookup batch + 1 task batch + in-progress | same, with the task lookup no longer rewriting `workflow_lookup` |
| Get workflow | 2 reads | 2 reads |
| Get task | 3 reads (two lookup SELECTs) | 2 reads |

## 9. Sharding playbooks this enables

| Move | How | Needs a scan? |
|---|---|---|
| Tenant → own keyspace / cluster | Copy rows `WHERE tenant_id = X` from every table, copy the delta by `WRITETIME`, flip `TENANT_BACKEND_CLUSTER_MAP` | yes, one filtered pass (acceptable for occasional moves) |
| Account → own keyspace / cluster | `workflows_by_account` gives the exact workflow IDs, so copy those partitions + their lookups + index rows; then route `(tenant, account)` at the proxy | **no** |
| Delete an account (offboarding / GDPR) | same ID list, delete partitions and index rows | **no** |
| Very large workflow | set `conductor.scylla.shard-size > 0`: tasks spill to `base_shard + floor(seq / shardSize)` and `total_partitions` tracks the count (§3.5); readers already go through the lookups | no |

An account move needs a proxy route keyed by `(tenant, account)`. Today the map is tenant-only. That's a proxy change only;
no schema change is needed.

## 10. Migration mapping

### 10.1 Journeys Scylla → `conductor_journeys`

| Source | Target | Rule |
|---|---|---|
| `workflows` | `workflows` | copy every column, `tenant_id = 'journeys'`, `account_id = toString(shard_id)`, **keep `shard_id`**, payload byte for byte |
| `workflow_lookup`, `task_lookup` | same | **rebuild** from `SELECT DISTINCT workflow_id, shard_id FROM workflows` and the task rows, not from the source lookups (they're incomplete because of the bugs) |
| — | `workflows_by_account` | `(journeys, account_id, bucket(createTime), createTime, workflow_id, ...)`, with `createTime` taken from the payload |
| — | `workflows_by_correlation` | from `payload.correlationId` |
| — | `workflow_pending` | workflows whose payload status is non-terminal |
| `workflow_definitions` | same | `tenant_id = 'journeys'`; `latest_version = max(version)` per name |
| `task_definitions`, `event_handlers`, `workflow_defs_index` | same | swap the fixed partition value for `'journeys'` |
| `event_executions` | same | add the tenant; write `USING TTL` with the remaining `TTL(payload)` |
| `task_def_limit`, `task_in_progress_v2` | same | add the tenant (or rebuild from running tasks) |

**Journeys API compatibility after the move** (decision 4: Journeys clients keep their `shardId`):
- `conductor_journeys` runs with `shard-strategy=account`, so **new** Journeys workflows also get `shard_id = accountId`, the same value
  their clients already hold. Old and new rows look the same.
- `/accounts/{acc}/workflows/{id}` and `outputData.shardId` on task update use the client-hint fast path (§3.5): a direct partition
  read, with the tenant enforced by the key and the account by the static column, and a fallback to the lookup.
- Journeys clients need no change.

### 10.2 Postgres → `conductor_shared` / `conductor_sip`

| Field | Rule (first match wins) |
|---|---|
| `tenant_id` | `correlationId` prefix `"<t>: "` → `input._tenantContext.tenantID` → `workflowDefinition.ownerApp` → `'default'` (logged) |
| `account_id` | `input._tenantContext.accountID` when ≠ tenant → second prefix segment `"<t>: <a>: "` → `'-1'` |
| `shard_id` | `0` (`shard-strategy=zero`) |
| workflow + tasks | grouped via `workflow_to_task` into one `workflows` partition |
| metadata | tenant = `json_data.ownerApp` or `'default'`; report names owned by more than one tenant first |
| `sip` | every row `tenant_id = 'sip'` (own RDS, no inference) |

Pre-checks for both sources: every `workflow_id` / `task_id` parses as a UUID; per-tenant (and per-account) row counts reconcile after the copy.

## 11. Decisions

| # | Decision | Outcome | Status |
|---|---|---|---|
| 1 | Sentinel for "no account" | `'-1'` (matches `StatusChangePublisher`) | **Decided** |
| 2 | Store `correlationId` with or without the proxy prefix | Keep it as received; the OpenSearch indexing/search logic depends on the prefix | **Decided** |
| 3 | Account guard when `X-Account-ID` is present | Not found for other accounts' workflows on **every** ID endpoint, not just `/accounts/...` | **Decided** |
| 4 | Shard ID | Journeys keeps sending its `shardId` (`shard-strategy=account`, client-hint fast path); our data uses `base 0 + optional spill bucket` (§3.5) | **Decided** |
| 5 | `workflows_by_account` bucket granularity | `YYYY-MM`; switch to `YYYY-MM-DD` per keyspace if a single account exceeds ~100 MB per month | Default, confirm |
| 6 | `workflow_pending` bucket count `N` | 16, fixed per keyspace | Default, confirm |
| 7 | Tenant for events | Deferred (events aren't in use). `event_handlers` and `event_executions` carry `tenant_id` now; the handler's tenant comes from the registering request; the event→tenant/account mapping is designed later | **Decided (deferred)** |
| 8 | Enable shard spill (`shard-size > 0`) | Off at launch; decide after the Phase 3 load test | Open |

## 12. Code change map

| Area | Change |
|---|---|
| `core/.../tenant/TenantContext` (new) | thread-local `(tenantId, accountId)` + `runAs` |
| `rest` filter (new) | `X-Tenant-ID` / `X-Account-ID` → context; per-deployment default |
| `WorkflowModel`, `TaskModel` | `tenantId`, `accountId` fields |
| `StartWorkflowInput` + `WorkflowExecutorOps` (~L1917) | resolve and set on create (§3.3) |
| `SubWorkflow` (~L80), `StartWorkflow` (~L70), `SimpleActionProcessor` (~L220) | inherit from the parent / handler |
| `TaskMapperContext` (~L105), `ForkJoinDynamicTaskMapper` (~L331), `WorkflowExecutorOps` (~L1416) | copy to tasks, next to `correlationId` |
| async entry points (decider, sweeper, system-task worker, event processor) | wrap the work in `runAs` from the loaded workflow |
| `ScyllaProperties` + `ShardStrategy` (new) | `shard-strategy` (`zero` / `account`), `shard-size` (0 = spill off); base shard assigned at create (§3.5) |
| `ScyllaBaseDAO` | DDL from §6 (versioned) |
| `Statements` | `tenant_id` first in every builder; new tables |
| `ScyllaExecutionDAO` | lookup-first reads with the §7.1 check; remove all `parseInt(correlationId)`; write the lookup + index tables at create; fix `removeTaskLookup` |
| `ScyllaMetadataDAO`, `ScyllaEventHandlerDAO` | tenant partitions, `latest_version` |
| `Cacheable*DAO` | tenant in cache keys |
| `ScyllaPollDataDAO` | implement on `poll_data` |
| `TenantMetadataDAO` | single-partition Scylla reads in Scylla mode (drops the Postgres dependency) |
| Tests | cross-tenant / cross-account Spock spec; guard test that every `Statements` query contains `tenant_id` |
