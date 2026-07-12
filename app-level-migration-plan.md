# App-Level Conductor Migration — Implementation Spec (v2: Standalone Migrator Service)

**Goal:** migrate Conductor workflow executions — **including in-flight (RUNNING/PAUSED) and
pending workflows** — from a **source** Conductor deployment to a **destination** Conductor
deployment with **zero-to-minimal downtime**.

**Architecture decision (v2):** all migration logic lives in a **standalone migrator service**.
**Zero changes to Conductor itself** — no new endpoints, no redeploys of either cluster. The
service:

1. calls **source** Conductor REST APIs (read-only export + pause/terminate),
2. calls **destination** Conductor REST APIs (bootstrap: decide / verifyAndRepair),
3. connects **directly to the destination Postgres** — but *only* through Conductor's own
   embedded DAO classes (see D2 below, this is mandatory),
4. **continuously syncs in-flight changes** (pre-copy + delta sync) so the final flip pause is
   near-zero.

**Non-goals:** migrating terminal (COMPLETED/FAILED/TERMINATED) history — it stays on the
source and ages out via retention TTL (`conductor.workflow-status-listener.type=archive` +
`archival.ttlDuration`). Queue contents are never copied — they are **rebuilt** on the
destination by the bootstrap call (fact F5/F6 below).

---

## 1. Verified engine facts (foundation — do NOT re-derive, do NOT skip reading)

File paths relative to the `conductor-freshworks-oss` repo root. Every design decision below
traces back to one of these.

| # | Fact | Evidence |
|---|------|----------|
| F1 | `PostgresExecutionDAO.createWorkflow` inserts using the **workflow's own `workflow_id`** (app-supplied UUID string). `updateWorkflow` overwrites in place. Importing/re-importing a workflow with a preserved ID is natively supported. | `postgres-persistence/.../dao/PostgresExecutionDAO.java:305-311`, insert at `:637` |
| F2 | `PostgresExecutionDAO.createTasks` is **idempotent**: dedups on `taskKey` (referenceTaskName + retryCount), skips already-scheduled tasks. Crashed imports can be blindly retried. | `PostgresExecutionDAO.java:136-171` |
| F3 | `createTasks` **overwrites `scheduledTime` with now()**. Imported pending tasks get a fresh timeout clock (benign; but absolute WAIT deadlines shift — see §6). | `PostgresExecutionDAO.java:146` |
| F4 | The **Redis decider queue (`_deciderQueue`) is the ONLY trigger** for workflow evaluation. The reconciler pops IDs from that queue; it **never scans the database**. A workflow present in the DB but absent from the decider queue is **inert — the engine will never touch it**. | `core/.../reconciliation/WorkflowReconciler.java:70`; `core/.../utils/Utils.java:25` |
| F5 | `WorkflowRepairService` re-enqueues missing queue entries: SCHEDULED tasks' queue messages **and** the workflow's decider-queue entry. | `core/.../reconciliation/WorkflowRepairService.java:141-173`; property `conductor.workflow-repair-service.enabled=true` |
| F6 | Repair auto-runs **only from the sweeper**, which only runs for IDs popped from the decider queue (F4). ⇒ repair cannot wake an inert workflow. **One explicit bootstrap API call per workflow is required to activate it** — and conversely, F4+F6 guarantee an imported-but-not-bootstrapped workflow stays **dormant**. Dormancy is what makes safe pre-copy possible (§4). | `WorkflowSweeper.java:89-91` + F4 |
| F7 | Destination bootstrap endpoints that **already exist** (single-ID): `PUT /api/workflow/decide/{id}` (full decide pass); `POST /api/admin/sweep/requeue/{id}` (decider-queue push only); `POST /api/admin/consistency/verifyAndRepair/{id}` (**preferred**: repairs decider entry AND task-queue entries in one call). No bulk variants exist — the service loops per-ID. | `rest/.../WorkflowResource.java:131`, `rest/.../AdminResource.java:59,65` |
| F8 | Bulk endpoints that exist and are used by the service: `PUT /api/workflow/bulk/pause`, `PUT /api/workflow/bulk/resume`, `POST /api/workflow/bulk/terminate` (max 1000 IDs, per-ID `BulkResponse`). | `rest/.../WorkflowBulkResource.java` |
| F9 | **Sub-workflows link by ID both ways**: parent `SUB_WORKFLOW` task holds `subWorkflowId`; child holds `parentWorkflowId`. The migration unit is therefore the **workflow tree**, moved atomically. | `core/.../tasks/SubWorkflow.java:85-93` |
| F10 | Workflow/task JSON may carry **external payload pointers** (`externalInputPayloadStoragePath` / `externalOutputPayloadStoragePath`) instead of inline data. Copying JSON copies the pointer, not the blob. | `common/.../run/Workflow.java:110-113` |
| F11 | There is **no native import API** on the server — which is fine in v2: the service writes execution state via the **embedded DAO** (D2), so no server change is needed. | verified: no such endpoint in `rest/` |
| F12 | Retention: `workflow-status-listener.type=archive` + `archival.ttlDuration`. Functional on Postgres (`PostgresExecutionDAO.java:338`), **UnsupportedOperationException on Scylla** — source retention may work differently; get the real TTL from the source team's deploy config. | docs + `ScyllaExecutionDAO.java:759-761` (netflix-conductor repo) |

---

## 2. Architecture

```
                 (A) new StartWorkflow / event triggers ──────────────┐
                                                                      ▼
┌─────────────────────┐        REST (read + pause/term)   ┌───────────────────────┐
│  SOURCE Conductor   │◀──────────────┐                   │ DESTINATION Conductor │
│  (Scylla or PG      │               │                   │ (Postgres + Redis)    │
│   + its Redis)      │       ┌───────┴────────┐  REST    │                       │
│  workers keep       │       │   MIGRATOR     │─────────▶│ decide /              │
│  polling (draining) │       │   SERVICE      │ bootstrap│ verifyAndRepair /     │
└─────────────────────┘       │  (Java/Spring  │          │ bulk pause-resume     │
                              │   Boot, state- │          └───────────▲───────────┘
                              │   less +       │   embedded DAO      │ workers poll
                              │   checkpoints) │   (JDBC, direct)    │ (new + migrated)
                              └───────┬────────┘                     │
                                      └──────────────────────────────┘
                                        writes execution state into
                                        DESTINATION Postgres via
                                        conductor-postgres-persistence DAOs
```

### Design rules (mandatory)

- **D1 — Zero Conductor changes.** Both clusters run unmodified. All logic is in the service.
- **D2 — Never hand-write SQL against the destination.** The destination schema has ~8 derived
  tables per write (`workflow`, `workflow_def_to_workflow`, `workflow_pending`,
  `workflow_to_task`, `task`, `task_in_progress`, `task_scheduled`, index tables).
  The service therefore **embeds Conductor's own persistence code as a library**:
  depend on the repo's `conductor-postgres-persistence` + `conductor-core` +
  `conductor-common` jars and instantiate `PostgresExecutionDAO` (via a Spring context or
  direct construction with a `DataSource`, `ObjectMapper`, `RetryTemplate`) pointed at the
  destination DB. This is why the service **must be Java** — any other language forces the
  hand-written-SQL trap.
- **D3 — Source is accessed via REST only** (works identically for Scylla or Postgres source;
  the service never needs source DB credentials).
- **D4 — Dormant-until-flipped.** Imported workflows are NOT bootstrapped during sync. Per
  F4+F6 the destination engine ignores them completely until the service calls
  `verifyAndRepair`. Import ≠ activate.
- **D5 — Exactly-once ownership.** Each workflow ID is owned by exactly one system at any
  instant, tracked in the service's checkpoint DB (§5.5). Flip is the only ownership change.
- **D6 — Everything idempotent/checkpointed.** Any step can crash and be re-run (F1/F2 give
  this for imports; bootstrap repair is naturally idempotent).

---

## 3. Migrator service — concrete component spec

Spring Boot app, single deployable, horizontal scaling optional (shard by tree hash).

| Module | Responsibility | Implementation notes |
|---|---|---|
| `SourceClient` | REST client for source: search non-terminal workflows, `GET /api/workflow/{id}?includeTasks=true`, `PUT /api/workflow/bulk/pause`, `PUT /api/workflow/bulk/resume`, `POST /api/workflow/bulk/terminate`. | plain WebClient/RestTemplate + auth header config |
| `DestClient` | REST client for destination: `POST /api/admin/consistency/verifyAndRepair/{id}`, `PUT /api/workflow/decide/{id}`, `GET /api/workflow/{id}` (verification). | same |
| `DestWriter` | Writes execution state into destination DB **via embedded `PostgresExecutionDAO` / `ExecutionDAOFacade`** (D2): `createWorkflow`, `createTasks`, `updateWorkflow`, `updateTask`. Converts the exported `Workflow` JSON (common model) → `WorkflowModel`/`TaskModel`. | build the DAO with destination `DataSource`; run NO Flyway from the service (schema owned by destination Conductor) |
| `TreeResolver` | Groups workflows into **trees** via `parentWorkflowId`/`subWorkflowId` (F9). Input: workflow export JSON. Output: ordered tree (children first). | recurse `tasks[].subWorkflowId`; cycle-guard |
| `SafePointGate` | Tree is flippable iff **no task in any workflow of the tree is `IN_PROGRESS`** (terminal or SCHEDULED/waiting only). | pure function over exports |
| `PayloadHandler` | Detects `external*PayloadStoragePath` (F10); downloads blob from source payload store, inlines it into the JSON **or** uploads to destination store and rewrites the path. | must be verified in P0 spike |
| `SyncLoop` | Pre-copy + delta sync (§4, states NEW→COPIED→SYNCING). | polling by `updateTime` |
| `FlipOrchestrator` | Executes the flip state machine (§4, SYNCING→FLIPPED). | the only code that pauses source workflows |
| `CheckpointStore` | Service-owned Postgres schema (NOT the Conductor schema): tables `migration_run`, `tree_state(tree_id, root_workflow_id, state, last_synced_at, export_hash, error)`, `workflow_ownership(workflow_id, system, flipped_at)`. | plain JDBC/JPA, service-private |
| `Verifier` | Post-flip assertions (§4 step F7, §7). | REST reads on both sides |
| `Api/CLI` | operator controls: start/stop run, batch size, dry-run, status report, per-tree retry/skip. | minimal REST or CLI |

Config (env): `source.url`, `source.auth`, `dest.url`, `dest.auth`, `dest.datasource.*`,
`batch.size` (default 10 trees), `sync.pollInterval` (default 10s), `flip.maxPauseMillis`.

---

## 4. Migration lifecycle per tree — the exact state machine

Terminal history is never touched. Every non-terminal workflow tree goes through:

```
NEW ──▶ COPIED ──▶ SYNCING ──▶ FLIPPING ──▶ FLIPPED ──▶ VERIFIED
                      │                        (on any failure before DISABLE: resume source,
                      └────▶ DRAIN_ON_SOURCE    revert to SYNCING; after DISABLE: fix forward)
```

### Phase A — PRE-COPY (source keeps running; destination copy is dormant)

- **A1. Enumerate.** `SourceClient` lists non-terminal workflows (search
  `status IN (RUNNING, PAUSED)` + SCHEDULED-only pending). Re-enumerate every pass; the set
  only shrinks (new starts already go to destination, §7 P2).
- **A2. Resolve tree.** `TreeResolver` builds the tree (F9). The whole tree is one unit forever.
- **A3. Export.** `GET /workflow/{id}?includeTasks=true` for every workflow in the tree.
- **A4. Payloads.** `PayloadHandler` inlines/copies external payloads (F10).
- **A5. Import dormant.** `DestWriter.createWorkflow` + `createTasks` for each workflow,
  **children first**, preserving all IDs and statuses (F1/F2). **Do NOT bootstrap.** Per D4 the
  copies are inert (F4+F6). Record state = COPIED, store `export_hash`.

### Phase B — DELTA SYNC (both copies exist; source is authoritative)

- **B1.** Every `sync.pollInterval`: re-export the tree from source; compare
  `updateTime`/hash per workflow.
- **B2.** Changed → re-import: `updateWorkflow` + `createTasks`/`updateTask` via `DestWriter`
  (idempotent, F1/F2). Destination copy converges to near-real-time. State = SYNCING.
- **B3.** If the tree **terminates on source** during sync → it finished before flip: delete
  the dormant destination copy (or import final terminal state if history-on-destination is
  wanted), state = DRAIN_ON_SOURCE. Done — this is normal and fine.

### Phase C — FLIP (the only pause; target < a few seconds per tree)

- **C1. Gate.** `SafePointGate` on the freshest export: **no `IN_PROGRESS` task anywhere in
  the tree** (a task held by a worker would report to source and be lost). Not safe → stay
  SYNCING, retry next pass.
- **C2. Pause source tree.** `PUT /api/workflow/bulk/pause` (all IDs in the tree).
- **C3. Re-check the gate** on a fresh export **after** pausing (closes the race where a
  worker picked up a task between C1 and C2). Violated → `bulk/resume` on source, back to
  SYNCING.
- **C4. Final delta.** Re-export → re-import the last changes (usually nothing).
- **C5. BOOTSTRAP destination** — the wake-up call (F6/F7), children first, per ID:
  `POST /api/admin/consistency/verifyAndRepair/{id}`. This re-creates the decider-queue entry
  + task-queue entries; the destination engine now owns execution. (Workflows that were PAUSED
  on source were imported as PAUSED — they wake paused; owners resume them later.)
- **C6. DISABLE on source** — exactly-once guarantee:
  `POST /api/workflow/bulk/terminate` with reason `MIGRATED:<runId>` (or leave paused forever —
  pick ONE convention for the whole run; terminate is cleaner but check no status-listener side
  effects on source). Record ownership flip in `workflow_ownership`. State = FLIPPED.
- **C7. Verify** (state = VERIFIED):
  - destination `GET /workflow/{id}` matches final export (status, task count);
  - within ~2 sweep intervals the tree **advances** (a SCHEDULED task appears in a task queue /
    changes state) OR is legitimately waiting (WAIT/event);
  - source workflows show TERMINATED-with-`MIGRATED` reason (or PAUSED per convention).

### Failure handling

| Failure point | Action |
|---|---|
| A/B any step | retry blindly (idempotent, F1/F2); tree stays SYNCING |
| C2–C4 | `bulk/resume` on source → back to SYNCING (source still authoritative; destination copy still dormant) |
| C5 partially done | re-run C5 for the whole tree (repair is idempotent); do NOT resume source |
| C6 fails after C5 | **both engines could act** → highest-priority alert; retry terminate immediately; workers' idempotency (§6 precondition) is the backstop |
| after C6 | fix forward on destination only |

---

## 5. Routing & coexistence during the migration window

- **5.1 New starts:** from cutover-start, ALL `StartWorkflow` calls and event-handler triggers
  point at the destination. (This is a client/config change, not a Conductor change.)
- **5.2 Workers dual-poll** both systems for the whole window. Each task exists in exactly one
  system's queues, so there is no duplication. Drop source polling when the source non-terminal
  set is empty.
- **5.3 Lookups/callbacks by workflowId:** dual-read (destination first, fallback source), or
  consult `workflow_ownership`.
- **5.4 Definitions precondition:** all workflow/task defs + event handlers exist on the
  destination **before** Phase A (definitions migration is a separate, earlier step: export via
  source `GET /api/metadata/*`, register via destination `PUT /api/metadata/*`, all versions,
  task defs first). Freeze source definition edits during the window or re-sync before each batch.
- **5.5 Ownership directory** lives in the service's checkpoint DB; it is the single source of
  truth for "who owns workflow X".

---

## 6. Edge cases — explicit handling (each needs a test)

| Case | Handling |
|---|---|
| **Sub-workflow trees** (F9) | Tree = atomic unit; children-first in every import/bootstrap loop; tree-aware safe point. Never migrate a partial tree. |
| **External payloads** (F10) | Phase A4/B2: inline or copy+rewrite. P0 spike item. |
| **`scheduledTime` reset** (F3) | Accepted (fresh timeout clock is safer than instant timeout). |
| **WAIT with duration/`until`** | Timer normally lives in a queue `callbackAfterSeconds` — which is NOT copied. After bootstrap, verify the wait re-arms from task input; if the remaining delay is lost, compute remaining wait in the service and re-apply at C5 (e.g. via requeue with delay). **P0 spike item — must verify.** |
| **Tasks in retry-backoff (`callbackAfterSeconds`)** | Repair re-enqueues without the original delay → may fire early. Harmless if workers are idempotent; else re-apply delay at C5. |
| **PAUSED source workflows** | Import as PAUSED; they wake paused after C5; owners resume on destination. |
| **Trees that never hit a safe point** | Stay SYNCING with a max-age; report; either coordinate a quiet moment with the owning team or let them DRAIN_ON_SOURCE. |
| **Human/API action on a dormant destination copy** (someone terminates/retries a not-yet-flipped ID on destination) | Low risk; mitigation: ownership dashboard + operator convention; verifier detects divergence via export_hash on next sync pass and re-imports. |
| **`correlationId` duplicate reads during window** | Dual-read de-dup by workflowId + ownership. |
| **Task concurrency limits (`task_def_limit`)** | Rebuilt as tasks run on destination; brief cross-system double-counting possible — flag any task def relying on strict concurrency=1. |

---

## 7. Build & rollout plan (ordered; each step has a done-check)

| Phase | Steps | Done-check |
|---|---|---|
| **P0 — Spike (1–2 wk)** | Local: destination Conductor (Postgres+Redis, repair enabled) + the embedded-DAO harness. (a) import a waiting workflow with preserved IDs → verifyAndRepair → completes; (b) sub-workflow tree; (c) WAIT-with-duration timer re-arm; (d) external-payload workflow; (e) dormancy: imported-not-bootstrapped workflow untouched for 24h. | all five pass, findings folded back into §6 |
| **P1 — Service build (2–3 wk)** | Spring Boot skeleton; embed `conductor-postgres-persistence`/`core`/`common` jars (D2); implement modules of §3; checkpoint schema; CLI/status API; dashboards (migrated count, SYNCING age, flip failures, both `_deciderQueue` depths). | E2E on staging: seeded source → full A→C lifecycle → invariants of §8 hold |
| **P2 — Cutover-start** | Route new starts + event triggers → destination; workers dual-poll; definitions frozen & synced (§5.4). | new executions appear only on destination |
| **P3 — Trickle run** | Batches of `batch.size` trees; monitor; handle skips/stragglers. | source non-terminal set → 0 (minus accepted drains) |
| **P4 — Drain & decommission** | Stragglers finish on source; retention TTL clears history; stop dual-poll; retire source cluster + the migrator run. | source empty past retention window |

---

## 8. Verification invariants (assert continuously during P3)

1. **No double execution:** no workflowId is simultaneously non-terminal on both systems
   (join `workflow_ownership` against both search APIs).
2. **Convergence:** for every SYNCING tree, destination `export_hash` ≤ `sync.pollInterval`
   stale.
3. **Liveness after flip:** every FLIPPED tree advances or is legitimately waiting within 2
   sweep intervals (else alert).
4. **Dormancy:** destination decider queue contains no IDs of SYNCING (unflipped) trees.
5. **Counts:** migrated + drained + remaining == initial enumeration (per run).

Metrics to watch (both clusters, Prometheus/Grafana): `task_queue_depth`, `_deciderQueue`
gauge, `workflow_running` per def, service throughput/error/skip counters.

---

## 9. Effort

| Item | Estimate |
|---|---|
| P0 spike | 1–2 wk |
| Migrator service (modules §3, incl. embedded DAO wiring) | 2–3 wk |
| Routing/dual-poll rollout + dashboards | ~1 wk |
| E2E rehearsal + runbook + production trickle (P2–P4) | 1–2 wk |
| **Total** | **~5–8 wk** |

**Preconditions to confirm with workflow owners before P2:**
1. **Task idempotency** for anything re-schedulable (backstop for C6-failure and early-fire).
2. Never-safe trees may **finish on source** (drain) — acceptable?
3. Source **retention TTL value** (sets decommission date; F12 — verify how retention actually
   works on the Scylla source, since `removeWorkflowWithExpiry` is unsupported there).
4. Does the source use **external payload storage**? (activates PayloadHandler work)
5. **Terminate-vs-pause-forever** convention for C6 (check source status listeners).
