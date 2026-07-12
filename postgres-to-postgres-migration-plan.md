# Postgres → Postgres Migration Plan (in-flight, ~zero downtime)

Migrating a Conductor deployment from one Postgres database to another (new
instance / cluster / region / account) **including in-flight executions**, with
little-to-zero downtime.

---

## Why this is "easy mode"

Source and target run the **same Conductor schema**, so the migration happens at
the **database layer** using native Postgres **logical replication**. In-flight
`RUNNING` workflows, their tasks, variables, and payloads replicate as ordinary
rows — the target becomes a live mirror. **No app-level reconstruction, no import
tooling, no DAO changes.** Row IDs are preserved, which is exactly what makes
in-flight continuity trivial.

### Schema facts that shape the plan
- Tables use `id SERIAL` → **sequence state must be synced at cutover** (logical
  replication does **not** copy sequence positions; skipping this causes
  duplicate-key errors on the target).
- All tables have primary keys → logical replication is supported.
- `conductor.queue.type=redis_standalone` → the live queues are in **Redis**, not
  Postgres. The Postgres `queue`/`queue_message` tables are idle. **Redis is the
  in-flight coordination point**, not the DB.
- `conductor.external-payload-storage.type=postgres` → payloads live in the DB and
  replicate for free.

---

## Recommended approach

**Logical replication + a short quiesce-and-repoint cutover.**
Downtime = a brief quiesce window (seconds to ~1–2 min), not a data copy.

### Prerequisites (parity)
- Identical **Conductor version** on both sides.
- Identical **Flyway schema version** (create target schema via Flyway, then
  **freeze migrations** for the window — logical replication does not replicate DDL).
- Source Postgres `wal_level=logical`.
- Every replicated table has a PK / replica identity (✅ already true).

### AWS RDS specifics (both source and target are RDS Postgres)
Using **native logical replication only** — no AWS DMS.
- `wal_level=logical` is set via the DB **parameter group** parameter
  `rds.logical_replication=1` (this also sizes `max_wal_senders`/
  `max_replication_slots`/`max_worker_processes` for you). Apply to the parameter
  group attached to the **source**, then **reboot the source instance**.
  - This reboot is a **separate downtime window from the cutover** — schedule it
    days ahead, off-peak. Single-AZ: ~1–3 min unavailable. Multi-AZ: use
    "reboot with failover" → ~≤60s (fails over to standby instead of a cold
    restart).
- No real superuser on RDS — grant the replication role instead:
  `GRANT rds_replication TO <app_user>;` on **both** source and target.
- **Network path**: target must reach source on 5432 — same VPC, or VPC
  peering/PrivateLink for cross-account/cross-region. Verify with a plain
  `psql` connection test before touching replication config.
- **SSL**: if `rds.force_ssl` is on, add `sslmode=require` to the subscription's
  `CONNECTION` string.
- **Slot disk risk**: if the subscription ever disconnects for an extended
  period, the source's replication slot prevents WAL recycling — storage
  autoscaling on RDS will silently grow (and bill) instead of erroring. Monitor
  slot age / `pg_replication_slots` during the sync window.

---

## Phases

### Phase 1 — Stand up target + start replication (no downtime)
1. Provision the target Postgres; run Flyway to the **exact same version** as source.
2. On **source**: create a publication for the Conductor tables.
   ```sql
   CREATE PUBLICATION conductor_pub FOR TABLE
     workflow, task, workflow_to_task, workflow_pending, task_in_progress,
     task_scheduled, task_execution_logs,
     meta_workflow_def, meta_task_def, meta_event_handler,
     event_execution, poll_data,
     workflow_index, task_index;
   -- Skip transient tables: locks; and the idle queue / queue_message (Redis-backed).
   ```
3. On **target**: create the subscription (initial snapshot copy + continuous stream).
   ```sql
   CREATE SUBSCRIPTION conductor_sub
     CONNECTION 'host=SOURCE_HOST port=5432 dbname=conductor user=... password=...'
     PUBLICATION conductor_pub;
   ```
4. Let it run until **replication lag ≈ 0** (monitor `pg_stat_replication` /
   `pg_stat_subscription`).

### Phase 2 — Cutover window (the only downtime, seconds)
1. **Quiesce the source Conductor** — stop the reconciler/sweeper + workers (or
   maintenance mode) so no new writes occur. *(single-writer guarantee)*
2. **Drain replication** — wait for lag → 0. Target is now byte-identical,
   including in-flight workflows.
3. **Sync sequences on target** — advance every `id` sequence to the source's
   current value (+ buffer). **Mandatory.** Example per table:
   ```sql
   SELECT setval(pg_get_serial_sequence('workflow','id'),
                 (SELECT COALESCE(MAX(id),0)+1000 FROM workflow), false);
   -- repeat for every table with a SERIAL id
   ```
4. **Repoint Conductor + workers** `spring.datasource.url` → target Postgres.
5. **Redis** — keep the **same Redis** (shared/unchanged). Because workflowIds are
   preserved, existing decider/task-queue entries still resolve → **in-flight
   continues with zero re-seed.**
6. **Resume Conductor** on the target. Drop the subscription
   (`DROP SUBSCRIPTION conductor_sub;`).

### Phase 3 — Verify & decommission
- Confirm in-flight workflows advance, new workflows start, and row counts match
  (watch `workflow`/`task` metrics in Grafana).
- Soak, then decommission the source DB.

---

## Conductor-specific rules (what differs from a generic PG migration)

1. **Single active writer.** Never run two Conductor clusters against source +
   target at the same time — that is split-brain / double execution. Source writes
   until Phase 2 step 1; target writes only after step 6.
2. **Redis is the in-flight linchpin.**
   - **Shared Redis across the switch (recommended):** zero re-seed, in-flight just
     continues.
   - **New Redis:** you must re-seed the decider queue on the new side — Conductor
     does **not** rebuild it from Postgres automatically. Bulk-trigger
     `PUT /api/workflow/decide/{workflowId}` for every RUNNING workflow after cutover.
3. **Sequences + schema freeze** as described above.

---

## Gotchas
- **Sequences not replicated** → handled in Phase 2 step 3.
- **No DDL replication** → freeze Flyway migrations during the window.
- **Version skew** between the two Conductors → keep identical.
- If queues are ever moved to Postgres, `queue_message` becomes very high-churn —
  reconsider replication scope at that point.

---

## Rollback
The source DB is untouched and stays consistent. If the target misbehaves after
cutover, repoint Conductor back to the source (with shared Redis, in-flight state
is intact). Fast and low-risk.

---

## Downtime & effort
- **Downtime:** only the quiesce window — realistically **< 1–2 minutes** (mostly
  repoint + restart); effectively **zero** if the datasource can be hot-repointed
  (connection-pool swap without a restart).
- **Effort:** **~1–2 weeks** — replication setup + dry-run + cutover runbook +
  rehearsal. Much cheaper than a cross-engine migration because there is no
  reconstruction.

---

## Decisions to lock
1. **Shared Redis across cutover** (recommended, zero re-seed) vs. new Redis
   (requires re-decide of running workflows)?
2. **Whole-DB** logical replication vs. **scoped** to non-transient tables?
3. Is a **few-seconds quiesce** acceptable, or is **absolute zero** required
   (hot datasource repoint without restart)?

---

## Alternatives considered
- **`pg_dump`/restore:** simplest but requires downtime for the whole dump+restore
  (not suitable for in-flight / zero downtime).
- **Physical streaming replica + promote:** near-zero downtime but copies the entire
  cluster and requires the same major version; less selective than logical replication.
- **Managed DMS (cloud):** equivalent to logical replication, managed — viable if
  both databases are in a supported managed service. **Decided against**: native
  logical replication gives direct control over publication scope, sequence
  sync timing, and cutover sequencing without an extra managed layer (DMS
  serverless environment, IAM role, Secrets Manager secrets) to provision and
  clean up.
