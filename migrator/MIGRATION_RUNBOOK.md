# Conductor DB Migration Runbook (app-level migrator + shared-Redis cutover)

Step-by-step for migrating a Conductor tenant's execution + definition data from one Postgres
DB to another, using the **app-level migrator** (`migrator/` module) and cutting over with the
**shared-Redis** model (the tenant keeps its own Redis; only Postgres changes).

This is the process that was executed for **freshservice** (source DB
`ceapps-conductor-freshservice.conductor.db.ai/conductor` → dest DB
`ceapps.conductor.db.ai/conductor_freshservice`, staging). Reuse it for the next tenant by
substituting the parameters below.

---

## What the migrator does (and doesn't)

- Reads the **source** Conductor over **REST** (`/api/workflow/search` to enumerate,
  `/api/workflow/{id}` to export). It never touches the source DB directly.
- Writes to the **destination Postgres** via **JDBC**, using Conductor's own embedded
  `PostgresExecutionDAO` / `PostgresIndexDAO` / `PostgresMetadataDAO` — so rows are byte-compatible
  with what the dest Conductor expects.
- Keeps dest copies **dormant** (no `verifyAndRepair` bootstrap). The source stays authoritative
  until cutover.
- Migration set = **source non-terminal workflows ∩ source search index**. Definitions
  (task/workflow defs, event handlers) are copied wholesale.

**Does NOT migrate:** terminal workflows (by design — only in-flight), Redis queue state (that's
why cutover reuses the source Redis), or the WAIT-timer callbacks (also preserved via source Redis).

---

## Parameters (fill these in per migration)

| Name | freshservice example |
|---|---|
| SOURCE Conductor REST URL (in-cluster) | `http://conductor-core-freshservice.conductor-freshservice` |
| SOURCE DB (for verification only) | `ceapps-conductor-freshservice.conductor.db.ai:5432/conductor` |
| DEST host | `ceapps.conductor.db.ai:5432` |
| DEST DB name | `conductor_freshservice` |
| DEST DB password secret (host root creds) | `ceapps--staging-postgresql-credentials` (conductor-common's) |
| Tenant namespace | `conductor-freshservice` |
| Tenant Deployments | `conductor-core-freshservice`, `conductor-async-freshservice` |
| Image tag | `conductor-migrator-vN` |

---

## Phase 0 — Prerequisites

1. **Create the destination database** on the dest host (as a user with CREATEDB):
   ```sql
   CREATE DATABASE conductor_freshservice;
   ```
2. **Confirm network reachability** from the migrator's namespace (`shared-services`) to:
   - the source Conductor REST service, and
   - the dest Postgres host:5432.
3. **JDK**: local Gradle builds must run on JDK 21 (`export JAVA_HOME=.../ms-21.0.8`); the Docker
   build uses a containerized JDK 17 so local JDK doesn't matter there.

---

## Phase 1 — Build the migrator image (existing Jenkins pipeline)

The existing `conductor-image-build.groovy` builds the migrator when `SERVICE=conductor-migrator`.

Run the Jenkins job with:

| Param | Value |
|---|---|
| `SERVICE` | **`conductor-migrator`**  ← the switch; `conductor` builds the server instead |
| `VERSION` | `conductor-migrator-vN` (bump N each rebuild; nodes cache by tag with `IfNotPresent`) |
| `CONDUCTOR_OSS_VERSION` | the conductor-oss branch/tag that has `migrator/` + `docker/migrator/` (e.g. `migrator-module`) |
| `GIT_BRANCH` | the ceapps branch with the pipeline change + wrapper (e.g. `origin/migrator-templates`) |
| `STAGING_IMAGE_URL` | `381491938688.dkr.ecr.us-east-1.amazonaws.com/conductor` (reuses the conductor ECR repo) |
| `PRODUCTION_IMAGE_URL` | empty for staging-only |
| `REGION` | `us-east-1` |

Verify the pushed image is actually the migrator (not the server):
```bash
docker run --rm --entrypoint sh <ecr>/conductor:conductor-migrator-vN -c 'ls -1 /app/libs'
# expect: conductor-migrator.jar   (NOT conductor-server.jar)
```

### Gotchas hit here
- **`SERVICE=conductor` builds the server image** and pushes it under your migrator tag — the pod
  then logs `Starting Conductor server` + nginx and crashes on `tenantMetadataDAO … DataSource`.
  Fix: set `SERVICE=conductor-migrator`.
- **`COPY docker/migrator/bin` fails in CI** (`"/docker/migrator/bin": not found`). A bare `bin`
  rule in `.gitignore` silently excludes `docker/migrator/bin/startup.sh`, so it never got pushed.
  Fix: `git add -f` the file + add a scoped un-ignore (`!docker/migrator/bin/`). Already fixed on
  `migrator-module`.

---

## Phase 2 — Deploy the migrator (staging, shared-services)

Overlay: `k8s/workflow-engine/staging/us-east-1/shared-services/conductor-migrator/`
(in ceapps-infra-templates). Set in `config.properties`:
```properties
migrator.mode=metadata,sync
migrator.metadata.writer=jdbc
migrator.source.url=http://conductor-core-freshservice.conductor-freshservice
migrator.dest-datasource.url=jdbc:postgresql://ceapps.conductor.db.ai:5432/conductor_freshservice
migrator.dest-datasource.username=root
migrator.dest-datasource.password=<dest host root password>   # staging: inline; prod: use a Secret
migrator.dest-datasource.init-schema=true
migrator.sync.interval-seconds=15
```
`kustomization.yaml`: image `name: conductor-migrator` → `newName: <ecr>/conductor`,
`newTag: conductor-migrator-vN` (the placeholder name must match the base deployment's
`image: conductor-migrator`, or the override silently no-ops).

Apply + watch:
```bash
kubectl apply -k k8s/workflow-engine/staging/us-east-1/shared-services/conductor-migrator
kubectl -n shared-services rollout restart deploy/conductor-migrator-staging
kubectl -n shared-services logs -f deploy/conductor-migrator-staging
```

Expected log sequence:
```
Starting Conductor migrator
Destination schema ready: N migration(s) applied, schema at version 14
Migrating definitions (source → dest)
Task defs (JDBC): X created, Y updated (Z on source)
Workflow defs (JDBC): X created, Y updated (Z on source)
Definition migration complete
Starting delta-sync loop (interval=15s). Copies are kept DORMANT ...
Delta-sync pass: tracked=N in-sync=N out-of-sync=0 drained=0 errored=0
DELTA=0 — all N tracked tree(s) in sync with source; safe to begin cutover/flip.
```

### Gotchas hit here
- **Flyway wedges after V8, pod stuck `0/1` forever.** Migrations `V9`/`V14` use
  `CREATE INDEX CONCURRENTLY`, which self-deadlocks while Flyway holds its transactional lock.
  Fix (already in `SchemaInitializer`): `flyway.postgresql.transactional.lock=false`. If a DB got
  stuck mid-migration, reset it (`DROP SCHEMA public CASCADE; CREATE SCHEMA public;`) since it's
  fresh, then redeploy.
- **`0/1` for a long time with no error** = readiness waits until schema-init + metadata finish;
  if it never advances, tail logs — usually the source REST URL is unreachable, or Flyway is stuck.
- **Stale/ghost ids** — the source search index can return archived workflow ids that
  `GET /workflow/{id}` 404s. The migrator skips them (`Skipping id … 404`) and continues.
  These are terminal/removed and not migration targets.

---

## Phase 3 — Verify (before cutover)

Migrator log must hold **`DELTA=0`** (`out-of-sync=0 errored=0`) across several passes, and there
must be **no** `Delta-sync pass failed` lines.

DB parity — run on BOTH source and dest, compare ids/status/updatedTime
(note: `json_data` is a **text** column → cast to `json`; the field is **`updatedTime`**):
```sql
select workflow_id,
       json_data::json->>'status'       as status,
       json_data::json->>'workflowName' as name,
       json_data::json->>'updatedTime'  as upd
from workflow
where json_data::json->>'status' in ('RUNNING','PAUSED')
order by workflow_id;
```
Every workflow you intend to migrate must appear on the dest with matching status + updatedTime.
Spot-check task counts: `select workflow_id, count(*) from task group by workflow_id;`

> Reminder: migration set = source non-terminal **∩ source search index**. A workflow that is
> RUNNING in source Postgres but *not indexed* will NOT be migrated. If that matters, migrate it by
> explicit id or add DB-based enumeration.

---

## Phase 4 — Cutover (shared-Redis: repoint the tenant's Postgres, keep its Redis)

Change the tenant's `conductor-core` **and** `conductor-async` overlays (staging):
- `spring.datasource.url` → `jdbc:postgresql://<dest-host>:5432/<dest-db>`
- DB password `secretKeyRef.name` → the dest host's credential secret (e.g.
  `ceapps--staging-postgresql-credentials`)
- **Leave the Redis secrets unchanged** — reusing the tenant's own Redis preserves the queues and
  WAIT timers, so in-flight work resumes without any per-workflow bootstrap.

**Provision the DB secret into the tenant namespace first** (secrets are namespace-scoped — a pod
can only read a secret in its own namespace):
```bash
kubectl -n conductor get secret ceapps--staging-postgresql-credentials -o json \
| jq 'del(.metadata.namespace,.metadata.resourceVersion,.metadata.uid,.metadata.creationTimestamp,.metadata.ownerReferences,.metadata.annotations,.metadata.managedFields)' \
| kubectl -n conductor-freshservice apply -f -
```
(For GitOps, provision it through external-secrets/sealed-secrets so it survives reconciliation.)

**Clean stop-then-start (avoids two-engines-on-one-Redis split-brain):**
```bash
# 1. Final gate: migrator DELTA=0 + parity confirmed.

# 2. Freeze source: scale the tenant conductor to 0 (stops writes to old DB + Redis).
kubectl -n conductor-freshservice scale deploy/conductor-core-freshservice deploy/conductor-async-freshservice --replicas=0
# confirm 0/0 before proceeding
kubectl -n conductor-freshservice get deploy conductor-core-freshservice conductor-async-freshservice

# 3. One more migrator pass → DELTA=0 against the frozen source; re-verify parity.

# 4. Apply the new config (updates the ConfigMap: new DB URL + dest secret). Pods stay at 0.
kubectl apply -k k8s/workflow-engine/staging/us-east-1/conductor-freshservice/conductor-core
kubectl apply -k k8s/workflow-engine/staging/us-east-1/conductor-freshservice/conductor-async

# 5. Scale back up (apply won't do this — replicas didn't change vs last-applied, so live 0 stays).
kubectl -n conductor-freshservice scale deploy/conductor-core-freshservice deploy/conductor-async-freshservice --replicas=2

# 6. Stop the migrator — done.
kubectl -n shared-services scale deploy/conductor-migrator-staging --replicas=0

# 7. Verify.
kubectl -n conductor-freshservice get pods
kubectl -n conductor-freshservice logs -f deploy/conductor-async-freshservice
```

### Gotchas hit here
- **`CreateContainerConfigError` after cutover** = the DB secret doesn't exist in the tenant
  namespace (secrets are namespace-scoped). Fix: replicate it into the tenant namespace (above).
- **Pods `0/1` after start** = readiness not passing; tail core logs — check DB connect
  (`password authentication failed` / `Connection refused` / `database … does not exist`).
- **`kubectl apply -k` does not restore replicas** after a manual `scale --replicas=0` (the
  manifest value is unchanged vs last-applied, so apply skips it) — you must scale up explicitly
  (step 5). Alternative: set `count: 0` in the kustomization, apply, then `count: 2`, apply.

---

## Phase 5 — Post-cutover

- **Commit + push** the tenant overlay changes (URL + secret ref). If Argo/GitOps reconciles the
  cluster, uncommitted changes get reverted.
- **Keep the old DB** as the rollback target until confident. Rollback = revert the two overlay
  changes + restart; the old DB is untouched.
- **Scale the migrator to 0** (or delete the overlay) once cutover is confirmed.
- **Search/UI (OpenSearch)**: execution runs on Postgres+Redis alone, but search/UI visibility of
  migrated workflows depends on the dest OpenSearch index. Reindex if you need UI/search parity.

---

## Known limitations

- WAIT-timer re-arm only survives cutover because Redis is reused; a Redis-less move would stall
  migrated `WAIT(duration)` tasks.
- Enumeration depends on the source search index, so RUNNING-but-unindexed workflows are skipped.
- The migrator holds in-memory sync state → `replicas: 1` + `Recreate` are mandatory.
