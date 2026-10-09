# Scylla POC — handoff for Claude Code

Context carried over from a Cowork session (2026-10-06). Read this first, then continue from "Next steps".

## Goal
Replace Postgres with ScyllaDB as the persistence layer of `conductor-freshworks-oss`, with tenant
isolation enforced by the schema, then migrate existing data (default Postgres cluster, `sip` Postgres,
and Journeys' Scylla data) into it.

Immediate POC goal: **run conductor-freshworks-oss locally on Scylla** (Podman for Scylla + Redis).

## Repos (siblings under `~/Desktop/Projects/Freshworks projects/`)
- `conductor-freshworks-oss` — target. Spring Boot 3.3.5, Java 17, Gradle 8.5. Currently Postgres persistence.
- `netflix-conductor` — Journeys fork, source of `scylla-persistence` (Spring Boot 2 era).
- `ceapps-infra-templates` — k8s/envoy. `conductor-auth-proxy` (Envoy + Lua) does tenant separation today.

## Design docs (claude.ai)
- Schema comparison / Tenant isolation / Schema decision tabs:
  https://claude.ai/code/artifact/1e11b43c-88c7-4459-877f-497f59b08b19
- Execution plan (phases 0–5): https://claude.ai/code/artifact/a800862a-1120-4dde-892b-24246bd5416f
- POC guide (9 steps): https://claude.ai/code/artifact/8fe540d6-fc8c-4922-be62-1fdd913323f4

## Key facts learned
- Freshworks prod: queues + locks on Redis; Postgres for execution/metadata/poll data; indexing is
  Postgres on default cluster, OpenSearch on `sip`. `sip` has its own deployment + RDS.
- Auth proxy isolation (shared cluster): prefixes `correlationId` with `"<tenant>: "`, forces
  `taskToDomain["*"]=<tenant>`, prefixes poll domain, injects `ownerApp` on metadata writes,
  403s `GET /workflow/{id}` if `workflowDefinition.ownerApp` mismatches. Postgres schema itself has no tenant column.
- Journeys Scylla schema: 11 tables, no tenant column. `shard_id = Integer.parseInt(correlationId)`
  (0 if null) — **crashes with the proxy's `"<tenant>: ..."` correlationIds**. `shardSize` unused.
- Journeys bugs: `workflow_lookup` only written in `createTasks`; `removeTaskLookup` deletes the
  workflow's lookup row. Rebuild lookups during migration.
- Metadata names are global in both Postgres and Scylla → cross-tenant overwrite risk.

## Schema decision (agreed direction)
- D1 `tenant_id text` first in every partition key (all keyspaces, same DDL).
- D2 `shard_id` kept, opaque, always read from `workflow_lookup`; new workflows use 0; never parse correlationId.
- D3 `workflow_lookup` / `task_lookup` keyed by ID, add `tenant_id` column; DAO returns *not found* on tenant mismatch.
- D4 metadata partitioned by tenant; `latest_version int STATIC` on `workflow_definitions`.
- D5 one keyspace per deployment (`conductor_shared`, `conductor_sip`, `conductor_journeys`); proxy maps decide placement.
- D6 `account_id` a plain column, not a key.
- D7 new tables `workflows_by_correlation`, `workflow_pending`, `poll_data`, all tenant-first; drop `task_in_progress` v1.

## Done so far (uncommitted, on branch `staging` of conductor-freshworks-oss)
1. Copied `netflix-conductor/scylla-persistence` → `conductor-freshworks-oss/scylla-persistence` (no build dirs).
2. `settings.gradle`: `include 'scylla-persistence'` (after cassandra-persistence).
3. `dependencies.gradle`: `revScylla = '3.11.5.0'`.
4. `scylla-persistence/build.gradle`: test dep → `org.apache.groovy:groovy-all`; kept `:conductor-redis-lock`.
5. `server/build.gradle`: `implementation project(':conductor-scylla-persistence')` + substitution
   `cassandra-driver-core` → `scylla-driver-core` (same packages; avoid duplicate classes).
6. `ScyllaExecutionDAO`: removed `@Override` on `getTask(shardId, workflowId, taskId)` and
   `getWorkflow(shardId, workflowId, includeTasks)` (not in Freshworks ExecutionDAO);
   `getBean("provideRedisLock")` → `getBean(RedisLock.class)` (Freshworks bean is `provideLock`).
7. `Cacheable{Metadata,EventHandler}DAO`: `javax.annotation.PostConstruct` → `jakarta.annotation.PostConstruct`.
8. `core/.../model/WorkflowModel.java`: added `@JsonIgnore private int version` + getter/setter
   (Scylla optimistic locking via `UPDATE ... IF version = ?`; not serialized).
9. User reverted an unrelated local change to `core/.../annotations/Trace.java` (must stay `@Target(TYPE)`).

Verified statically: all ExecutionDAO / MetadataDAO / EventHandlerDAO / PollDataDAO / RateLimitingDAO /
ConcurrentExecutionLimitDAO methods match; no other Journeys-only WorkflowModel/TaskModel methods.

## Status update (2026-10-06, Claude Code) — POC steps 1–5 done
Server boots and runs a workflow end to end on Scylla (`SPRING_PROFILES_ACTIVE=scylla-local`,
`./docker/start-scylla-local.sh`). Extra fixes needed beyond the list above (all uncommitted):
- `scylla/config/cache/CachingConfig`: bean-name clash with cassandra's `cachingConfig` →
  `@Configuration("scyllaCachingConfig")` + `@ConditionalOnMissingClass(cassandra CachingConfig)` (reuses its identical cacheManager).
- `Cacheable{Metadata,EventHandler}DAO`: SpEL `#taskDef.name` / `#eventHandler.name` → `#p0.name` (no `-parameters`; caused 500 on taskdef POST).
- `ScyllaExecutionDAO.setApplicationContext`: `getBean(RedisLock.class)` fails (bean declared as `Lock`) → `(RedisLock) getBean(Lock.class)`.
- `core/.../tenant/TenantMetadataConfiguration`: `@Import(DataSourceAutoConfiguration.class)` (Conductor excludes it; only postgres re-imports).
- Profile `application-scylla-local.properties` (not `application-scylla.properties`): Scylla 2026.x enables **tablets**, which
  reject SimpleStrategy → `NetworkTopologyStrategy` / key `datacenter1` / RF 1, consistency `LOCAL_ONE`; Redis on :6379 with
  `scylla.wf`/`scylla.q` prefixes; Postgres :6432 for TenantMetadataDAO; `spring.flyway.enabled=false`.
- Smoke test used `correlationId=null`; a proxy-style `"t1: ORDER-1"` still crashes (parseInt at ScyllaExecutionDAO 281/429/642/670/733/1101/1184, plus 581/787/796/925).
- CLI: `pipx install scylla-cqlsh` → `cqlsh localhost 9042 -k conductor` (used instead of DBeaver).

## Next steps
1. `./gradlew :conductor-scylla-persistence:compileJava :conductor-server:compileJava` — fix any remaining errors
   (last run failed only on `getVersion/setVersion`, fixed in item 8; not yet re-run).
2. Add Scylla to `docker/docker-compose-podman-local.yaml` (profiles `infra`, `scylla`):
   image `docker.io/scylladb/scylla:6.2`, `--smp 1 --memory 1G --overprovisioned 1 --developer-mode 1`,
   port 9042, container `conductor-scylla-local`. Infra profile already has Redis :7379, Postgres :6432.
   `podman-compose -f docker-compose-podman-local.yaml --profile infra up -d`
3. Create `server/src/main/resources/application-scylla.properties`:
   ```
   conductor.db.type=scylla
   conductor.scylla.hostAddress=localhost
   conductor.scylla.port=9042            # module default is 9142!
   conductor.scylla.keyspace=conductor
   conductor.scylla.replicationStrategy=SimpleStrategy
   conductor.scylla.replicationFactorKey=replication_factor
   conductor.scylla.replicationFactorValue=1
   conductor.queue.type=redis_standalone
   conductor.redis.hosts=localhost:7379:us-east-1c
   conductor.redis.availabilityZone=us-east-1c
   conductor.redis.dataCenterRegion=us-east-1
   conductor.app.workflowExecutionLockEnabled=true
   conductor.workflow-execution-lock.type=redis
   conductor.redis-lock.serverType=single
   conductor.redis-lock.serverAddress=redis://localhost:7379
   conductor.indexing.enabled=false
   conductor.external-payload-storage.type=dummy
   # TenantMetadataDAO (core/.../tenant) needs a DataSource unconditionally — temp workaround:
   spring.datasource.url=jdbc:postgresql://localhost:6432/postgres
   spring.datasource.username=conductor
   spring.datasource.password=conductor
   spring.flyway.enabled=false
   ```
4. Run: `./gradlew :conductor-server:bootRun --args='--spring.profiles.active=scylla'`
   Expect log `... initialization complete! Tables created!`; `cqlsh -k conductor -e "DESCRIBE TABLES"` → 11 tables.
5. Smoke test (keep `correlationId` numeric until D2 is implemented): create taskdef `poc_task`,
   workflow `poc_flow`, start with `"correlationId":"1"`, poll, complete, check status COMPLETED.
6. Then implement the schema decision (D1–D7): `TenantContext` (X-Tenant-ID filter), tenant_id in
   `ScyllaBaseDAO` DDL + every `Statements` builder + every `bind()`, lookup tenant check, remove
   `parseInt(correlationId)`, fix the two lookup bugs, add new tables, then cross-tenant tests.

## GUI
DBeaver Community + `ing-bank/cassandra-jdbc-wrapper` bundle jar; class
`com.ing.data.cassandra.jdbc.CassandraDriver`, URL `jdbc:cassandra://{host}:{port}/{database}?localdatacenter=datacenter1`.
Helper script: `setup-scylla-gui.sh` (from the Cowork session outputs).