# Freshworks Fork: Customizations Over Upstream

This page documents what Freshworks' fork adds or changes relative to
[conductor-oss/conductor](https://github.com/conductor-oss/conductor). It is intended for engineers
looking this component up in Prism, not as a replacement for the upstream [Developer's
Guide](devguide/concepts/why.md) or [API/configuration reference](documentation/api/index.md), which
still apply as-is.

## Multi-tenancy support

A parallel, read-only, tenant-scoped metadata API sits alongside the stock metadata API. It filters
`WorkflowDef`/`TaskDef` rows by tenant, keyed off the pre-existing upstream `ownerApp` field (declared
on `Auditable`, inherited by `WorkflowDef`/`TaskDef`/`WorkflowModel`) — Freshworks repurposes this stock
field as the tenant identifier rather than adding a new column.

- `core/src/main/java/com/netflix/conductor/tenant/TenantMetadataConfiguration.java` — wires a
  `TenantMetadataDAO` bean from the app's `DataSource` + `ObjectMapper`.
- `core/src/main/java/com/netflix/conductor/tenant/TenantMetadataDAO.java` — raw JDBC DAO querying the
  `meta_workflow_def` / `meta_task_def` JSONB columns directly (`json_data::jsonb->>'ownerApp'`), bypassing
  the normal `MetadataDAO`/`MetadataService` abstraction. Currently Postgres-shaped only.
- `rest/src/main/java/com/netflix/conductor/rest/controllers/TenantMetadataResource.java` — controller at
  `/api/tenant/metadata`. Every endpoint requires header `X-Tenant-ID`, with no default/fallback:
  `GET /workflow`, `/workflow/latest-versions`, `/workflow/names`, `/workflow/names-and-versions`,
  `/workflow/{name}/versions`, `/workflow/{name}`, `/taskdefs`, `/taskdefs/{tasktype}`.

**This is a separate surface, not a filter on the existing API** — the stock endpoints in
`MetadataResource.java` (`/metadata/workflow/names`, `/metadata/workflow/{name}/versions`, etc.) delegate
straight through `MetadataServiceImpl` to `metadataDAO` with no tenant awareness. Tenant scoping only
applies under `/api/tenant/metadata/*`.

On the UI side, `ui/src/components/TenantContext.jsx` fetches `/api/userinfo` (served by an external
gateway/BFF, not by this repo) for `{ tenants: { tenantId: role }, email }`, persists the active tenant in
an `x-tenant-id` cookie, and reloads the page on tenant switch. `ui/src/components/TenantSelect.jsx` is the
switcher dropdown.

Separately, workflow/task payloads can carry a `_tenantContext` object that gets propagated into outbound
webhook payloads — see [Central event publishing](#central-event-webhook-publishing) below.

## Central event/webhook publishing

A pluggable, queue-buffered publisher pipeline pushes workflow/task status-change notifications as JSON to
an external HTTP endpoint ("Central"), wrapped in an envelope of `account_id`, `payload_type`,
`payload_version`, `payload`. A composite listener allows several publisher types to run concurrently
(webhook, Kafka, internal queue, S3 archival) with per-listener error isolation, so one failing sink
doesn't block the others.

- `workflow-event-listener/.../composite/CompositeWorkflowStatusListener.java` — fans every workflow
  lifecycle callback out to all configured listeners in parallel, catching/logging exceptions per listener.
- `workflow-event-listener/.../composite/CompositeWorkflowStatusListenerConfiguration.java` — active when
  `conductor.workflow-status-listener.type=composite`; reads
  `conductor.workflow-status-listener.composite.types` (subset of `workflow_publisher`, `queue_publisher`,
  `kafka`, `archive`).
- `workflow-event-listener/.../statuschange/StatusChangePublisher.java` — the webhook (`workflow_publisher`)
  implementation. Only enqueues workflows whose terminal status is in
  `conductor.status-notifier.notification.subscribedWorkflowStatuses`. Publishing is async via an internal
  bounded queue (`ENV_WORKFLOW_NOTIFICATION_QUEUE_SIZE`, default 50) drained by a self-restarting consumer
  thread. Sets `payload_type=conductor_workflow_status`, `account_id` from `workflow.input.accountId`
  (falls back to `"-1"` with a warning if absent), and copies `_tenantContext` to the payload root.
- `task-status-listener/.../CentralPayloadUtils.java` — shared helper: `inlineJsonString(...)` un-escapes
  double-encoded `input`/`output` fields into real JSON; `exposeTenantContextAtRoot(...)` copies (not
  moves) `input._tenantContext` to the payload root, so both old and new consumers work.
- `task-status-listener/.../TaskStatusPublisher.java` — task-level analog
  (`payload_type=conductor_task_status`, enabled via `conductor.task-status-listener.type=task_publisher`).
  In practice only `SCHEDULED` fires automatically per `validateSubscribedTaskStatuses`; other statuses only
  fire when pushed via API. Explicitly skips `SUB_WORKFLOW` tasks. Falls back to a DB read of the parent
  `WorkflowModel` to find `_tenantContext` if it isn't present directly on the task input.
- `task-status-listener/.../RestClientManager.java` — the HTTP client (pooled Apache `HttpClient`, retry
  handling for transient IOExceptions/SSL and 503s). Generates a fresh UUID per POST and sets it as the
  `x-request-id` header for traceability.

Relevant `application.properties` keys (all disabled by default):

```properties
conductor.workflow-status-listener.type=workflow_publisher   # or: composite
conductor.task-status-listener.type=task_publisher
conductor.status-notifier.notification.url=
conductor.status-notifier.notification.endpointWorkflow=
conductor.status-notifier.notification.endpointTask=
conductor.status-notifier.notification.subscribedTaskStatuses=SCHEDULED
conductor.status-notifier.notification.subscribedWorkflowStatuses=
conductor.status-notifier.notification.headerPrefer=
conductor.status-notifier.notification.headerPreferValue=
conductor.status-notifier.notification.requestTimeoutMsConnect=100
conductor.status-notifier.notification.requestTimeoutMsRead=300
conductor.status-notifier.notification.requestTimeoutMsConnMgr=300
conductor.status-notifier.notification.requestRetryCount=3
conductor.status-notifier.notification.requestRetryIntervalMs=50
conductor.status-notifier.notification.connectionPoolMaxRequest=3
conductor.status-notifier.notification.connectionPoolMaxRequestPerRoute=3

# only when type=composite:
conductor.workflow-status-listener.composite.types=workflow_publisher,queue_publisher,kafka,archive
```

## RBAC

This is a **UI-only gate, not server-side authorization**. There is no enforcement in any `rest/`
controller — it only hides/shows UI affordances based on the tenant-scoped role returned by
`/api/userinfo`.

- `ui/src/components/RoleGate.jsx` — `RoleGate({ minRole, children })` renders `children` only if
  `canAccess(minRole)` is true.
- `ui/src/components/TenantContext.jsx` — defines `ROLE_LEVELS = { user: 0, Editor: 1, SuperAdmin: 2 }` and
  `canAccess(minRole)`. Role is per-tenant, so the same user can hold different roles in different tenants.
- Gated at `minRole="Editor"` in: `ui/src/App.jsx` (Workbench nav/route),
  `ui/src/pages/definitions/{Workflow,Task,EventHandler}.jsx` ("New ... Definition" buttons),
  `ui/src/pages/definition/{WorkflowDefinition,TaskDefinition,EventHandlerDefinition}.jsx` (save/edit
  actions), `ui/src/pages/execution/ActionModule.jsx` (pause/resume/retry/terminate actions).

**Known limitation:** since enforcement is client-side only, calling the underlying write endpoints
(`POST`/`PUT`/`DELETE` on `/metadata/*`, or execution action endpoints) directly bypasses the role check
entirely. Treat this as UI convenience, not an authorization boundary.

## Pyroscope continuous profiling

Starts the Pyroscope Java agent in-process at startup to continuously push CPU (itimer) profiles in JFR
format to a Pyroscope server, complementing the existing Prometheus metrics.

- `server/src/main/java/com/netflix/conductor/instrumentation/PyroscopeConfiguration.java` — active only
  when `conductor.pyroscope.enabled=true`. Adds a `host` label from the `POD_NAME` env var when present,
  useful for per-pod filtering in the Pyroscope UI/Grafana.

```properties
conductor.pyroscope.enabled=false
conductor.pyroscope.application-name=conductor-server
conductor.pyroscope.server-address=http://localhost:4040
# conductor.pyroscope.auth-token=
```

Enabling requires `conductor.pyroscope.enabled=true` plus a reachable `server-address`. Set `POD_NAME` via
the Kubernetes downward API for per-pod labeling.

## S3 prefix support (multi-tenant storage)

Adds an optional key-prefix namespace to the external S3 payload storage backend (used to offload large
workflow/task input/output payloads outside the primary DB), so multiple deployments/tenants can share one
S3 bucket without colliding on object keys.

- `awss3-storage/src/main/java/com/netflix/conductor/s3/config/S3Properties.java` — new field `prefix`
  (default `""`, e.g. `"sip/"`), prepended to every payload object key.
- `awss3-storage/src/main/java/com/netflix/conductor/s3/storage/S3PayloadStorage.java` —
  `normalizePrefix(String)` normalizes the value to `""` or a single segment with exactly one trailing
  slash. `getObjectKey(PayloadType)` prepends it before the existing `workflow/input/`, `workflow/output/`,
  `task/input/`, `task/output/` sub-paths, e.g. `sip/workflow/input/<uuid>.json`. Applies to both the
  presigned-URL path and direct upload/download.

Set via `conductor.external-payload-storage.s3.prefix` (not present in the default
`application.properties` — set per-deployment/tenant, e.g. via the env var override
`CONDUCTOR_EXTERNAL_PAYLOAD_STORAGE_S3_PREFIX`).

## Not tenant-related: Redis namespace prefixes

`redis-persistence`'s `RedisProperties` exposes `workflowNamespacePrefix` / `queueNamespacePrefix` and a
stack-based `getQueuePrefix()`, which is stock upstream Conductor/Dynomite behavior for separating
workflow/queue data within a shared Redis keyspace by stack/domain. It is conceptually similar to the S3
prefix above but is inherited from upstream, not Freshworks-specific, and is not wired to the tenant/
`ownerApp` concept used in multi-tenancy above.
