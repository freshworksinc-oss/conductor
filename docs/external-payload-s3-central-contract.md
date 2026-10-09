# External Payload Storage (S3) — Tenant Contract

**Owner:** CEApps  
**Audience:** Tenant workflow teams consuming Conductor workflow/task events from Central

This document describes how **large workflow and task payloads** are stored in S3, what appears in Conductor API responses, and what tenants receive in **Central** events (`conductor_workflow_status`, `conductor_task_status`).

CEApps operates the shared S3 bucket and Conductor configuration. Tenants do not upload to S3 directly unless using the optional client pre-upload flow.

---

## Overview

| | CEApps provides | Tenant provides |
|---|----------------|-----------------|
| S3 bucket & Conductor S3 integration | Yes | — |
| Automatic externalization above size threshold | Yes | — |
| Central event delivery (`/collector`) | Yes | — |
| `external*PayloadStoragePath` in Central payloads | Yes | — |
| Full payload bytes in Central events | **No** | Fetch from S3 when needed |
| `accountId` / `_tenantContext` in workflow input | — | **Required** for correct `account_id` |
| Workflow definitions & task workers | — | Yes |

When a payload exceeds the configured threshold, Conductor:

1. Uploads the JSON to S3
2. Clears inline `input` / `output` in the execution record
3. Stores the **relative object key** in `externalInputPayloadStoragePath` or `externalOutputPayloadStoragePath`
4. Publishes that key (not the blob) to Central

---

## Size thresholds

Externalization is **automatic** based on serialized JSON size. No special keys are required inside `input` or `output`.

| Property | Typical sandbox value | Meaning |
|----------|----------------------|---------|
| `conductor.app.workflowInputPayloadSizeThreshold` | `1` KB | Workflow input above this → S3 |
| `conductor.app.workflowOutputPayloadSizeThreshold` | `1` KB | Workflow output above this → S3 |
| `conductor.app.taskInputPayloadSizeThreshold` | `1` KB | Task input above this → S3 |
| `conductor.app.taskOutputPayloadSizeThreshold` | `1` KB | Task output above this → S3 |

Values are in **kilobytes**. A payload is externalized when:

```text
serialized JSON size > threshold × 1024 bytes
```

**Max limits** (separate): payloads above `max*PayloadSizeThreshold` (default ~10 MB) are rejected and the workflow/task fails.

---

## When each payload is externalized

Externalization runs on **persist** (create/update), not on Central publish.

| Payload | When evaluated | Example |
|---------|----------------|---------|
| **Workflow input** | Workflow start (`createWorkflow`) | Large `largeBlob` in start `input` |
| **Workflow output** | Workflow terminal update (`updateWorkflow` on `COMPLETED`, etc.) | `outputParameters` resolved at completion |
| **Task input** | Task create (`createTasks`) | Large resolved `inputData` for a SIMPLE task |
| **Task output** | Task update (`updateTask` on completion) | Large worker/human `outputData` |

**Important:** Workflow input being in S3 does **not** externalize task input automatically. Each entity is checked independently. Task `inputParameters` that only map small fields (e.g. `orderId`, `amount`) will stay inline even when workflow input is in S3.

---

## S3 object key layout

Conductor stores **relative keys**, not `s3://` URLs. CEApps maps keys to the shared bucket (e.g. `ceapps-conductor-sandbox-payloads`).

| Payload type | Key pattern | Example |
|--------------|-------------|---------|
| Workflow input | `workflow/input/<uuid>.json` | `workflow/input/76875b6d-22fe-4269-b79c-f471254380fb.json` |
| Workflow output | `workflow/output/<uuid>.json` | `workflow/output/9fa16687-a40f-4094-b63c-5db2e5c22fc9.json` |
| Task input | `task/input/<uuid>.json` | `task/input/<uuid>.json` |
| Task output | `task/output/<uuid>.json` | `task/output/<uuid>.json` |

Optional bucket prefix may be configured platform-side (`conductor.external-payload-storage.s3.prefix`).

**Tenant fetch example:**

```bash
aws s3 cp s3://<bucket>/workflow/input/<uuid>.json -
```

Tenants need IAM/read access to the shared payloads bucket (or a CEApps-provided proxy API) to hydrate full content.

---

## Conductor API representation

`GET /api/workflow/{workflowId}?includeTasks=true` does **not** re-download S3 by default. Externalized executions show:

```json
{
  "status": "COMPLETED",
  "input": {},
  "output": {},
  "externalInputPayloadStoragePath": "workflow/input/76875b6d-22fe-4269-b79c-f471254380fb.json",
  "externalOutputPayloadStoragePath": "workflow/output/9fa16687-a40f-4094-b63c-5db2e5c22fc9.json"
}
```

| Field | Meaning |
|-------|---------|
| `input: {}` / `output: {}` | Inline payload cleared after externalization |
| `externalInputPayloadStoragePath` | S3 key for workflow input JSON |
| `externalOutputPayloadStoragePath` | S3 key for workflow output JSON (present after completion if output > threshold) |

Tasks follow the same pattern with `inputData` / `outputData` and `externalInputPayloadStoragePath` / `externalOutputPayloadStoragePath` when externalized.

---

## Central event contract

### Envelope (all workflow status events)

```json
{
  "account_id": "<string>",
  "payload_type": "conductor_workflow_status",
  "payload_version": "1.0",
  "payload": { ... }
}
```

### Workflow `RUNNING` (start)

Published when the workflow starts. Output is not built yet.

```json
{
  "account_id": "<account-id>",
  "payload_type": "conductor_workflow_status",
  "payload_version": "1.0",
  "payload": {
    "workflowId": "78b636ae-5f2a-49e8-bc3a-156c1c5e2720",
    "workflowType": "simple_with_human_task_wf",
    "version": 3,
    "status": "RUNNING",
    "correlationId": "namespace-payments: ",
    "input": "{}",
    "inputSize": 2,
    "output": "{}",
    "outputSize": 2,
    "externalInputPayloadStoragePath": "workflow/input/76875b6d-22fe-4269-b79c-f471254380fb.json",
    "externalOutputPayloadStoragePath": null,
    "startTime": "2026-09-09T07:13:45.774Z",
    "taskToDomain": { "*": "namespace-payments" }
  }
}
```

| Field | RUNNING | Notes |
|-------|---------|-------|
| `input` | `"{}"` | Full input is **not** inlined; use S3 path |
| `externalInputPayloadStoragePath` | Set if input was externalized | Relative S3 key |
| `externalOutputPayloadStoragePath` | `null` | Output not computed yet |
| `output` | `"{}"` | Always empty at start |

### Workflow `COMPLETED`

Published when the workflow finishes. Output is built from `outputParameters` then externalized if large enough.

```json
{
  "account_id": "<account-id>",
  "payload_type": "conductor_workflow_status",
  "payload_version": "1.0",
  "payload": {
    "workflowId": "78b636ae-5f2a-49e8-bc3a-156c1c5e2720",
    "workflowType": "simple_with_human_task_wf",
    "version": 3,
    "status": "COMPLETED",
    "correlationId": "namespace-payments: ",
    "input": "{}",
    "inputSize": 2,
    "output": "{}",
    "outputSize": 2,
    "externalInputPayloadStoragePath": "workflow/input/76875b6d-22fe-4269-b79c-f471254380fb.json",
    "externalOutputPayloadStoragePath": "workflow/output/9fa16687-a40f-4094-b63c-5db2e5c22fc9.json",
    "startTime": "2026-09-09T07:13:45.774Z",
    "endTime": "2026-09-09T07:16:33.048Z",
    "executionTime": 167274,
    "taskToDomain": { "*": "namespace-payments" }
  }
}
```

| Field | COMPLETED | Notes |
|-------|-----------|-------|
| `externalOutputPayloadStoragePath` | Set if output > threshold | Appears only after completion |
| `input` / `output` | `"{}"` when externalized | Stringified JSON; size 2 = empty object `{}` |

### Task events (`conductor_task_status`)

Same rules apply per task:

```json
{
  "account_id": "<account-id>",
  "payload_type": "conductor_task_status",
  "payload_version": "1.0",
  "payload": {
    "workflowId": "...",
    "taskId": "...",
    "taskType": "process_order",
    "status": "SCHEDULED",
    "input": "{}",
    "output": "{}",
    "externalInputPayloadStoragePath": "task/input/<uuid>.json",
    "externalOutputPayloadStoragePath": null
  }
}
```

- If task input/output was **not** externalized (under threshold), Central receives **full** `input` / `output` strings as today.
- If externalized, `input` / `output` are empty and the corresponding `external*PayloadStoragePath` is set.

---

## What Central does **not** include

| Not sent | Where to get it |
|----------|-----------------|
| Full workflow input JSON | S3: `externalInputPayloadStoragePath` |
| Full workflow output JSON | S3: `externalOutputPayloadStoragePath` |
| Full task input/output JSON | S3: task `external*PayloadStoragePath` |
| `s3://` bucket URL | Platform docs / bucket config |
| Presigned download URL | Tenants use bucket IAM or CEApps fetch API |

Central events are intentionally **small references** plus metadata. Consumers that need the full blob must fetch from S3 using the path.

---

## Tenant workflow input requirements

### Required for Central routing

Include `accountId` in workflow start `input` (top-level, not only in `_tenantContext`):

```json
{
  "input": {
    "accountId": "2140003010",
    "_tenantContext": {
      "accountID": "2140003010"
    },
    "orderId": "ORD-004",
    "amount": 99.99,
    "largeBlob": "<string longer than 1 KB if testing externalization>"
  }
}
```

| If missing | Central `account_id` |
|------------|----------------------|
| `accountId` absent | Falls back to `"-1"` |

### Designing for external output

Workflow output is built at **completion** from `outputParameters`. To externalize output:

1. Add a large field to `outputParameters`, e.g. `"largeOutput": "${workflow.input.largeBlob}"`
2. Ensure the workflow reaches `COMPLETED`
3. Resolved output map must exceed the threshold when serialized

Example `outputParameters`:

```json
{
  "orderId": "${workflow.input.orderId}",
  "processedBy": "${process_order_ref.output.workerId}",
  "approvedBy": "${human_approval_ref.output.approvedBy}",
  "largeOutput": "${workflow.input.largeBlob}"
}
```

---

## Consumer decision tree

```
Central event received
        │
        ├─ payload_type = conductor_workflow_status
        │       │
        │       ├─ externalInputPayloadStoragePath set?
        │       │     YES → fetch workflow/input/*.json from S3 for full input
        │       │     NO  → parse payload.input string
        │       │
        │       └─ externalOutputPayloadStoragePath set? (usually on COMPLETED)
        │             YES → fetch workflow/output/*.json from S3 for full output
        │             NO  → parse payload.output string
        │
        └─ payload_type = conductor_task_status
                │
                ├─ externalInputPayloadStoragePath set?
                │     YES → fetch task/input/*.json from S3
                │     NO  → parse payload.input string
                │
                └─ externalOutputPayloadStoragePath set?
                      YES → fetch task/output/*.json from S3
                      NO  → parse payload.output string
```

---

## S3 JSON shape

Objects are **JSON maps** (same shape as Conductor `input` / `output`):

**Workflow input object** (`workflow/input/*.json`):

```json
{
  "accountId": "2140003010",
  "_tenantContext": { "accountID": "2140003010" },
  "orderId": "ORD-004",
  "amount": 99.99,
  "largeBlob": "... >1KB ..."
}
```

**Workflow output object** (`workflow/output/*.json`):

```json
{
  "orderId": "ORD-004",
  "processedBy": null,
  "approvedBy": null,
  "largeOutput": "... >1KB ..."
}
```

Null fields may appear when `outputParameters` reference missing task output keys.

---

## Status lifecycle vs external paths

| Workflow status | `externalInputPayloadStoragePath` | `externalOutputPayloadStoragePath` |
|-----------------|-----------------------------------|-------------------------------------|
| `RUNNING` | Set if input externalized at start | `null` |
| `COMPLETED` | Unchanged from start | Set if output externalized at completion |
| `FAILED` / `TERMINATED` | May be set | Set only if output was computed and large |

---

## Tenant checklist

- [ ] Include `accountId` (and `_tenantContext`) in workflow start input
- [ ] Expect `input` / `output` in Central to be `"{}"` when external paths are present
- [ ] Use `externalInputPayloadStoragePath` / `externalOutputPayloadStoragePath` to fetch full JSON from S3
- [ ] Do not assume task events inherit workflow S3 paths — check task-level paths separately
- [ ] Keep task-mapped fields small unless task-level externalization is intended
- [ ] Confirm bucket read access with CEApps for your environment (sandbox vs prod)

---

## Related configuration (platform / CEApps)

Tenants do not set these; documented for awareness:

```properties
conductor.external-payload-storage.type=s3
conductor.external-payload-storage.s3.bucket-name=<shared-bucket>
conductor.external-payload-storage.s3.region=us-east-1

conductor.app.workflowInputPayloadSizeThreshold=1
conductor.app.workflowOutputPayloadSizeThreshold=1
conductor.app.taskInputPayloadSizeThreshold=1
conductor.app.taskOutputPayloadSizeThreshold=1

conductor.app.summary-input-output-json-serialization.enabled=true
```

---

## Summary

| Question | Answer |
|----------|--------|
| Does Central get the full large payload? | **No** — only metadata + S3 **relative key** |
| Does `input: "{}"` mean data is lost? | **No** — data is in S3 at `externalInputPayloadStoragePath` |
| When does output path appear? | On **COMPLETED** (or other terminal update with output), if output > threshold |
| Are task events affected? | Only if **task** input/output exceeds threshold |
| What must tenants implement? | S3 fetch (or CEApps proxy) using path fields when `input`/`output` are empty |
