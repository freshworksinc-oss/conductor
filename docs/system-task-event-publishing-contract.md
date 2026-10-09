# System Task Event Publishing — Tenant Guide

**Owner:** CEApps  
**Audience:** Tenant workflow teams

This document describes how tenants can publish **system task** events (HUMAN, HTTP, WAIT, etc.) to Central with the same payload shape as native SIMPLE task notifications.

CEApps provides the shared notification pipeline. Tenants wire their workflows into it by following the conventions below.

---

## Overview

| | CEApps provides | Tenant provides |
|---|----------------|-----------------|
| Notification workflow | `system_task_notification_workflow` | — |
| Central delivery | POST to `/collector` | — |
| Task fetch + envelope build | Internal Conductor API + JQ | — |
| Parent workflow | — | FORK + `system_task` + EVENT |
| Event handler | — | One per workflow name |
| Workflow input | — | `accountId`, `_tenantContext` |

No Conductor core changes are required.

---

## What tenants receive

When a system task reaches `IN_PROGRESS`, Central receives:

```json
{
  "account_id": "<account>",
  "payload_type": "conductor_task_status",
  "payload_version": "1.0",
  "payload": {
    "workflowId": "...",
    "workflowType": "...",
    "taskId": "...",
    "taskType": "HUMAN",
    "status": "IN_PROGRESS",
    "referenceTaskName": "system_task",
    "input": { ... },
    "_tenantContext": { ... }
  }
}
```

This matches the envelope produced by `TaskStatusPublisher` for SIMPLE tasks.

---

## How it works

```
Tenant workflow                         CEApps (shared)
───────────────                         ───────────────
system_task (HUMAN / HTTP / …)  ─┐
                                  ├─► EVENT ─► Event handler ─► Notification WF ─► Central
notify_ref (EVENT)              ─┘
```

---

## Tenant setup

### Step 1 — Naming conventions (required)

| Item | Value | Notes |
|------|--------|--------|
| System task ref | `system_task` | Fixed — CEApps reads this ref |
| EVENT task ref | `notify_ref` | Publishes the notification event |
| EVENT sink | `conductor:system_task_notify` | Conductor prefixes with your workflow name |

Resolved event queue your handler must listen on:

```
conductor:<YourWorkflowName>:system_task_notify
```

Example: `conductor:OrderApprovalWorkflow:system_task_notify`

### Step 2 — Workflow input

Start the workflow with `accountId` and `_tenantContext`:

```json
{
  "accountId": "<account-id>",
  "_tenantContext": {
    "accountID": "<account-id>",
    "tenantID": "<tenant-id>"
  }
}
```

`accountId` becomes `account_id` in the Central envelope. If omitted, CEApps falls back to `_tenantContext.accountID`.

### Step 3 — Add FORK + system task + EVENT

Add this pattern to your workflow definition:

```json
{
  "inputParameters": ["accountId", "_tenantContext"],
  "tasks": [
    {
      "name": "fork_system_task_and_notify",
      "taskReferenceName": "fork_system_task_and_notify",
      "type": "FORK_JOIN",
      "forkTasks": [
        [
          {
            "name": "system_task",
            "taskReferenceName": "system_task",
            "type": "HUMAN",
            "inputParameters": {
              "accountId": "${workflow.input.accountId}",
              "_tenantContext": "${workflow.input._tenantContext}"
            }
          }
        ],
        [
          {
            "name": "notify_ref",
            "taskReferenceName": "notify_ref",
            "type": "EVENT",
            "sink": "conductor:system_task_notify",
            "inputParameters": {
              "accountId": "${workflow.input.accountId}",
              "taskRefName": "system_task"
            }
          }
        ]
      ]
    },
    {
      "name": "join",
      "taskReferenceName": "join",
      "type": "JOIN",
      "joinOn": ["system_task", "notify_ref"]
    }
  ]
}
```

Replace `HUMAN` with your system task type (`HTTP`, `WAIT`, etc.). Do **not** change ref names `system_task` or `notify_ref`.

### Step 4 — EVENT payload rules

Publish **pointers only** in the EVENT task:

| Field | Value |
|-------|--------|
| `accountId` | `${workflow.input.accountId}` |
| `taskRefName` | `"system_task"` (literal string) |

**Do not** use `${system_task.taskId}` or any `${system_task.*}` expression in the EVENT task. In a FORK, sibling task fields are unavailable at schedule time and resolve to `null`.

CEApps fetches the full task via Conductor API using `workflowInstanceId` + `taskRefName`.

### Step 5 — Register an event handler (per workflow name)

Register once per **workflow name** (not per version):

```json
{
  "name": "<your_workflow_name>_system_task_notification_handler",
  "event": "conductor:<YourWorkflowName>:system_task_notify",
  "condition": "$.workflowInstanceId != null && $.taskRefName == 'system_task'",
  "actions": [
    {
      "action": "start_workflow",
      "start_workflow": {
        "name": "system_task_notification_workflow",
        "version": 1,
        "correlationId": "${workflowInstanceId}",
        "input": {
          "accountId": "${accountId}",
          "workflowId": "${workflowInstanceId}",
          "taskRefName": "${taskRefName}"
        }
      }
    }
  ],
  "active": true
}
```

Register via `POST /api/event`. Contact CEApps if the notification workflow version differs in your environment.

---

## CEApps-provided components

Tenants do **not** need to build or maintain these:

| Component | Name |
|-----------|------|
| Notification workflow | `system_task_notification_workflow` |
| Parent workflow fetch | `GET /api/workflow/{id}?includeTasks=true` |
| Central envelope | Built via `JSON_JQ_TRANSFORM` |
| Central endpoint | `http://staging.central.us-east-1.edge/collector` |

---

## Central payload contract

| Field | Type | Required | Source |
|-------|------|----------|--------|
| `account_id` | string | yes | Workflow `accountId` or `_tenantContext` |
| `payload_type` | string | yes | `conductor_task_status` |
| `payload_version` | string | yes | `1.0` |
| `payload.workflowId` | string | yes | Parent workflow instance |
| `payload.workflowType` | string | yes | Parent workflow name |
| `payload.taskId` | string | yes | Fetched task |
| `payload.taskType` | string | yes | e.g. `HUMAN`, `HTTP` |
| `payload.status` | string | yes | e.g. `IN_PROGRESS` |
| `payload.referenceTaskName` | string | yes | `system_task` |
| `payload.input` | object | yes | Task input data |
| `payload._tenantContext` | object | no | From task input when present |

---

## Go-live checklist

- [ ] Workflow uses ref `system_task` for the task to notify
- [ ] FORK includes EVENT with sink `conductor:system_task_notify`
- [ ] EVENT publishes `accountId` + `taskRefName` only (no `${system_task.*}`)
- [ ] Workflow started with `accountId` and `_tenantContext`
- [ ] Event handler registered for `conductor:<YourWorkflowName>:system_task_notify`
- [ ] Test run: `event_execution` shows `COMPLETED` with `output.workflowId`
- [ ] Central receives full task payload

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Handler `SKIPPED` | Condition failed or `taskRefName` mismatch | Verify EVENT output has `taskRefName: "system_task"` |
| No `workflowId` in `event_execution` | Handler skipped or failed | See above |
| `taskId: null` in Central payload | Wrong ref name or fetch failed | Use `system_task`; verify workflow fetch returns the task |
| Central `400` missing fields | Malformed envelope | Contact CEApps |
| `account_id` is `default` | `accountId` not passed at start | Pass `accountId` in workflow input |

---

## API reference

| Action | Endpoint |
|--------|----------|
| Register event handler | `POST /api/event` |
| Register workflow | `POST /api/metadata/workflow` |
| Start workflow | `POST /api/workflow/<YourWorkflowName>` |
| List event handlers | `GET /api/event` |

---

## Example: start a workflow

```bash
curl -X POST "$CONDUCTOR_URL/api/workflow/OrderApprovalWorkflow" \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "namespace-payments",
    "_tenantContext": {
      "accountID": "namespace-payments",
      "tenantID": "namespace-payments"
    }
  }'
```

---

## Support

For onboarding, sandbox enablement, or issues with the shared notification workflow, contact **CEApps**.
