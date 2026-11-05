/*
 * Copyright 2026 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.pgmirror;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Builds, for each Conductor write operation, the Postgres statements that leave the Postgres
 * Conductor's tables exactly as {@code PostgresExecutionDAO} would have left them. One method per
 * operation, and every table that operation touches in Postgres is written:
 *
 * <pre>
 * createWorkflow / updateWorkflow  workflow, workflow_def_to_workflow, workflow_pending
 * createTasks / updateTask         task, workflow_to_task, task_scheduled, task_in_progress
 * removeTask                       task_scheduled, workflow_to_task, task_in_progress, task
 * removeWorkflow                   workflow_def_to_workflow, workflow, workflow_pending + removeTask for every task
 * removeFromPendingWorkflow        workflow_pending
 * add/update/removeEventExecution  event_execution
 * </pre>
 *
 * Statements are idempotent upserts of the <b>state</b> the models hold after the Scylla write, so
 * replaying a message, or applying messages in a different order, ends in the same rows (see {@link
 * PgMirrorMessage} for the guard). The JSON columns use the Conductor's own object mapper, as the
 * Postgres DAO does.
 */
public class PgMirrorSql {

    public static final int VERSION = 1;

    // ---------- workflow ----------
    // The guard compares the updatedTime inside the stored JSON with the incoming one. Equal is
    // accepted, so a repeated message is applied again (harmless) instead of skipped.
    static final String WORKFLOW_UPSERT =
            "INSERT INTO workflow (workflow_id, correlation_id, json_data) VALUES ($1, $2, $3) "
                    + "ON CONFLICT (workflow_id) DO UPDATE SET json_data = EXCLUDED.json_data, "
                    + "modified_on = CURRENT_TIMESTAMP "
                    + "WHERE COALESCE((workflow.json_data::jsonb ->> 'updatedTime')::bigint, 0) <= $4::bigint";
    static final String WORKFLOW_DEF_TO_WORKFLOW =
            "INSERT INTO workflow_def_to_workflow (workflow_def, date_str, workflow_id) "
                    + "VALUES ($1, $2, $3) ON CONFLICT (workflow_def, date_str, workflow_id) DO NOTHING";
    static final String PENDING_INSERT =
            "INSERT INTO workflow_pending (workflow_type, workflow_id) VALUES ($1, $2) "
                    + "ON CONFLICT (workflow_type, workflow_id) DO NOTHING";
    static final String PENDING_DELETE =
            "DELETE FROM workflow_pending WHERE workflow_type = $1 AND workflow_id = $2";
    static final String WORKFLOW_DELETE = "DELETE FROM workflow WHERE workflow_id = $1";
    static final String WORKFLOW_DEF_TO_WORKFLOW_DELETE =
            "DELETE FROM workflow_def_to_workflow WHERE workflow_def = $1 AND date_str = $2 AND workflow_id = $3";

    // ---------- task ----------
    static final String TASK_UPSERT =
            "INSERT INTO task (task_id, json_data, modified_on) VALUES ($1, $2, CURRENT_TIMESTAMP) "
                    + "ON CONFLICT (task_id) DO UPDATE SET json_data = EXCLUDED.json_data, "
                    + "modified_on = CURRENT_TIMESTAMP "
                    + "WHERE COALESCE((task.json_data::jsonb ->> 'updateTime')::bigint, 0) <= $3::bigint";
    static final String WORKFLOW_TO_TASK =
            "INSERT INTO workflow_to_task (workflow_id, task_id) VALUES ($1, $2) "
                    + "ON CONFLICT (workflow_id, task_id) DO NOTHING";
    static final String TASK_SCHEDULED =
            "INSERT INTO task_scheduled (workflow_id, task_key, task_id) VALUES ($1, $2, $3) "
                    + "ON CONFLICT (workflow_id, task_key) DO NOTHING";
    // Every open task has a row; in_progress_status is only maintained for tasks whose definition
    // has
    // a concurrency limit (as PostgresExecutionDAO.updateTask does).
    static final String IN_PROGRESS_INSERT =
            "INSERT INTO task_in_progress (task_def_name, task_id, workflow_id) VALUES ($1, $2, $3) "
                    + "ON CONFLICT (task_def_name, task_id) DO NOTHING";
    static final String IN_PROGRESS_UPSERT =
            "INSERT INTO task_in_progress (task_def_name, task_id, workflow_id, in_progress_status) "
                    + "VALUES ($1, $2, $3, $4) ON CONFLICT (task_def_name, task_id) DO UPDATE SET "
                    + "in_progress_status = EXCLUDED.in_progress_status, modified_on = CURRENT_TIMESTAMP";
    static final String IN_PROGRESS_DELETE =
            "DELETE FROM task_in_progress WHERE task_def_name = $1 AND task_id = $2";
    static final String TASK_SCHEDULED_DELETE =
            "DELETE FROM task_scheduled WHERE workflow_id = $1 AND task_key = $2";
    static final String WORKFLOW_TO_TASK_DELETE =
            "DELETE FROM workflow_to_task WHERE workflow_id = $1 AND task_id = $2";
    static final String TASK_DELETE = "DELETE FROM task WHERE task_id = $1";

    // ---------- event execution ----------
    static final String EVENT_INSERT =
            "INSERT INTO event_execution (event_handler_name, event_name, message_id, execution_id, json_data) "
                    + "VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING";
    static final String EVENT_UPSERT =
            "INSERT INTO event_execution (event_handler_name, event_name, message_id, execution_id, json_data) "
                    + "VALUES ($1, $2, $3, $4, $5) ON CONFLICT (event_handler_name, event_name, execution_id) "
                    + "DO UPDATE SET json_data = EXCLUDED.json_data, modified_on = CURRENT_TIMESTAMP";
    static final String EVENT_DELETE =
            "DELETE FROM event_execution WHERE event_handler_name = $1 AND event_name = $2 "
                    + "AND message_id = $3 AND execution_id = $4";

    private final ObjectMapper objectMapper;

    public PgMirrorSql(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // ================= workflow =================

    /**
     * createWorkflow and updateWorkflow: the workflow row without its tasks, and the two mappings.
     */
    public PgMirrorMessage workflow(String op, WorkflowModel wf) throws JsonProcessingException {
        List<PgStatement> s = new ArrayList<>();
        long updated = wf.getUpdatedTime() == null ? 0L : wf.getUpdatedTime();
        s.add(
                st(
                        WORKFLOW_UPSERT,
                        wf.getWorkflowId(),
                        wf.getCorrelationId(),
                        workflowJson(wf),
                        updated));
        s.add(
                st(
                        WORKFLOW_DEF_TO_WORKFLOW,
                        wf.getWorkflowName(),
                        dateStr(wf.getCreateTime()),
                        wf.getWorkflowId()));
        if (wf.getStatus().isTerminal()) {
            s.add(st(PENDING_DELETE, wf.getWorkflowName(), wf.getWorkflowId()));
        } else {
            s.add(st(PENDING_INSERT, wf.getWorkflowName(), wf.getWorkflowId()));
        }
        return msg(op, wf.getWorkflowId(), true, s);
    }

    /**
     * removeWorkflow, built from the workflow as it was <b>before</b> it was removed, with its
     * tasks.
     */
    public PgMirrorMessage removeWorkflow(WorkflowModel before) {
        List<PgStatement> s = new ArrayList<>();
        s.add(
                st(
                        WORKFLOW_DEF_TO_WORKFLOW_DELETE,
                        before.getWorkflowName(),
                        dateStr(before.getCreateTime()),
                        before.getWorkflowId()));
        s.add(st(WORKFLOW_DELETE, before.getWorkflowId()));
        s.add(st(PENDING_DELETE, before.getWorkflowName(), before.getWorkflowId()));
        for (TaskModel t : before.getTasks()) {
            s.addAll(removeTaskStatements(t));
        }
        return msg("removeWorkflow", before.getWorkflowId(), false, s);
    }

    public PgMirrorMessage removeFromPending(String workflowType, String workflowId) {
        return msg(
                "removeFromPendingWorkflow",
                workflowId,
                false,
                List.of(st(PENDING_DELETE, workflowType, workflowId)));
    }

    // ================= tasks =================

    /** One message per task: createTasks and updateTask write the same rows. */
    public PgMirrorMessage task(String op, TaskModel t) throws JsonProcessingException {
        List<PgStatement> s = new ArrayList<>();
        s.add(
                st(
                        TASK_UPSERT,
                        t.getTaskId(),
                        objectMapper.writeValueAsString(t),
                        t.getUpdateTime()));
        s.add(st(WORKFLOW_TO_TASK, t.getWorkflowInstanceId(), t.getTaskId()));
        s.add(st(TASK_SCHEDULED, t.getWorkflowInstanceId(), taskKey(t), t.getTaskId()));
        if (t.getStatus() != null && t.getStatus().isTerminal()) {
            s.add(st(IN_PROGRESS_DELETE, t.getTaskDefName(), t.getTaskId()));
        } else if (concurrencyLimit(t) > 0) {
            s.add(
                    st(
                            IN_PROGRESS_UPSERT,
                            t.getTaskDefName(),
                            t.getTaskId(),
                            t.getWorkflowInstanceId(),
                            t.getStatus() == TaskModel.Status.IN_PROGRESS));
        } else {
            s.add(
                    st(
                            IN_PROGRESS_INSERT,
                            t.getTaskDefName(),
                            t.getTaskId(),
                            t.getWorkflowInstanceId()));
        }
        return msg(op, t.getWorkflowInstanceId(), true, s);
    }

    /** removeTask, built from the task as it was <b>before</b> it was removed. */
    public PgMirrorMessage removeTask(TaskModel before) {
        return msg(
                "removeTask", before.getWorkflowInstanceId(), false, removeTaskStatements(before));
    }

    private List<PgStatement> removeTaskStatements(TaskModel t) {
        List<PgStatement> s = new ArrayList<>();
        s.add(st(TASK_SCHEDULED_DELETE, t.getWorkflowInstanceId(), taskKey(t)));
        s.add(st(WORKFLOW_TO_TASK_DELETE, t.getWorkflowInstanceId(), t.getTaskId()));
        s.add(st(IN_PROGRESS_DELETE, t.getTaskDefName(), t.getTaskId()));
        s.add(st(TASK_DELETE, t.getTaskId()));
        return s;
    }

    // ================= event executions =================

    public PgMirrorMessage addEventExecution(EventExecution e) throws JsonProcessingException {
        return msg("addEventExecution", null, false, List.of(eventStatement(EVENT_INSERT, e)));
    }

    public PgMirrorMessage updateEventExecution(EventExecution e) throws JsonProcessingException {
        return msg("updateEventExecution", null, false, List.of(eventStatement(EVENT_UPSERT, e)));
    }

    public PgMirrorMessage removeEventExecution(EventExecution e) {
        return msg(
                "removeEventExecution",
                null,
                false,
                List.of(st(EVENT_DELETE, e.getName(), e.getEvent(), e.getMessageId(), e.getId())));
    }

    private PgStatement eventStatement(String sql, EventExecution e)
            throws JsonProcessingException {
        return st(
                sql,
                e.getName(),
                e.getEvent(),
                e.getMessageId(),
                e.getId(),
                objectMapper.writeValueAsString(e));
    }

    // ================= helpers (the same rules as PostgresExecutionDAO) =================

    /** The workflow JSON the Postgres DAO stores: the workflow with an empty task list. */
    private String workflowJson(WorkflowModel wf) throws JsonProcessingException {
        List<TaskModel> tasks = wf.getTasks();
        wf.setTasks(new LinkedList<>());
        try {
            return objectMapper.writeValueAsString(wf);
        } finally {
            wf.setTasks(tasks);
        }
    }

    static String taskKey(TaskModel task) {
        return task.getReferenceTaskName() + "_" + task.getRetryCount();
    }

    /** yyyyMMdd in the JVM default time zone, like the Postgres DAO. */
    static String dateStr(Long timeInMs) {
        return new SimpleDateFormat("yyyyMMdd").format(new Date(timeInMs == null ? 0L : timeInMs));
    }

    private static int concurrencyLimit(TaskModel t) {
        return t.getTaskDefinition().map(TaskDef::concurrencyLimit).orElse(0);
    }

    private static PgStatement st(String sql, Object... args) {
        return new PgStatement(sql, java.util.Arrays.asList(args));
    }

    private static PgMirrorMessage msg(
            String op, String workflowId, boolean gated, List<PgStatement> statements) {
        return new PgMirrorMessage(
                VERSION,
                UUID.randomUUID().toString(),
                op,
                workflowId,
                System.currentTimeMillis(),
                gated,
                statements);
    }
}
