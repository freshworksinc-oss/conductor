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
package com.netflix.conductor.tracing;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/** Wraps async workflow execution paths with optional manual tracing spans. */
@Component
public class WorkflowExecutionTracing {

    private final TracingFacade tracing;

    public WorkflowExecutionTracing(TracingFacade tracing) {
        this.tracing = tracing;
    }

    public void injectTraceContext(Map<String, Object> workflowVariables) {
        if (!tracing.isEnabled() || workflowVariables == null) {
            return;
        }
        tracing.injectCurrentContext(workflowVariables, TracingFacade.TRACE_CONTEXT_VARIABLE_KEY);
    }

    public void decide(WorkflowModel workflow, Runnable action) {
        if (!tracing.isEnabled()) {
            action.run();
            return;
        }
        tracing.runWithStoredContext(
                workflow.getVariables(),
                TracingFacade.TRACE_CONTEXT_VARIABLE_KEY,
                "workflow.decide",
                () -> {
                    tagWorkflow(tracing, workflow);
                    action.run();
                    tagTerminalWorkflowStatus(tracing, workflow);
                    return null;
                });
    }

    public void enqueueDecider(WorkflowModel workflow, Runnable enqueue) {
        if (!tracing.isEnabled()) {
            enqueue.run();
            return;
        }
        tracing.run(
                "workflow.enqueue_decider",
                () -> {
                    tagWorkflow(tracing, workflow);
                    enqueue.run();
                });
    }

    public void enqueueTask(TaskModel task, Runnable enqueue) {
        if (!tracing.isEnabled()) {
            enqueue.run();
            return;
        }
        tracing.run(
                "task.enqueue",
                () -> {
                    tagTask(tracing, task);
                    enqueue.run();
                });
    }

    public void executeSystemTask(
            WorkflowModel workflow,
            TaskModel task,
            WorkflowSystemTask systemTask,
            Runnable execution) {
        if (!tracing.isEnabled()) {
            execution.run();
            return;
        }
        tracing.runWithStoredContext(
                workflow.getVariables(),
                TracingFacade.TRACE_CONTEXT_VARIABLE_KEY,
                "system_task.execute",
                () -> {
                    tagWorkflow(tracing, workflow);
                    tagTask(tracing, task);
                    tracing.tag("queue.name", QueueUtils.getQueueName(task));
                    if (systemTask != null) {
                        tracing.tag("system.task.type", systemTask.getTaskType());
                    }
                    execution.run();
                    return null;
                });
    }

    private static void tagWorkflow(TracingFacade tracing, WorkflowModel workflow) {
        if (workflow == null) {
            return;
        }
        tagIfPresent(tracing, "workflow.id", workflow.getWorkflowId());
        tagIfPresent(tracing, "workflow.name", workflow.getWorkflowName());
        tracing.tag("workflow.version", String.valueOf(workflow.getWorkflowVersion()));
        tagIfPresent(tracing, "correlation.id", workflow.getCorrelationId());
    }

    private static void tagTask(TracingFacade tracing, TaskModel task) {
        if (task == null) {
            return;
        }
        tagIfPresent(tracing, "task.id", task.getTaskId());
        tagIfPresent(tracing, "task.type", task.getTaskType());
        tagIfPresent(tracing, "workflow.id", task.getWorkflowInstanceId());
    }

    private static void tagTerminalWorkflowStatus(TracingFacade tracing, WorkflowModel workflow) {
        if (workflow == null || workflow.getStatus() == null) {
            return;
        }
        if (workflow.getStatus().isTerminal()) {
            tracing.tag("workflow.status", workflow.getStatus().name());
        }
    }

    private static void tagIfPresent(TracingFacade tracing, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            tracing.tag(key, value);
        }
    }
}
