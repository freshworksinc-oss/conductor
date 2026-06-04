/*
 * Copyright 2022 Conductor Authors.
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
package com.netflix.conductor.core.execution.tasks;

import org.springframework.stereotype.Component;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_SWITCH;

/** {@link Switch} task is a replacement for now deprecated {@link Decision} task. */
@Component(TASK_TYPE_SWITCH)
public class Switch extends WorkflowSystemTask {

    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("conductor-server-system-task");

    public Switch() {
        super(TASK_TYPE_SWITCH);
    }

    @Override
    public boolean execute(
            WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        Span span = tracer.spanBuilder("system-task-execute-switch")
                .setAttribute("taskId", task.getTaskId()).startSpan();
        try (Scope scope = span.makeCurrent()) {
            task.setStatus(TaskModel.Status.COMPLETED);
            return true;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}