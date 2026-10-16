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
package com.netflix.conductor.instrumentation;

import java.util.function.Supplier;

import com.netflix.conductor.model.TaskModel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

/** Custom OpenTelemetry spans for Conductor system task execution. */
public final class SystemTaskTracing {

    private static final String TRACER_NAME = "conductor-server-system-task";
    private static final String SWITCH_EXECUTE_SPAN = "system-task-execute-switch";

    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer(TRACER_NAME);

    private SystemTaskTracing() {}

    public static boolean traceSwitchExecute(TaskModel task, Supplier<Boolean> action) {
        return traceSystemTaskExecute(SWITCH_EXECUTE_SPAN, task, action);
    }

    private static <T> T traceSystemTaskExecute(
            String spanName, TaskModel task, Supplier<T> action) {
        Span span =
                TRACER.spanBuilder(spanName)
                        .setAttribute(TraceAttributes.TASK_ID, task.getTaskId())
                        .setAttribute(TraceAttributes.TASK_TYPE, task.getTaskType())
                        .startSpan();
        try (Scope scope = span.makeCurrent()) {
            return action.get();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}
