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

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.metrics.Monitors;

/**
 * Mirrors the writes of the metadata DAO (task and workflow definitions) and of the event handler
 * DAO. Like the execution interceptor it runs the real call first and only publishes when that
 * succeeded; nothing the mirror does can change the result or throw.
 */
public class PgMirrorMetadataDaoInterceptor implements MethodInterceptor {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PgMirrorMetadataDaoInterceptor.class);
    private static final String CLASS_NAME = "PgMirror";

    private final PgMirrorSql sql;
    private final PgMirrorPublisher publisher;

    public PgMirrorMetadataDaoInterceptor(PgMirrorSql sql, PgMirrorPublisher publisher) {
        this.sql = sql;
        this.publisher = publisher;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        String name = invocation.getMethod().getName();
        Object[] args = invocation.getArguments();
        if (!isMirrored(name)) {
            return invocation.proceed();
        }
        Object result = invocation.proceed(); // the real write; its exceptions pass through
        try {
            publisher.publish(java.util.List.of(message(name, args)));
        } catch (Throwable t) {
            Monitors.error(CLASS_NAME, name);
            LOGGER.warn(
                    "pg-mirror: could not mirror {} (the change is not mirrored; reconciliation will repair it)",
                    name,
                    t);
        }
        return result;
    }

    static boolean isMirrored(String name) {
        switch (name) {
            case "createTaskDef":
            case "updateTaskDef":
            case "removeTaskDef":
            case "createWorkflowDef":
            case "updateWorkflowDef":
            case "removeWorkflowDef":
            case "addEventHandler":
            case "updateEventHandler":
            case "removeEventHandler":
                return true;
            default:
                return false;
        }
    }

    private PgMirrorMessage message(String name, Object[] args) throws Exception {
        switch (name) {
            case "createTaskDef":
            case "updateTaskDef":
                return sql.taskDef(name, (TaskDef) args[0]);
            case "removeTaskDef":
                return sql.removeTaskDef((String) args[0]);
            case "createWorkflowDef":
            case "updateWorkflowDef":
                return sql.workflowDef(name, (WorkflowDef) args[0]);
            case "removeWorkflowDef":
                return sql.removeWorkflowDef((String) args[0], (Integer) args[1]);
            case "addEventHandler":
            case "updateEventHandler":
                return sql.eventHandler(name, (EventHandler) args[0]);
            default:
                return sql.removeEventHandler((String) args[0]);
        }
    }
}
