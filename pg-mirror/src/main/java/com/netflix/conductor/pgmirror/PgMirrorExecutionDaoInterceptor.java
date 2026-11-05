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

import java.util.ArrayList;
import java.util.List;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * Runs the real DAO call first, untouched, and only if it succeeds builds and publishes the
 * Postgres statements for it. Nothing the mirror does can change the result or throw: any error
 * while building or publishing is logged and counted, and the caller gets the DAO's own result.
 *
 * <p>For removeTask and removeWorkflow the statements need the rows as they were, so those are read
 * before the call (a read, nothing else).
 */
public class PgMirrorExecutionDaoInterceptor implements MethodInterceptor {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PgMirrorExecutionDaoInterceptor.class);
    private static final String CLASS_NAME = "PgMirror";

    private final ExecutionDAO dao;
    private final PgMirrorSql sql;
    private final PgMirrorPublisher publisher;

    public PgMirrorExecutionDaoInterceptor(
            ExecutionDAO dao, PgMirrorSql sql, PgMirrorPublisher publisher) {
        this.dao = dao;
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

        Object before = null;
        try {
            if ("removeTask".equals(name)) {
                before = dao.getTask((String) args[0]);
            } else if ("removeWorkflow".equals(name)) {
                before = dao.getWorkflow((String) args[0], true);
            }
        } catch (Throwable t) {
            fail(name, t);
        }

        Object result =
                invocation.proceed(); // the real write; its exceptions pass through untouched

        try {
            publisher.publish(messages(name, args, result, before));
        } catch (Throwable t) {
            fail(name, t);
        }
        return result;
    }

    static boolean isMirrored(String name) {
        switch (name) {
            case "createWorkflow":
            case "updateWorkflow":
            case "createTasks":
            case "updateTask":
            case "removeTask":
            case "removeWorkflow":
            case "removeFromPendingWorkflow":
            case "addEventExecution":
            case "updateEventExecution":
            case "removeEventExecution":
                return true;
            default:
                return false;
        }
    }

    @SuppressWarnings("unchecked")
    private List<PgMirrorMessage> messages(String name, Object[] args, Object result, Object before)
            throws Exception {
        List<PgMirrorMessage> out = new ArrayList<>();
        switch (name) {
            case "createWorkflow":
            case "updateWorkflow":
                out.add(sql.workflow(name, (WorkflowModel) args[0]));
                break;
            case "createTasks":
                // what Scylla stored: every task it returned
                for (TaskModel t : (List<TaskModel>) result) {
                    out.add(sql.task(name, t));
                }
                break;
            case "updateTask":
                out.add(sql.task(name, (TaskModel) args[0]));
                break;
            case "removeTask":
                if (before != null) {
                    out.add(sql.removeTask((TaskModel) before));
                }
                break;
            case "removeWorkflow":
                if (before != null) {
                    out.add(sql.removeWorkflow((WorkflowModel) before));
                }
                break;
            case "removeFromPendingWorkflow":
                out.add(sql.removeFromPending((String) args[0], (String) args[1]));
                break;
            case "addEventExecution":
                if (Boolean.TRUE.equals(result)) { // false: it already existed, nothing was written
                    out.add(sql.addEventExecution((EventExecution) args[0]));
                }
                break;
            case "updateEventExecution":
                out.add(sql.updateEventExecution((EventExecution) args[0]));
                break;
            case "removeEventExecution":
                out.add(sql.removeEventExecution((EventExecution) args[0]));
                break;
            default:
                break;
        }
        return out;
    }

    private static void fail(String op, Throwable t) {
        Monitors.error(CLASS_NAME, op);
        LOGGER.warn(
                "pg-mirror: could not mirror {} (the change is not mirrored; reconciliation will repair it)",
                op,
                t);
    }
}
