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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.model.TaskModel;

class BusinessTraceTags {

    private BusinessTraceTags() {}

    static void tagBefore(TracingFacade tracing, String operation, ProceedingJoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        switch (operation) {
            case "metadata.register_task_def" -> tagRegisterTaskDefBefore(tracing, args);
            case "metadata.get_task_def" -> tagStringArg(tracing, "task.type", firstStringArg(args));
            case "metadata.register_workflow_def" -> tagWorkflowDefBefore(tracing, args);
            case "metadata.get_workflow_def" -> tagGetWorkflowDefBefore(tracing, args);
            case "workflow.start" -> tagWorkflowStartBefore(tracing, args);
            case "workflow.get_execution" -> tagGetExecutionBefore(tracing, args);
            case "task.poll" -> tagTaskPollBefore(tracing, args);
            case "task.batch_poll" -> tagTaskBatchPollBefore(tracing, args);
            case "task.update" -> tagTaskUpdateBefore(tracing, args);
            case "workflow.search",
                    "workflow.search_v2",
                    "task.search",
                    "task.search_v2" -> tagSearchBefore(tracing, args);
            default -> {}
        }
    }

    static void tagAfter(
            TracingFacade tracing, String operation, ProceedingJoinPoint joinPoint, Object result) {
        switch (operation) {
            case "workflow.start" -> tagStringResult(tracing, "workflow.id", result);
            case "workflow.get_execution" -> tagGetExecutionAfter(tracing, result);
            case "task.poll" -> tagTaskPollAfter(tracing, result);
            case "task.batch_poll" -> tagTaskBatchPollAfter(tracing, result);
            case "task.update" -> tagTaskUpdateAfter(tracing, result);
            case "workflow.search",
                    "workflow.search_v2",
                    "task.search",
                    "task.search_v2" -> tagSearchAfter(tracing, result);
            default -> {}
        }
    }

    private static void tagRegisterTaskDefBefore(TracingFacade tracing, Object[] args) {
        if (args.length == 0 || !(args[0] instanceof List<?> taskDefs)) {
            return;
        }
        tracing.tag("task.count", String.valueOf(taskDefs.size()));
        String names =
                taskDefs.stream()
                        .filter(TaskDef.class::isInstance)
                        .map(TaskDef.class::cast)
                        .map(TaskDef::getName)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.joining(","));
        if (StringUtils.isNotBlank(names)) {
            tracing.tag("task.name", names);
        }
    }

    private static void tagWorkflowDefBefore(TracingFacade tracing, Object[] args) {
        if (args.length == 0 || !(args[0] instanceof WorkflowDef workflowDef)) {
            return;
        }
        tracing.tag("workflow.name", workflowDef.getName());
        tracing.tag("workflow.version", String.valueOf(workflowDef.getVersion()));
    }

    private static void tagGetWorkflowDefBefore(TracingFacade tracing, Object[] args) {
        if (args.length > 0) {
            tracing.tag("workflow.name", stringValue(args[0]));
        }
        if (args.length > 1 && args[1] != null) {
            tracing.tag("workflow.version", String.valueOf(args[1]));
        }
    }

    private static void tagWorkflowStartBefore(TracingFacade tracing, Object[] args) {
        if (args.length == 0) {
            return;
        }
        if (args[0] instanceof StartWorkflowRequest request) {
            tracing.tag("workflow.name", request.getName());
            if (request.getVersion() != null) {
                tracing.tag("workflow.version", String.valueOf(request.getVersion()));
            }
            tracing.tag("correlation.id", request.getCorrelationId());
            return;
        }
        if (args.length > 0) {
            tracing.tag("workflow.name", stringValue(args[0]));
        }
        if (args.length > 1 && args[1] != null) {
            tracing.tag("workflow.version", String.valueOf(args[1]));
        }
        if (args.length > 2) {
            tracing.tag("correlation.id", stringValue(args[2]));
        }
    }

    private static void tagGetExecutionBefore(TracingFacade tracing, Object[] args) {
        if (args.length > 0) {
            tracing.tag("workflow.id", stringValue(args[0]));
        }
        if (args.length > 1 && args[1] instanceof Boolean includeTasks) {
            tracing.tag("workflow.include_tasks", String.valueOf(includeTasks));
        }
    }

    private static void tagGetExecutionAfter(TracingFacade tracing, Object result) {
        if (!(result instanceof Workflow workflow)) {
            return;
        }
        tracing.tag("workflow.name", workflow.getWorkflowName());
        tracing.tag("workflow.version", String.valueOf(workflow.getWorkflowVersion()));
        if (workflow.getStatus() != null) {
            tracing.tag("workflow.status", workflow.getStatus().name());
        }
    }

    private static void tagTaskPollBefore(TracingFacade tracing, Object[] args) {
        if (args.length > 0) {
            tracing.tag("task.type", stringValue(args[0]));
        }
        if (args.length > 1) {
            tracing.tag("worker.id", stringValue(args[1]));
        }
    }

    private static void tagTaskPollAfter(TracingFacade tracing, Object result) {
        if (result instanceof Task task) {
            tracing.tag("task.poll.result", "hit");
            tracing.tag("task.id", task.getTaskId());
            tracing.tag("workflow.id", task.getWorkflowInstanceId());
        } else {
            tracing.tag("task.poll.result", "miss");
        }
    }

    private static void tagTaskBatchPollBefore(TracingFacade tracing, Object[] args) {
        if (args.length > 0) {
            tracing.tag("task.type", stringValue(args[0]));
        }
        if (args.length > 1) {
            tracing.tag("worker.id", stringValue(args[1]));
        }
        if (args.length > 3 && args[3] != null) {
            tracing.tag("task.poll.count_requested", String.valueOf(args[3]));
        }
    }

    private static void tagTaskBatchPollAfter(TracingFacade tracing, Object result) {
        if (result instanceof Collection<?> tasks) {
            tracing.tag("task.poll.count_returned", String.valueOf(tasks.size()));
        }
    }

    private static void tagTaskUpdateBefore(TracingFacade tracing, Object[] args) {
        if (args.length == 0) {
            return;
        }
        if (args[0] instanceof TaskResult taskResult) {
            tracing.tag("task.id", taskResult.getTaskId());
            tracing.tag("workflow.id", taskResult.getWorkflowInstanceId());
            if (taskResult.getStatus() != null) {
                tracing.tag("task.status", taskResult.getStatus().name());
            }
        }
    }

    private static void tagTaskUpdateAfter(TracingFacade tracing, Object result) {
        if (result instanceof TaskModel taskModel) {
            tracing.tag("task.type", taskModel.getTaskType());
        }
    }

    private static void tagSearchBefore(TracingFacade tracing, Object[] args) {
        if (args.length > 0 && args[0] != null) {
            tracing.tag("search.start", String.valueOf(args[0]));
        }
        if (args.length > 1 && args[1] != null) {
            tracing.tag("search.size", String.valueOf(args[1]));
        }
        if (args.length > 4) {
            tracing.tag(
                    "search.has_free_text",
                    String.valueOf(StringUtils.isNotBlank(stringValue(args[3]))));
            tracing.tag(
                    "search.has_query", String.valueOf(StringUtils.isNotBlank(stringValue(args[4]))));
        } else if (args.length > 3) {
            tracing.tag(
                    "search.has_query", String.valueOf(StringUtils.isNotBlank(stringValue(args[3]))));
        }
    }

    private static void tagSearchAfter(TracingFacade tracing, Object result) {
        if (!(result instanceof SearchResult<?> searchResult)) {
            return;
        }
        tracing.tag("search.result.total_hits", String.valueOf(searchResult.getTotalHits()));
        if (searchResult.getResults() != null) {
            tracing.tag("search.result.page_size", String.valueOf(searchResult.getResults().size()));
        }
    }

    private static void tagStringArg(TracingFacade tracing, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            tracing.tag(key, value);
        }
    }

    private static void tagStringResult(TracingFacade tracing, String key, Object result) {
        if (result != null) {
            tracing.tag(key, result.toString());
        }
    }

    private static String firstStringArg(Object[] args) {
        if (args.length == 0 || args[0] == null) {
            return null;
        }
        return args[0].toString();
    }

    private static String stringValue(Object value) {
        return value != null ? value.toString() : null;
    }
}
