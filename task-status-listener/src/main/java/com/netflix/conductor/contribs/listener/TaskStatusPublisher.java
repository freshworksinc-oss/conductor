/*
 * Copyright 2024 Conductor Authors.
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
package com.netflix.conductor.contribs.listener;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.listener.TaskStatusListener;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Singleton
public class TaskStatusPublisher implements TaskStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusPublisher.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String PAYLOAD_VERSION = "1.0";
    private static final String TASK_PAYLOAD_TYPE = "conductor_task_status";
    private static final Integer QDEPTH =
            Integer.parseInt(
                    System.getenv().getOrDefault("ENV_TASK_NOTIFICATION_QUEUE_SIZE", "50"));
    private BlockingQueue<TaskModel> blockingQueue = new LinkedBlockingDeque<>(QDEPTH);

    private RestClientManager rcm;
    private ExecutionDAOFacade executionDAOFacade;
    private List<String> subscribedTaskStatusList;

    class ExceptionHandler implements Thread.UncaughtExceptionHandler {
        public void uncaughtException(Thread t, Throwable e) {
            LOGGER.info("An exception has been captured\n");
            LOGGER.info("Thread: {}\n", t.getName());
            LOGGER.info("Exception: {}: {}\n", e.getClass().getName(), e.getMessage());
            LOGGER.info("Stack Trace: \n");
            e.printStackTrace(System.out);
            LOGGER.info("Thread status: {}\n", t.getState());
            new ConsumerThread().start();
        }
    }

    class ConsumerThread extends Thread {

        public void run() {
            this.setUncaughtExceptionHandler(new ExceptionHandler());
            String tName = Thread.currentThread().getName();
            LOGGER.info("{}: Starting consumer thread", tName);
            TaskModel task = null;
            TaskNotification taskNotification = null;
            while (true) {
                try {
                    task = blockingQueue.take();
                    // Extract accountId from TaskModel BEFORE serialization
                    Object accountId =
                            task.getInputData() != null
                                    ? task.getInputData().get("accountId")
                                    : null;
                    Task taskForNotification = task.toTask();
                    // Enrich the task input with _tenantContext read from the parent
                    // workflow's input, so Central consumers receive tenant context on
                    // task events as well (task input does not carry it by default).
                    addTenantContext(task, taskForNotification);
                    taskNotification = new TaskNotification(taskForNotification);
                    String jsonTask = taskNotification.toJsonString();
                    LOGGER.info("Publishing TaskNotification: {}", jsonTask);
                    if (taskNotification.getTaskType().equals("SUB_WORKFLOW")) {
                        LOGGER.info(
                                "Skip task '{}' notification. Task type is SUB_WORKFLOW.",
                                taskNotification.getTaskId());
                        continue;
                    }
                    publishTaskNotification(taskNotification, accountId);
                    LOGGER.debug("Task {} publish is successful.", taskNotification.getTaskId());
                    Thread.sleep(5);
                } catch (Exception e) {
                    if (taskNotification != null) {
                        LOGGER.error(
                                "Error while publishing task. Hence updating elastic search index taskId {} taskname {}",
                                task.getTaskId(),
                                task.getTaskDefName());
                        // TBD executionDAOFacade.indexTask(task);

                    } else {
                        LOGGER.error("Failed to publish task: Task is NULL");
                    }
                    LOGGER.error("Error on publishing ", e);
                }
            }
        }
    }

    @Inject
    public TaskStatusPublisher(
            RestClientManager rcm,
            ExecutionDAOFacade executionDAOFacade,
            List<String> subscribedTaskStatuses) {
        this.rcm = rcm;
        this.executionDAOFacade = executionDAOFacade;
        this.subscribedTaskStatusList = subscribedTaskStatuses;
        validateSubscribedTaskStatuses(subscribedTaskStatuses);
        ConsumerThread consumerThread = new ConsumerThread();
        consumerThread.start();
    }

    private void validateSubscribedTaskStatuses(List<String> subscribedTaskStatuses) {
        for (String taskStausType : subscribedTaskStatuses) {
            if (!taskStausType.equals("SCHEDULED")) {
                LOGGER.error(
                        "Task Status Type {} will only push notificaitons when updated through the API. Automatic notifications only work for SCHEDULED type.",
                        taskStausType);
            }
        }
    }

    private void enqueueTask(TaskModel task) {
        try {
            blockingQueue.put(task);
        } catch (Exception e) {
            LOGGER.debug(
                    "Failed to enqueue task: Id {} Type {} of workflow {} ",
                    task.getTaskId(),
                    task.getTaskType(),
                    task.getWorkflowInstanceId());
            LOGGER.debug(e.toString());
        }
    }

    /**
     * Reads {@code _tenantContext} from the parent workflow's input and adds it to the task input
     * used for the notification payload. The task's own input does not carry {@code _tenantContext}
     * by default, so Central consumers need it copied in from the workflow. Failures here must
     * never block publishing, so any exception is swallowed with a warning.
     */
    private void addTenantContext(TaskModel task, Task taskForNotification) {
        try {
            Object tenantContext = extractTenantContext(task);
            if (tenantContext == null) {
                return;
            }
            Map<String, Object> enrichedInput =
                    taskForNotification.getInputData() != null
                            ? new LinkedHashMap<>(taskForNotification.getInputData())
                            : new LinkedHashMap<>();
            enrichedInput.put("_tenantContext", tenantContext);
            taskForNotification.setInputData(enrichedInput);
        } catch (Exception e) {
            LOGGER.warn(
                    "Unable to add _tenantContext to task {} notification: {}",
                    task.getTaskId(),
                    e.getMessage());
        }
    }

    /**
     * Resolves {@code _tenantContext} for a task without a DB round-trip when possible. The task
     * input usually already embeds it (either directly, or nested under the injected {@code
     * ${workflow.input}} under the {@code workflow} key), so we reuse that first and only fetch the
     * parent workflow as a fallback for workflows that do not pass their input into the task.
     */
    private Object extractTenantContext(TaskModel task) {
        Map<String, Object> taskInput = task.getInputData();
        if (taskInput != null) {
            Object direct = taskInput.get("_tenantContext");
            if (direct != null) {
                return direct;
            }
            Object workflowInput = taskInput.get("workflow");
            if (workflowInput instanceof Map) {
                Object nested = ((Map<?, ?>) workflowInput).get("_tenantContext");
                if (nested != null) {
                    return nested;
                }
            }
        }
        // Fallback: the task did not carry the workflow input, so read it from the workflow.
        WorkflowModel workflow =
                executionDAOFacade.getWorkflowModel(task.getWorkflowInstanceId(), false);
        if (workflow != null && workflow.getInput() != null) {
            return workflow.getInput().get("_tenantContext");
        }
        return null;
    }

    @Override
    public void onTaskScheduled(TaskModel task) {
        if (subscribedTaskStatusList.contains(TaskModel.Status.SCHEDULED.name())) {
            enqueueTask(task);
        }
    }

    @Override
    public void onTaskCanceled(TaskModel task) {
        if (subscribedTaskStatusList.contains(TaskModel.Status.CANCELED.name())) {
            enqueueTask(task);
        }
    }

    @Override
    public void onTaskCompleted(TaskModel task) {
        if (subscribedTaskStatusList.contains(TaskModel.Status.COMPLETED.name())) {
            enqueueTask(task);
        }
    }

    @Override
    public void onTaskCompletedWithErrors(TaskModel task) {
        if (subscribedTaskStatusList.contains(TaskModel.Status.COMPLETED_WITH_ERRORS.name())) {
            enqueueTask(task);
        }
    }

    @Override
    public void onTaskFailed(TaskModel task) {
        if (subscribedTaskStatusList.contains(TaskModel.Status.FAILED.name())) {
            enqueueTask(task);
        }
    }

    @Override
    public void onTaskFailedWithTerminalError(TaskModel task) {
        if (subscribedTaskStatusList.contains(TaskModel.Status.FAILED_WITH_TERMINAL_ERROR.name())) {
            enqueueTask(task);
        }
    }

    @Override
    public void onTaskInProgress(TaskModel task) {
        if (subscribedTaskStatusList.contains(TaskModel.Status.IN_PROGRESS.name())) {
            enqueueTask(task);
        }
    }

    @Override
    public void onTaskSkipped(TaskModel task) {
        if (subscribedTaskStatusList.contains(TaskModel.Status.SKIPPED.name())) {
            enqueueTask(task);
        }
    }

    @Override
    public void onTaskTimedOut(TaskModel task) {
        if (subscribedTaskStatusList.contains(TaskModel.Status.TIMED_OUT.name())) {
            enqueueTask(task);
        }
    }

    /**
     * The summary {@code input}/{@code output} fields are Strings that already contain serialized
     * JSON, so they show up double-encoded (an escaped JSON string) in the payload. Central expects
     * real nested objects, so this replaces the String node with the parsed JSON when the content
     * is valid JSON. If it is not valid JSON (e.g. Java {@code toString()} when {@code
     * conductor.app.summary-input-output-json-serialization.enabled=false}) the original string is
     * kept so publishing never fails.
     */
    private void inlineJsonString(ObjectNode payload, String field) {
        JsonNode value = payload.get(field);
        if (value != null && value.isTextual()) {
            String text = value.asText();
            if (text != null && !text.isEmpty()) {
                try {
                    payload.set(field, objectMapper.readTree(text));
                } catch (IOException e) {
                    LOGGER.debug(
                            "Field '{}' is not valid JSON; leaving as string for task {}",
                            field,
                            payload.path("taskId").asText());
                }
            }
        }
    }

    /**
     * Central consumers need tenant identity as a top-level field alongside {@code correlationId}
     * and {@code domain}, not buried inside {@code input}. Copy (rather than move) it so existing
     * consumers reading {@code input._tenantContext} keep working.
     */
    private void exposeTenantContextAtRoot(ObjectNode payload) {
        JsonNode inputNode = payload.get("input");
        if (inputNode instanceof ObjectNode) {
            JsonNode tenantContext = inputNode.get("_tenantContext");
            if (tenantContext != null) {
                payload.set("_tenantContext", tenantContext);
            }
        }
    }

    private void publishTaskNotification(TaskNotification taskNotification, Object accountId)
            throws IOException {
        // Get the existing task JSON (with all current fields)
        String existingTaskJson = taskNotification.toJsonStringWithInputOutput();

        if (!Objects.nonNull(accountId) || accountId.toString().trim().isEmpty()) {
            accountId = "-1";
            LOGGER.warn(
                    "Account ID is missing in task input. Task ID: {}, Workflow ID: {}. Using default fallback account_id: {}",
                    taskNotification.getTaskId(),
                    taskNotification.getWorkflowId(),
                    accountId);
        }

        // Parse existing JSON into JsonNode for wrapping
        JsonNode existingPayload = objectMapper.readTree(existingTaskJson);

        // input/output are String fields that already hold serialized JSON, so they arrive
        // double-encoded (an escaped JSON string, not an object). Inline them into real JSON
        // nodes so Central receives clean nested objects it can parse in one pass.
        if (existingPayload instanceof ObjectNode) {
            ObjectNode payloadNode = (ObjectNode) existingPayload;
            inlineJsonString(payloadNode, "input");
            inlineJsonString(payloadNode, "output");
            exposeTenantContextAtRoot(payloadNode);
        }

        // Wrap in Central envelope
        ObjectNode centralMessage = objectMapper.createObjectNode();
        centralMessage.put("account_id", String.valueOf(accountId));
        centralMessage.put("payload_type", TASK_PAYLOAD_TYPE);
        centralMessage.put("payload_version", PAYLOAD_VERSION);
        centralMessage.set("payload", existingPayload); // Keep ALL existing fields

        String wrappedJson = centralMessage.toString();

        LOGGER.info(
                "Publishing Task to Central with envelope. Task ID: {}, Account ID: {}",
                taskNotification.getTaskId(),
                accountId);
        LOGGER.info("Task Event Payload being published to Central: {}", wrappedJson);

        // Send wrapped JSON to Central
        rcm.postNotification(
                RestClientManager.NotificationType.TASK,
                wrappedJson,
                taskNotification.getTaskId(),
                null);

        LOGGER.debug("Task {} publish to Central is successful.", taskNotification.getTaskId());
    }
}
