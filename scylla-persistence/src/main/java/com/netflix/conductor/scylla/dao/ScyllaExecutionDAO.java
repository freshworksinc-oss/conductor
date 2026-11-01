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
package com.netflix.conductor.scylla.dao;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.CollectionUtils;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.core.exception.NonTransientException;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.core.sync.Lock;
import com.netflix.conductor.dao.ConcurrentExecutionLimitDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.redislock.lock.RedisLock;
import com.netflix.conductor.scylla.config.ScyllaProperties;
import com.netflix.conductor.scylla.util.ShardResolver;
import com.netflix.conductor.scylla.util.ShardResolver.WorkflowOwner;
import com.netflix.conductor.scylla.util.Statements;
import com.netflix.conductor.tenant.TenantContext;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import static com.netflix.conductor.scylla.util.Constants.ACCOUNT_ID_KEY;
import static com.netflix.conductor.scylla.util.Constants.DEFAULT_TOTAL_PARTITIONS;
import static com.netflix.conductor.scylla.util.Constants.ENTITY_KEY;
import static com.netflix.conductor.scylla.util.Constants.ENTITY_TYPE_TASK;
import static com.netflix.conductor.scylla.util.Constants.ENTITY_TYPE_WORKFLOW;
import static com.netflix.conductor.scylla.util.Constants.PAYLOAD_KEY;
import static com.netflix.conductor.scylla.util.Constants.SHARD_ID_KEY;
import static com.netflix.conductor.scylla.util.Constants.TASK_ID_KEY;
import static com.netflix.conductor.scylla.util.Constants.TENANT_ID_KEY;
import static com.netflix.conductor.scylla.util.Constants.TOTAL_PARTITIONS_KEY;
import static com.netflix.conductor.scylla.util.Constants.TOTAL_TASKS_KEY;
import static com.netflix.conductor.scylla.util.Constants.VERSION;
import static com.netflix.conductor.scylla.util.Constants.WORKFLOW_ID_KEY;

@Trace
public class ScyllaExecutionDAO extends ScyllaBaseDAO
        implements ExecutionDAO, ConcurrentExecutionLimitDAO, ApplicationContextAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaExecutionDAO.class);
    private static final String CLASS_NAME = ScyllaExecutionDAO.class.getSimpleName();

    protected final PreparedStatement insertWorkflowStatement;
    protected final PreparedStatement insertTaskStatement;
    protected final PreparedStatement insertEventExecutionStatement;
    protected final PreparedStatement insertTaskInProgressStatement;
    protected final PreparedStatement insertTaskInProgressV2Statement;
    protected final PreparedStatement selectTaskInProgressStatement;
    protected final PreparedStatement selectTaskInProgressV2Statement;
    protected final PreparedStatement updateTaskInProgressStatement;
    protected final PreparedStatement updateTaskInProgressV2Statement;
    protected final PreparedStatement deleteTaskInProgressStatement;
    protected final PreparedStatement deleteTaskInProgressV2Statement;
    protected final PreparedStatement selectTotalStatement;
    protected final PreparedStatement selectTaskStatement;
    protected final PreparedStatement selectWorkflowStatement;
    protected final PreparedStatement selectWorkflowWithTasksStatement;
    protected final PreparedStatement selectTaskLookupStatement;
    protected final PreparedStatement selectShardFromTaskLookupStatement;

    protected final PreparedStatement selectWorkflowsByCorIdFromWorkflowStatement;

    protected final PreparedStatement selectShardFromWorkflowLookupStatement;
    protected final PreparedStatement insertWorkflowLookupStatement;
    protected final PreparedStatement updateWorkflowLookupStatement;
    protected final PreparedStatement deleteWorkflowLookupStatement;
    protected final PreparedStatement selectTasksFromTaskDefLimitStatement;
    protected final PreparedStatement selectEventExecutionsStatement;

    protected final PreparedStatement updateWorkflowStatement;
    protected final PreparedStatement updateTotalTasksStatement;
    protected final PreparedStatement updateTotalPartitionsStatement;
    protected final PreparedStatement updateTaskLookupStatement;
    protected final PreparedStatement updateTaskDefLimitStatement;
    protected final PreparedStatement updateEventExecutionStatement;

    protected final PreparedStatement deleteWorkflowStatement;
    protected final PreparedStatement deleteTaskStatement;
    protected final PreparedStatement deleteTaskLookupStatement;
    protected final PreparedStatement deleteTaskDefLimitStatement;
    protected final PreparedStatement deleteEventExecutionStatement;

    protected final int eventExecutionsTTL;
    private RedisLock redisLock;

    // workflowId -> shard and owner. Both are fixed at creation, so entries never go stale.
    private final Cache<String, WorkflowOwner> ownerCache =
            CacheBuilder.newBuilder()
                    .maximumSize(100_000)
                    .expireAfterAccess(1, TimeUnit.HOURS)
                    .build();

    private boolean isConcurrencyLimitEnabled;
    private boolean fallbackToOldTaskInProgressActive;

    public ScyllaExecutionDAO(
            Session session,
            ObjectMapper objectMapper,
            ScyllaProperties properties,
            Statements statements) {
        super(session, objectMapper, properties);

        eventExecutionsTTL = (int) properties.getEventExecutionPersistenceTtl().getSeconds();

        this.insertWorkflowStatement =
                session.prepare(statements.getInsertWorkflowStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.insertTaskStatement =
                session.prepare(statements.getInsertTaskStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.insertEventExecutionStatement =
                session.prepare(statements.getInsertEventExecutionStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.insertTaskInProgressStatement =
                session.prepare(statements.getInsertTaskInProgressStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.insertTaskInProgressV2Statement =
                session.prepare(statements.getInsertTaskInProgressV2Statement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.selectTaskInProgressStatement =
                session.prepare(statements.getSelectTaskInProgressStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());

        this.selectTaskInProgressV2Statement =
                session.prepare(statements.getSelectTaskInProgressV2Statement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());

        this.selectShardFromTaskLookupStatement =
                session.prepare(statements.getSelectShardFromTaskLookupTableStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());

        this.selectWorkflowsByCorIdFromWorkflowStatement =
                session.prepare(statements.getSelectWorkflowsByCorrelationIdStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());

        this.selectShardFromWorkflowLookupStatement =
                session.prepare(statements.getSelectShardFromWorkflowLookupTableStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());

        this.insertWorkflowLookupStatement =
                session.prepare(statements.getInsertWorkflowLookupStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.updateWorkflowLookupStatement =
                session.prepare(statements.getUpdateWorkflowLookupStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());

        this.deleteWorkflowLookupStatement =
                session.prepare(statements.getDeleteWorkflowLookupStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());

        this.updateTaskInProgressStatement =
                session.prepare(statements.getUpdateTaskInProgressStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.updateTaskInProgressV2Statement =
                session.prepare(statements.getUpdateTaskInProgressV2Statement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.deleteTaskInProgressStatement =
                session.prepare(statements.getDeleteTaskInProgressStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.deleteTaskInProgressV2Statement =
                session.prepare(statements.getDeleteTaskInProgressV2Statement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.selectTotalStatement =
                session.prepare(statements.getSelectTotalStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectTaskStatement =
                session.prepare(statements.getSelectTaskStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectWorkflowStatement =
                session.prepare(statements.getSelectWorkflowStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectWorkflowWithTasksStatement =
                session.prepare(statements.getSelectWorkflowWithTasksStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectTaskLookupStatement =
                session.prepare(statements.getSelectTaskFromLookupTableStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectTasksFromTaskDefLimitStatement =
                session.prepare(statements.getSelectTasksFromTaskDefLimitStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectEventExecutionsStatement =
                session.prepare(
                                statements
                                        .getSelectAllEventExecutionsForMessageFromEventExecutionsStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.updateWorkflowStatement =
                session.prepare(statements.getUpdateWorkflowStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.updateTotalTasksStatement =
                session.prepare(statements.getUpdateTotalTasksStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.updateTotalPartitionsStatement =
                session.prepare(statements.getUpdateTotalPartitionsStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.updateTaskLookupStatement =
                session.prepare(statements.getUpdateTaskLookupStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.updateTaskDefLimitStatement =
                session.prepare(statements.getUpdateTaskDefLimitStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.updateEventExecutionStatement =
                session.prepare(statements.getUpdateEventExecutionStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());

        this.deleteWorkflowStatement =
                session.prepare(statements.getDeleteWorkflowStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.deleteTaskStatement =
                session.prepare(statements.getDeleteTaskStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.deleteTaskLookupStatement =
                session.prepare(statements.getDeleteTaskLookupStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.deleteTaskDefLimitStatement =
                session.prepare(statements.getDeleteTaskDefLimitStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.deleteEventExecutionStatement =
                session.prepare(statements.getDeleteEventExecutionsStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
    }

    @Override
    public List<TaskModel> getPendingTasksByWorkflow(String taskName, String workflowId) {
        List<TaskModel> tasks = getTasksForWorkflow(workflowId);
        return tasks.stream()
                .filter(task -> taskName.equals(task.getTaskType()))
                .filter(task -> TaskModel.Status.IN_PROGRESS.equals(task.getStatus()))
                .collect(Collectors.toList());
    }

    /**
     * This is a dummy implementation and this feature is not implemented for Cassandra backed
     * Conductor
     */
    @Override
    public List<TaskModel> getTasks(String taskType, String startKey, int count) {
        throw new UnsupportedOperationException(
                "This method is not implemented in ScyllaExecutionDAO. Please use ExecutionDAOFacade instead.");
    }

    /**
     * Inserts tasks into the Cassandra datastore. <b>Note:</b> Creates the task_id to workflow_id
     * mapping in the task_lookup table first. Once this succeeds, inserts the tasks into the
     * workflows table. Tasks belonging to the same shard are created using batch statements.
     *
     * @param tasks tasks to be created
     */
    @Override
    public List<TaskModel> createTasks(List<TaskModel> tasks) {
        validateTasks(tasks);
        String workflowId = tasks.get(0).getWorkflowInstanceId();
        UUID workflowUUID = toUUID(workflowId, "Invalid workflow id");
        try {
            // Tasks live in their workflow's partition, so they take the workflow's stored shard.
            WorkflowOwner owner = ownerOf(workflowId, tasks.get(0).getCorrelationId());
            int correlationId = owner.getShardId();
            if (ownerCache.getIfPresent(workflowId) == null) {
                // ownerOf caches every workflow that has a lookup row, so this is a legacy workflow
                // without one: record its shard once.
                session.execute(updateWorkflowLookupStatement.bind(correlationId, workflowUUID));
                ownerCache.put(workflowId, owner);
            }
            WorkflowMetadata workflowMetadata = getWorkflowMetadata(workflowId, correlationId);
            int totalTasks = workflowMetadata.getTotalTasks() + tasks.size();
            // update the task_lookup table
            // update the workflow_lookup table
            LOGGER.debug(
                    "Create tasks list {} for workflowId {} ",
                    tasks.stream()
                            .map(TaskModel::getReferenceTaskName)
                            .collect(Collectors.toList()),
                    workflowId);
            tasks.forEach(
                    task -> {
                        if (task.getScheduledTime() == 0) {
                            task.setScheduledTime(System.currentTimeMillis());
                        }
                        session.execute(
                                updateTaskLookupStatement.bind(
                                        workflowUUID,
                                        correlationId,
                                        toUUID(task.getTaskId(), "Invalid task id")));
                        // Added the task to task_in_progress table
                        addTaskInProgress(task);
                    });

            // update all the tasks in the workflow using batch
            BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            tasks.forEach(
                    task -> {
                        String taskPayload = toJson(task);
                        batchStatement.add(
                                insertTaskStatement.bind(
                                        workflowUUID,
                                        correlationId,
                                        task.getTaskId(),
                                        taskPayload));

                        recordCassandraDaoRequests(
                                "createTask", task.getTaskType(), task.getWorkflowType());

                        recordCassandraDaoPayloadSize(
                                "createTask",
                                taskPayload.length(),
                                task.getTaskType(),
                                task.getWorkflowType());
                    });
            batchStatement.add(
                    updateTotalTasksStatement.bind(totalTasks, workflowUUID, correlationId));
            session.execute(batchStatement);
            // update the total tasks and partitions for the workflow
            session.execute(
                    updateTotalPartitionsStatement.bind(
                            DEFAULT_TOTAL_PARTITIONS, totalTasks, workflowUUID, correlationId));

            return tasks;
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "createTasks");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format(
                            "Error creating %d tasks for workflow: %s", tasks.size(), workflowId);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    /**
     * @method to add the task_in_progress table with the status of the task if task is not already
     *     present
     */
    public void addTaskInProgress(TaskModel task) {
        ResultSet resultSet = getTaskInProgressTaskFromTaskModel(task);
        if (CollectionUtils.isEmpty(resultSet.all())) {
            if (isFallbackToOldTaskInProgressActive()) {
                session.execute(
                        insertTaskInProgressStatement.bind(
                                task.getTaskDefName(),
                                UUID.fromString(task.getTaskId()),
                                UUID.fromString(task.getWorkflowInstanceId()),
                                true));
            }
            session.execute(
                    insertTaskInProgressV2Statement.bind(
                            task.getTaskDefName(),
                            UUID.fromString(task.getTaskId()),
                            UUID.fromString(task.getWorkflowInstanceId()),
                            true));
        } else {
            LOGGER.info(
                    "Task with defName {} and Id {} and status {} in addTaskInProgress NOT inserted as already exists  ",
                    task.getTaskDefName(),
                    task.getTaskId(),
                    task.getStatus());
        }
    }

    private ResultSet getTaskInProgressTaskFromTaskModel(TaskModel task) {
        try {
            ResultSet resultSet = fetchFromTaskInProgressV2Query(task);
            if (isResultSetNotValid(resultSet)) {
                return fetchFromTaskInProgressQuery(task);
            }
            return resultSet;
        } catch (Exception ex) {
            LOGGER.error(
                    "Error while reading task from task_in_progress_v2 table - taskDefName: {}, taskId: {}, status: {}",
                    task.getTaskDefName(),
                    task.getTaskId(),
                    task.getStatus(),
                    ex);
            return null;
        }
    }

    private ResultSet fetchFromTaskInProgressV2Query(TaskModel task) {
        return session.execute(
                selectTaskInProgressV2Statement.bind(
                        task.getTaskDefName(), UUID.fromString(task.getTaskId())));
    }

    private ResultSet fetchFromTaskInProgressQuery(TaskModel task) {
        logTaskInProgressStatus(task);
        return session.execute(
                selectTaskInProgressStatement.bind(
                        task.getTaskDefName(), UUID.fromString(task.getTaskId())));
    }

    private boolean isResultSetNotValid(ResultSet resultSet) {
        return Objects.isNull(resultSet) || CollectionUtils.isEmpty(resultSet.all());
    }

    /**
     * @method to remove the task_in_progress table with the status of the task
     */
    public void removeTaskInProgress(TaskModel task) {
        if (isFallbackToOldTaskInProgressActive()) {
            BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            batchStatement.add(
                    deleteTaskInProgressV2Statement.bind(
                            task.getTaskDefName(), UUID.fromString(task.getTaskId())));
            batchStatement.add(
                    deleteTaskInProgressStatement.bind(
                            task.getTaskDefName(), UUID.fromString(task.getTaskId())));
            session.execute(batchStatement);
        } else {
            session.execute(
                    deleteTaskInProgressV2Statement.bind(
                            task.getTaskDefName(), UUID.fromString(task.getTaskId())));
        }
    }

    private void logTaskInProgressStatus(TaskModel task) {
        LOGGER.info(
                "Task NOT found in task_in_progress_v2 table - taskDefName {} taskId {} status {}",
                task.getTaskDefName(),
                task.getTaskId(),
                task.getStatus());
    }

    /**
     * @method to update the task_in_progress table with the status of the task
     */
    public void updateTaskInProgress(TaskModel task, boolean inProgress) {
        if (isFallbackToOldTaskInProgressActive()) {
            BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            batchStatement.add(
                    updateTaskInProgressV2Statement.bind(
                            inProgress, task.getTaskDefName(), UUID.fromString(task.getTaskId())));
            batchStatement.add(
                    updateTaskInProgressStatement.bind(
                            inProgress, task.getTaskDefName(), UUID.fromString(task.getTaskId())));
            session.execute(batchStatement);
        } else {
            session.execute(
                    updateTaskInProgressV2Statement.bind(
                            inProgress, task.getTaskDefName(), UUID.fromString(task.getTaskId())));
        }
    }

    @Override
    public void updateTask(TaskModel task) {
        try {
            int correlationId =
                    ownerOf(task.getWorkflowInstanceId(), task.getCorrelationId()).getShardId();
            String taskPayload = toJson(task);
            recordCassandraDaoRequests("updateTask", task.getTaskType(), task.getWorkflowType());
            recordCassandraDaoPayloadSize(
                    "updateTask", taskPayload.length(), task.getTaskType(), task.getWorkflowType());
            if (redisLock.acquireLock(task.getTaskId(), 2, TimeUnit.SECONDS)) {
                /**
                 * commenting this block of code as its no longer required as we already have a
                 * redis based distributed locking, which can now handle dirty read write. TaskModel
                 * prevTask = getTask(task.getTaskId()); LOGGER.debug("Received updateTask for task
                 * {} with taskStatus {} in workflow {} with taskRefName {} and prevTaskStatus {} ",
                 * task.getTaskId(), task.getStatus(), task.getWorkflowInstanceId(),
                 * task.getReferenceTaskName(), prevTask.getStatus());
                 *
                 * <p>if (!prevTask.getStatus().equals(TaskModel.Status.COMPLETED)) {*
                 */
                session.execute(
                        insertTaskStatement.bind(
                                UUID.fromString(task.getWorkflowInstanceId()),
                                correlationId,
                                task.getTaskId(),
                                taskPayload));
                LOGGER.debug(
                        "Updated updateTask for task {} with taskStatus {}  with taskRefName {} for workflowId {} ",
                        task.getTaskId(),
                        task.getStatus(),
                        task.getReferenceTaskName(),
                        task.getWorkflowInstanceId());
                // }
                verifyTaskStatus(task);
            }
            redisLock.releaseLock(task.getTaskId());
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "updateTask");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format(
                            "Error updating task: %s in workflow: %s",
                            task.getTaskId(), task.getWorkflowInstanceId());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    private void logErrorToDebug(String mName, DriverException e) {
        LOGGER.error(
                "Error in ScyllaExecutionDAO method - {} message - {} stackTrace- {}",
                mName,
                e.getMessage(),
                e.getStackTrace());
    }

    /**
     * @method to verify the task status and update the task_in_progress table also removes if its a
     *     terminal task
     */
    private void verifyTaskStatus(TaskModel task) {
        boolean inProgress =
                task.getStatus() != null && task.getStatus().equals(TaskModel.Status.IN_PROGRESS);
        updateTaskInProgress(task, inProgress);
        if (task.getStatus().isTerminal()) {
            removeTaskFromLimit(task);
            removeTaskInProgress(task);
        } else if (task.getStatus() == TaskModel.Status.IN_PROGRESS) {
            addTaskToLimit(task);
        }
    }

    /**
     * This is a dummy implementation and this feature is not implemented for Cassandra backed
     * Conductor
     */
    @Override
    public boolean exceedsLimit(TaskModel task) {
        if (!isConcurrencyLimitEnabled()) {
            return false;
        }
        Optional<TaskDef> taskDefinition = task.getTaskDefinition();
        if (taskDefinition.isEmpty()) {
            return false;
        }
        int limit = taskDefinition.get().concurrencyLimit();
        if (limit <= 0) {
            return false;
        }

        try {
            recordCassandraDaoRequests(
                    "selectTaskDefLimit", task.getTaskType(), task.getWorkflowType());
            ResultSet resultSet =
                    session.execute(
                            selectTasksFromTaskDefLimitStatement.bind(task.getTaskDefName()));
            List<String> taskIds =
                    resultSet.all().stream()
                            .map(row -> row.getUUID(TASK_ID_KEY).toString())
                            .collect(Collectors.toList());
            long current = taskIds.size();

            if (!taskIds.contains(task.getTaskId()) && current >= limit) {
                LOGGER.info(
                        "Task execution count limited. task - {}:{}, limit: {}, current: {}",
                        task.getTaskId(),
                        task.getTaskDefName(),
                        limit,
                        current);
                Monitors.recordTaskConcurrentExecutionLimited(task.getTaskDefName(), limit);
                return true;
            }
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "exceedsLimit");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format(
                            "Failed to get in progress limit - %s:%s in workflow :%s",
                            task.getTaskDefName(), task.getTaskId(), task.getWorkflowInstanceId());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
        return false;
    }

    @Override
    public boolean removeTask(String taskId) {
        TaskModel task = getTask(taskId);
        if (task == null) {
            LOGGER.warn("No such task found by id {}", taskId);
            return false;
        }
        return removeTask(task);
    }

    // Not part of the Freshworks ExecutionDAO interface (Journeys-only overload)
    public TaskModel getTask(String shardId, String workflowId, String taskId) {
        try {
            return getTaskModel(ShardResolver.legacyShard(shardId), taskId, workflowId);
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getTask");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg = String.format("Error getting task by id: %s", taskId);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    @Override
    public TaskModel getTask(String taskId) {
        try {
            String workflowId = lookupWorkflowIdFromTaskId(taskId);
            String shardId = lookupShardIdFromTaskId(taskId);
            return getTaskModel(ShardResolver.legacyShard(shardId), taskId, workflowId);
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getTask");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg = String.format("Error getting task by id: %s", taskId);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    private TaskModel getTaskModel(int correlationId, String taskId, String workflowId) {
        if (workflowId == null) {
            return null;
        }
        // TODO: implement for query against multiple shards

        ResultSet resultSet =
                session.execute(
                        selectTaskStatement.bind(
                                UUID.fromString(workflowId), correlationId, taskId));
        return Optional.ofNullable(resultSet.one())
                .map(
                        row -> {
                            // The partition's static owner columns come back with the task row.
                            if (!callerOwns(
                                    row.getString(TENANT_ID_KEY), row.getString(ACCOUNT_ID_KEY))) {
                                LOGGER.info(
                                        "Task {} is not visible to {}",
                                        taskId,
                                        TenantContext.get());
                                return null;
                            }
                            String taskRow = row.getString(PAYLOAD_KEY);
                            TaskModel task = readValue(taskRow, TaskModel.class);
                            recordCassandraDaoRequests(
                                    "getTask", task.getTaskType(), task.getWorkflowType());
                            recordCassandraDaoPayloadSize(
                                    "getTask",
                                    taskRow.length(),
                                    task.getTaskType(),
                                    task.getWorkflowType());
                            return task;
                        })
                .orElse(null);
    }

    @Override
    public List<TaskModel> getTasks(List<String> taskIds) {
        Preconditions.checkNotNull(taskIds);
        Preconditions.checkArgument(taskIds.size() > 0, "Task ids list cannot be empty");
        String workflowId = lookupWorkflowIdFromTaskId(taskIds.get(0));
        if (workflowId == null) {
            return null;
        }
        WorkflowModel workflow = getWorkflow(workflowId, true);
        if (workflow == null) {
            return new ArrayList<>();
        }
        return workflow.getTasks().stream()
                .filter(task -> taskIds.contains(task.getTaskId()))
                .collect(Collectors.toList());
    }

    /**
     * This is a dummy implementation and this feature is not implemented for Cassandra backed
     * Conductor
     */
    @Override
    public List<TaskModel> getPendingTasksForTaskType(String taskType) {
        throw new UnsupportedOperationException(
                "This method is not implemented in ScyllaExecutionDAO. Please use ExecutionDAOFacade instead.");
    }

    @Override
    public List<TaskModel> getTasksForWorkflow(String workflowId) {
        WorkflowModel workflow = getWorkflow(workflowId, true);
        return workflow == null ? new ArrayList<>() : workflow.getTasks();
    }

    @Override
    public String createWorkflow(WorkflowModel workflow) {
        try {
            List<TaskModel> tasks = workflow.getTasks();
            workflow.setTasks(new LinkedList<>());
            String payload = toJson(workflow);
            // The only place a shard is decided; everything after reads the stored value.
            WorkflowOwner owner = resolveOwner(workflow);
            UUID workflowUUID = UUID.fromString(workflow.getWorkflowId());
            LOGGER.info("Workflow {} owner {}", workflow.getWorkflowId(), owner);
            recordCassandraDaoRequests("createWorkflow", "n/a", workflow.getWorkflowName());
            recordCassandraDaoPayloadSize(
                    "createWorkflow", payload.length(), "n/a", workflow.getWorkflowName());
            // Lookup first, so a reader that finds the workflow can always find its shard.
            session.execute(
                    insertWorkflowLookupStatement.bind(
                            workflowUUID,
                            owner.getShardId(),
                            owner.getTenantId(),
                            owner.getAccountId()));
            session.execute(
                    insertWorkflowStatement.bind(
                            workflowUUID,
                            owner.getShardId(),
                            "",
                            payload,
                            0,
                            1,
                            1,
                            owner.getTenantId(),
                            owner.getAccountId()));
            ownerCache.put(workflow.getWorkflowId(), owner);

            workflow.setTasks(tasks);
            return workflow.getWorkflowId();
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "createWorkflow");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format("Error creating workflow: %s", workflow.getWorkflowId());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    @Override
    public String updateWorkflow(WorkflowModel workflow) {
        try {
            List<TaskModel> tasks = workflow.getTasks();
            int correlationId =
                    ownerOf(workflow.getWorkflowId(), workflow.getCorrelationId()).getShardId();
            workflow.setTasks(new LinkedList<>());

            WorkflowModel prevWorkflow =
                    readWorkflow(
                                    workflow.getWorkflowId(),
                                    false,
                                    UUID.fromString(workflow.getWorkflowId()),
                                    correlationId)
                            .workflow();
            LOGGER.debug(
                    "Update workflow - getPrevious workflow status {} - current Status {} for workflowId {} and prevVersion {} ",
                    prevWorkflow.getStatus(),
                    workflow.getStatus(),
                    prevWorkflow.getWorkflowId(),
                    prevWorkflow.getVersion());

            String payload = toJson(workflow);

            if (!prevWorkflow.getStatus().equals(WorkflowModel.Status.COMPLETED)) {
                if (attemptUpdateWorkflow(
                        workflow, prevWorkflow, payload, correlationId, Boolean.FALSE)) {
                    workflow.setTasks(tasks);
                } else {
                    handleConcurrentUpdate(workflow, tasks, payload, correlationId);
                }
            }
            return workflow.getWorkflowId();
        } catch (DriverException e) {
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            handleError(workflow, e, "updateWorkflow");
            throw new TransientException(
                    "Failed to update workflow: " + workflow.getWorkflowId(), e);
        }
    }

    private boolean attemptUpdateWorkflow(
            WorkflowModel workflow,
            WorkflowModel prevWorkflow,
            String payload,
            Integer correlationId,
            Boolean isRetry) {
        Integer currentVersion = prevWorkflow.getVersion() == 0 ? null : prevWorkflow.getVersion();
        ResultSet resultSet =
                session.execute(
                        updateWorkflowStatement.bind(
                                payload,
                                prevWorkflow.getVersion() + 1,
                                UUID.fromString(workflow.getWorkflowId()),
                                correlationId,
                                currentVersion));
        if (resultSet.wasApplied()) {
            LOGGER.debug(
                    "Updated workflow with isRetry {} - current status {} for workflowId {} with version {}",
                    isRetry,
                    workflow.getStatus(),
                    workflow.getWorkflowId(),
                    prevWorkflow.getVersion() + 1);
            return true;
        }
        return false;
    }

    private void handleConcurrentUpdate(
            WorkflowModel workflow, List<TaskModel> tasks, String payload, Integer correlationId) {
        LOGGER.info(
                "Concurrent update detected, update failed for workflow: {} retrying..",
                workflow.getWorkflowId());
        WorkflowModel retriedWorkflow =
                readWorkflow(
                                workflow.getWorkflowId(),
                                false,
                                UUID.fromString(workflow.getWorkflowId()),
                                correlationId)
                        .workflow();

        if (!retriedWorkflow.getStatus().equals(WorkflowModel.Status.COMPLETED)) {
            if (attemptUpdateWorkflow(workflow, retriedWorkflow, payload, correlationId, true)) {
                workflow.setTasks(tasks);
            } else {
                LOGGER.info(
                        "Concurrent update retriedVersion detected, update failed for workflow: {} with version {}",
                        workflow.getWorkflowId(),
                        retriedWorkflow.getVersion());
            }
        }
    }

    private void handleError(WorkflowModel workflow, DriverException e, String methodName) {
        Monitors.error(CLASS_NAME, methodName);
        String errorMsg = String.format("Failed to update workflow: %s", workflow.getWorkflowId());
        LOGGER.error(errorMsg, e);
    }

    @Override
    public boolean removeWorkflow(String workflowId) {
        // Ownership-checked read: a caller that doesn't own the workflow removes nothing.
        WorkflowModel workflow = getWorkflow(workflowId, true);
        boolean removed = false;
        if (workflow != null) {
            int correlationId = ownerOf(workflowId, workflow.getCorrelationId()).getShardId();
            try {
                recordCassandraDaoRequests("removeWorkflow", "n/a", workflow.getWorkflowName());
                ResultSet resultSet =
                        session.execute(
                                deleteWorkflowStatement.bind(
                                        UUID.fromString(workflowId), correlationId));
                removed = resultSet.wasApplied();
            } catch (DriverException e) {
                Monitors.error(CLASS_NAME, "removeWorkflow");
                logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
                String errorMsg = String.format("Failed to remove workflow: %s", workflowId);
                LOGGER.error(errorMsg, e);
                throw new TransientException(errorMsg);
            }
            workflow.getTasks().forEach(this::removeTaskLookup);
            try {
                session.execute(deleteWorkflowLookupStatement.bind(UUID.fromString(workflowId)));
            } catch (DriverException e) {
                Monitors.error(CLASS_NAME, "removeWorkflow");
                String errorMsg = String.format("Failed to remove workflow lookup: %s", workflowId);
                LOGGER.error(errorMsg, e);
                throw new TransientException(errorMsg);
            }
            ownerCache.invalidate(workflowId);
        }
        return removed;
    }

    /**
     * This is a dummy implementation and this feature is not yet implemented for Cassandra backed
     * Conductor
     */
    @Override
    public boolean removeWorkflowWithExpiry(String workflowId, int ttlSeconds) {
        throw new UnsupportedOperationException(
                "This method is not currently implemented in ScyllaExecutionDAO. Please use RedisDAO mode instead now for using TTLs.");
    }

    /**
     * No-op: Scylla keeps no pending-workflows table, so there is nothing to remove. Conductor
     * calls this on every terminate; throwing here made terminate return 500 after the workflow
     * was already terminated.
     */
    @Override
    public void removeFromPendingWorkflow(String workflowType, String workflowId) {}

    @Override
    public WorkflowModel getWorkflow(String workflowId) {
        return getWorkflow(workflowId, true);
    }

    public WorkflowModel getWorkflow(String shardId, String workflowId) {
        return getWorkflow(shardId, workflowId, true);
    }

    // Not part of the Freshworks ExecutionDAO interface (Journeys-only overload)
    public WorkflowModel getWorkflow(String shardId, String workflowId, boolean includeTasks) {
        UUID workflowUUID = toUUID(workflowId, "Invalid workflow id");
        // Shard hint: read the partition directly and check its static owner. A wrong hint lands on
        // an
        // empty partition, so fall back to workflow_lookup (which also does the owner check).
        OwnedWorkflow read =
                readWorkflow(
                        workflowId, includeTasks, workflowUUID, ShardResolver.legacyShard(shardId));
        if (read.workflow() == null) {
            return getWorkflow(workflowId, includeTasks);
        }
        return visibleOrNull(read.workflow(), read.tenantId(), read.accountId());
    }

    @Override
    public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {
        UUID workflowUUID = toUUID(workflowId, "Invalid workflow id");
        WorkflowOwner owner = ownerOf(workflowId, null);
        OwnedWorkflow read =
                readWorkflow(workflowId, includeTasks, workflowUUID, owner.getShardId());
        if (read.workflow() == null) {
            return null;
        }
        String tenant = owner.getTenantId() != null ? owner.getTenantId() : read.tenantId();
        String account = owner.getTenantId() != null ? owner.getAccountId() : read.accountId();
        return visibleOrNull(read.workflow(), tenant, account);
    }

    private WorkflowModel visibleOrNull(
            WorkflowModel workflow, String ownerTenant, String ownerAccount) {
        if (callerOwns(ownerTenant, ownerAccount)) {
            return workflow;
        }
        LOGGER.info(
                "Workflow {} is not visible to {}", workflow.getWorkflowId(), TenantContext.get());
        return null;
    }

    /** Workflow row plus the partition's static owner columns; no ownership check. */
    private record OwnedWorkflow(WorkflowModel workflow, String tenantId, String accountId) {}

    private OwnedWorkflow readWorkflow(
            String workflowId, boolean includeTasks, UUID workflowUUID, int correlationId) {
        try {
            WorkflowModel workflow = null;
            String tenantId = null;
            String accountId = null;
            ResultSet resultSet;
            if (includeTasks) {
                resultSet =
                        session.execute(
                                selectWorkflowWithTasksStatement.bind(workflowUUID, correlationId));
                List<TaskModel> tasks = new ArrayList<>();

                List<Row> rows = resultSet.all();
                if (rows.size() == 0) {
                    LOGGER.info("Workflow {} not found in datastore", workflowId);
                    return new OwnedWorkflow(null, null, null);
                }
                tenantId = rows.get(0).getString(TENANT_ID_KEY);
                accountId = rows.get(0).getString(ACCOUNT_ID_KEY);
                for (Row row : rows) {
                    String entityKey = row.getString(ENTITY_KEY);
                    if (ENTITY_TYPE_WORKFLOW.equals(entityKey)) {
                        workflow = readValue(row.getString(PAYLOAD_KEY), WorkflowModel.class);
                        // Added version for version locking
                        workflow.setVersion(row.getInt(VERSION));
                    } else if (ENTITY_TYPE_TASK.equals(entityKey)) {
                        TaskModel task = readValue(row.getString(PAYLOAD_KEY), TaskModel.class);
                        tasks.add(task);
                    } else {
                        throw new NonTransientException(
                                String.format(
                                        "Invalid row with entityKey: %s found in datastore for workflow: %s",
                                        entityKey, workflowId));
                    }
                }

                if (workflow != null) {
                    recordCassandraDaoRequests("getWorkflow", "n/a", workflow.getWorkflowName());
                    tasks.sort(Comparator.comparingInt(TaskModel::getSeq));
                    workflow.setTasks(tasks);
                }
            } else {
                resultSet =
                        session.execute(selectWorkflowStatement.bind(workflowUUID, correlationId));
                Row workflowRow = resultSet.one();
                if (workflowRow != null) {
                    tenantId = workflowRow.getString(TENANT_ID_KEY);
                    accountId = workflowRow.getString(ACCOUNT_ID_KEY);
                }
                workflow =
                        Optional.ofNullable(workflowRow)
                                .map(
                                        row -> {
                                            WorkflowModel wf =
                                                    readValue(
                                                            row.getString(PAYLOAD_KEY),
                                                            WorkflowModel.class);
                                            // Added version for version locking
                                            wf.setVersion(row.getInt(VERSION));
                                            recordCassandraDaoRequests(
                                                    "getWorkflow", "n/a", wf.getWorkflowName());
                                            return wf;
                                        })
                                .orElse(null);
            }
            return new OwnedWorkflow(workflow, tenantId, accountId);
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getWorkflow");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg = String.format("Failed to get workflow: %s", workflowId);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    /**
     * This is a dummy implementation and this feature is not implemented for Cassandra backed
     * Conductor
     */
    @Override
    public List<String> getRunningWorkflowIds(String workflowName, int version) {
        throw new UnsupportedOperationException(
                "This method is not implemented in ScyllaExecutionDAO. Please use ExecutionDAOFacade instead.");
    }

    /**
     * This is a dummy implementation and this feature is not implemented for Cassandra backed
     * Conductor
     */
    @Override
    public List<WorkflowModel> getPendingWorkflowsByType(String workflowName, int version) {
        throw new UnsupportedOperationException(
                "This method is not implemented in ScyllaExecutionDAO. Please use ExecutionDAOFacade instead.");
    }

    /**
     * This is a dummy implementation and this feature is not implemented for Cassandra backed
     * Conductor
     */
    @Override
    public long getPendingWorkflowCount(String workflowName) {
        throw new UnsupportedOperationException(
                "This method is not implemented in ScyllaExecutionDAO. Please use ExecutionDAOFacade instead.");
    }

    /**
     * This is used to retrieve the total number of tasks in progress for a given task definition
     */
    @Override
    public long getInProgressTaskCount(String taskDefName) {
        return 0L;
    }

    /**
     * This is a dummy implementation and this feature is not implemented for Cassandra backed
     * Conductor
     */
    @Override
    public List<WorkflowModel> getWorkflowsByType(
            String workflowName, Long startTime, Long endTime) {
        throw new UnsupportedOperationException(
                "This method is not implemented in ScyllaExecutionDAO. Please use ExecutionDAOFacade instead.");
    }

    /** This is to get the list of workflows based on correlationId. */
    @Override
    public List<WorkflowModel> getWorkflowsByCorrelationId(
            String workflowName, String correlationId, boolean includeTasks) {
        try {
            recordCassandraDaoRequests("getWorkflowsByCorrelationId", "n/a", correlationId);

            ResultSet resultSet =
                    session.execute(
                            selectWorkflowsByCorIdFromWorkflowStatement.bind(
                                    ShardResolver.legacyShard(correlationId)));
            List<WorkflowModel> wfList =
                    resultSet.all().stream()
                            .map(
                                    row -> {
                                        WorkflowModel wf =
                                                readValue(
                                                        row.getString(PAYLOAD_KEY),
                                                        WorkflowModel.class);
                                        recordCassandraDaoRequests(
                                                "getWorkflowsByCorrelationId",
                                                "n/a",
                                                wf.getWorkflowName());
                                        return wf;
                                    })
                            .collect(Collectors.toList());
            return wfList;
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "getWorkflowsByCorrelationId");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format(
                            "Failed to retrieve workflows from correlationId: %s", correlationId);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    /**
     * false: ExecutionDAOFacade then resolves correlated workflows through the index (tenant-scoped
     * by the proxy's correlationId prefix) instead of getWorkflowsByCorrelationId, which scans the
     * table with ALLOW FILTERING and can't match non-numeric correlationIds.
     */
    @Override
    public boolean canSearchAcrossWorkflows() {
        return false;
    }

    @Override
    public boolean addEventExecution(EventExecution eventExecution) {
        try {
            String jsonPayload = toJson(eventExecution);
            recordCassandraDaoEventRequests("addEventExecution", eventExecution.getEvent());
            recordCassandraDaoPayloadSize(
                    "addEventExecution", jsonPayload.length(), eventExecution.getEvent(), "n/a");
            return session.execute(
                            insertEventExecutionStatement.bind(
                                    eventExecution.getMessageId(),
                                    eventExecution.getName(),
                                    eventExecution.getId(),
                                    jsonPayload))
                    .wasApplied();
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "addEventExecution");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format(
                            "Failed to add event execution for event: %s, handler: %s",
                            eventExecution.getEvent(), eventExecution.getName());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    @Override
    public void updateEventExecution(EventExecution eventExecution) {
        try {
            String jsonPayload = toJson(eventExecution);
            recordCassandraDaoEventRequests("updateEventExecution", eventExecution.getEvent());
            recordCassandraDaoPayloadSize(
                    "updateEventExecution", jsonPayload.length(), eventExecution.getEvent(), "n/a");
            session.execute(
                    updateEventExecutionStatement.bind(
                            eventExecutionsTTL,
                            jsonPayload,
                            eventExecution.getMessageId(),
                            eventExecution.getName(),
                            eventExecution.getId()));
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "updateEventExecution");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format(
                            "Failed to update event execution for event: %s, handler: %s",
                            eventExecution.getEvent(), eventExecution.getName());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    @Override
    public void removeEventExecution(EventExecution eventExecution) {
        try {
            recordCassandraDaoEventRequests("removeEventExecution", eventExecution.getEvent());
            session.execute(
                    deleteEventExecutionStatement.bind(
                            eventExecution.getMessageId(),
                            eventExecution.getName(),
                            eventExecution.getId()));
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "removeEventExecution");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format(
                            "Failed to remove event execution for event: %s, handler: %s",
                            eventExecution.getEvent(), eventExecution.getName());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    @VisibleForTesting
    List<EventExecution> getEventExecutions(
            String eventHandlerName, String eventName, String messageId) {
        try {
            return session
                    .execute(selectEventExecutionsStatement.bind(messageId, eventHandlerName))
                    .all()
                    .stream()
                    .filter(row -> !row.isNull(PAYLOAD_KEY))
                    .map(row -> readValue(row.getString(PAYLOAD_KEY), EventExecution.class))
                    .collect(Collectors.toList());
        } catch (DriverException e) {
            String errorMsg =
                    String.format(
                            "Failed to fetch event executions for event: %s, handler: %s",
                            eventName, eventHandlerName);
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    @Override
    public void addTaskToLimit(TaskModel task) {
        if (!isConcurrencyLimitEnabled()) {
            return;
        }
        try {
            recordCassandraDaoRequests(
                    "addTaskToLimit", task.getTaskType(), task.getWorkflowType());
            session.execute(
                    updateTaskDefLimitStatement.bind(
                            UUID.fromString(task.getWorkflowInstanceId()),
                            task.getTaskDefName(),
                            UUID.fromString(task.getTaskId())));
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "addTaskToLimit");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format(
                            "Error updating taskDefLimit for task - %s:%s in workflow: %s",
                            task.getTaskDefName(), task.getTaskId(), task.getWorkflowInstanceId());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    @Override
    public void removeTaskFromLimit(TaskModel task) {
        if (!isConcurrencyLimitEnabled()) {
            return;
        }
        try {
            recordCassandraDaoRequests(
                    "removeTaskFromLimit", task.getTaskType(), task.getWorkflowType());
            session.execute(
                    deleteTaskDefLimitStatement.bind(
                            task.getTaskDefName(), UUID.fromString(task.getTaskId())));
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "removeTaskFromLimit");
            String errorMsg =
                    String.format(
                            "Error updating taskDefLimit for task - %s:%s in workflow: %s",
                            task.getTaskDefName(), task.getTaskId(), task.getWorkflowInstanceId());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    protected boolean removeTask(TaskModel task) {
        // TODO: calculate shard number based on seq and maxTasksPerShard
        try {
            // get total tasks for this workflow
            int correlationId =
                    ownerOf(task.getWorkflowInstanceId(), task.getCorrelationId()).getShardId();
            WorkflowMetadata workflowMetadata =
                    getWorkflowMetadata(task.getWorkflowInstanceId(), correlationId);
            int totalTasks = workflowMetadata.getTotalTasks();

            // remove from task_lookup table
            removeTaskLookup(task);

            // remove the task from task_in_progress table
            removeTaskInProgress(task);

            recordCassandraDaoRequests("removeTask", task.getTaskType(), task.getWorkflowType());
            // delete task from workflows table and decrement total tasks by 1
            BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            batchStatement.add(
                    deleteTaskStatement.bind(
                            UUID.fromString(task.getWorkflowInstanceId()),
                            correlationId,
                            task.getTaskId()));
            batchStatement.add(
                    updateTotalTasksStatement.bind(
                            totalTasks - 1,
                            UUID.fromString(task.getWorkflowInstanceId()),
                            correlationId));
            ResultSet resultSet = session.execute(batchStatement);
            if (task.getTaskDefinition().isPresent()
                    && task.getTaskDefinition().get().concurrencyLimit() > 0) {
                removeTaskFromLimit(task);
            }
            return resultSet.wasApplied();
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "removeTask");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg = String.format("Failed to remove task: %s", task.getTaskId());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    protected void removeTaskLookup(TaskModel task) {
        try {
            recordCassandraDaoRequests(
                    "removeTaskLookup", task.getTaskType(), task.getWorkflowType());
            if (task.getTaskDefinition().isPresent()
                    && task.getTaskDefinition().get().concurrencyLimit() > 0) {
                removeTaskFromLimit(task);
            }
            // workflow_lookup holds the workflow's shard and owner, so it is only deleted with the
            // workflow (removeWorkflow), never with a single task.
            session.execute(deleteTaskLookupStatement.bind(UUID.fromString(task.getTaskId())));
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "removeTaskLookup");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg = String.format("Failed to remove task lookup: %s", task.getTaskId());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg);
        }
    }

    @VisibleForTesting
    void validateTasks(List<TaskModel> tasks) {
        Preconditions.checkNotNull(tasks, "Tasks object cannot be null");
        Preconditions.checkArgument(!tasks.isEmpty(), "Tasks object cannot be empty");
        tasks.forEach(
                task -> {
                    Preconditions.checkNotNull(task, "task object cannot be null");
                    Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
                    Preconditions.checkNotNull(
                            task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
                    Preconditions.checkNotNull(
                            task.getReferenceTaskName(), "Task reference name cannot be null");
                });

        String workflowId = tasks.get(0).getWorkflowInstanceId();
        Optional<TaskModel> optionalTask =
                tasks.stream()
                        .filter(task -> !workflowId.equals(task.getWorkflowInstanceId()))
                        .findAny();
        if (optionalTask.isPresent()) {
            throw new NonTransientException(
                    "Tasks of multiple workflows cannot be created/updated simultaneously");
        }
    }

    @VisibleForTesting
    WorkflowMetadata getWorkflowMetadata(String workflowId, int correlationId) {
        ResultSet resultSet =
                session.execute(
                        selectTotalStatement.bind(UUID.fromString(workflowId), correlationId));
        recordCassandraDaoRequests("getWorkflowMetadata");
        return Optional.ofNullable(resultSet.one())
                .map(
                        row -> {
                            WorkflowMetadata workflowMetadata = new WorkflowMetadata();
                            workflowMetadata.setTotalTasks(row.getInt(TOTAL_TASKS_KEY));
                            workflowMetadata.setTotalPartitions(row.getInt(TOTAL_PARTITIONS_KEY));
                            return workflowMetadata;
                        })
                .orElseThrow(
                        () ->
                                new NotFoundException(
                                        "Workflow with id: %s not found in data store",
                                        workflowId));
    }

    @VisibleForTesting
    String lookupWorkflowIdFromTaskId(String taskId) {
        UUID taskUUID = toUUID(taskId, "Invalid task id");
        try {
            ResultSet resultSet = session.execute(selectTaskLookupStatement.bind(taskUUID));
            return Optional.ofNullable(resultSet.one())
                    .map(row -> row.getUUID(WORKFLOW_ID_KEY).toString())
                    .orElse(null);
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "lookupWorkflowIdFromTaskId");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg = String.format("Failed to lookup workflowId from taskId: %s", taskId);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    /**
     * @return method to get the shardId from task_lookup table for shard_mapping
     */
    @VisibleForTesting
    String lookupShardIdFromTaskId(String taskId) {
        UUID taskUUID = toUUID(taskId, "Invalid task id");
        try {
            ResultSet resultSet =
                    session.execute(selectShardFromTaskLookupStatement.bind(taskUUID));
            return Optional.ofNullable(resultSet.one())
                    .map(row -> String.valueOf(row.getInt(SHARD_ID_KEY)))
                    .orElse(null);
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "lookupShardIdFromTaskId");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg = String.format("Failed to lookup shardId from taskId: %s", taskId);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    /**
     * @return shard and owner from the workflow_lookup row, or null when there is none
     */
    @VisibleForTesting
    WorkflowOwner lookupOwnerFromWorkflowId(String workflowId) {
        UUID workflowUUID = toUUID(workflowId, "Invalid workflow id");
        try {
            ResultSet resultSet =
                    session.execute(selectShardFromWorkflowLookupStatement.bind(workflowUUID));

            return Optional.ofNullable(resultSet.one())
                    .map(
                            row ->
                                    new WorkflowOwner(
                                            row.getInt(SHARD_ID_KEY),
                                            row.getString(TENANT_ID_KEY),
                                            row.getString(ACCOUNT_ID_KEY)))
                    .orElse(null);
        } catch (DriverException e) {
            Monitors.error(CLASS_NAME, "lookupShardIdFromWorkflowId");
            logErrorToDebug(Thread.currentThread().getStackTrace()[2].getMethodName(), e);
            String errorMsg =
                    String.format("Failed to lookup shardId from workflowId: %s", workflowId);
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    /**
     * Shard and owner of a workflow: cache, then workflow_lookup. A workflow with no lookup row was
     * written by the old code; it falls back to the old rule (numeric correlationId, else 0) and is
     * not cached, so a later lookup row is still picked up.
     */
    WorkflowOwner ownerOf(String workflowId, String legacyCorrelationId) {
        WorkflowOwner cached = ownerCache.getIfPresent(workflowId);
        if (cached != null) {
            return cached;
        }
        WorkflowOwner owner = lookupOwnerFromWorkflowId(workflowId);
        if (owner == null) {
            return new WorkflowOwner(ShardResolver.legacyShard(legacyCorrelationId), null, null);
        }
        ownerCache.put(workflowId, owner);
        return owner;
    }

    /**
     * Owner of a new workflow: the caller's headers, else the parent workflow's owner
     * (sub-workflows), else input._tenantContext, else conductor.scylla.default-tenant. With no
     * tenant at all the workflow gets no owner and the legacy shard, as before.
     */
    private WorkflowOwner resolveOwner(WorkflowModel workflow) {
        String tenantId = null;
        String accountId = null;
        TenantContext context = TenantContext.get();
        if (context != null) {
            tenantId = context.getTenantId();
            accountId = context.getAccountId();
        } else if (StringUtils.isNotBlank(workflow.getParentWorkflowId())) {
            WorkflowOwner parent = ownerOf(workflow.getParentWorkflowId(), null);
            tenantId = parent.getTenantId();
            accountId = parent.getAccountId();
        }
        if (StringUtils.isBlank(tenantId)) {
            Object tenantContext =
                    workflow.getInput() == null ? null : workflow.getInput().get("_tenantContext");
            if (tenantContext instanceof Map<?, ?> map) {
                tenantId = Objects.toString(map.get("tenantID"), null);
                accountId = Objects.toString(map.get("accountID"), null);
            }
        }
        if (StringUtils.isBlank(tenantId)) {
            tenantId = properties.getDefaultTenant();
        }
        if (StringUtils.isBlank(tenantId)) {
            return new WorkflowOwner(
                    ShardResolver.legacyShard(workflow.getCorrelationId()), null, null);
        }
        return ShardResolver.resolve(tenantId.trim(), accountId, workflow.getCorrelationId());
    }

    /** No caller context (internal work, or a request without X-Tenant-ID) is trusted. */
    private static boolean callerOwns(String ownerTenant, String ownerAccount) {
        TenantContext context = TenantContext.get();
        return context == null || context.owns(ownerTenant, ownerAccount);
    }

    public boolean isConcurrencyLimitEnabled() {
        return isConcurrencyLimitEnabled;
    }

    public void setConcurrencyLimitEnabled(boolean concurrencyLimitEnabled) {
        this.isConcurrencyLimitEnabled = concurrencyLimitEnabled;
    }

    public boolean isFallbackToOldTaskInProgressActive() {
        return fallbackToOldTaskInProgressActive;
    }

    public void setFallbackToOldTaskInProgressActive(boolean fallbackToOldTaskInProgressActive) {
        this.fallbackToOldTaskInProgressActive = fallbackToOldTaskInProgressActive;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        // Freshworks redis-lock registers the bean as "provideLock" with declared type Lock, so a
        // by-type lookup for RedisLock fails until it is instantiated; resolve via Lock and cast.
        this.redisLock = (RedisLock) applicationContext.getBean(Lock.class);
        setConcurrencyLimitEnabled(
                Boolean.parseBoolean(
                        applicationContext
                                .getEnvironment()
                                .getProperty("concurrencyLimitEnabled", "false")));
        setFallbackToOldTaskInProgressActive(
                Boolean.parseBoolean(
                        applicationContext
                                .getEnvironment()
                                .getProperty("fallbackToOldTaskInProgressActive", "false")));
    }
}
