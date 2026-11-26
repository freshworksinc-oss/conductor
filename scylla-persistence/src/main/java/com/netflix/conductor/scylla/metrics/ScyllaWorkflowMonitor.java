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
package com.netflix.conductor.scylla.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;

/**
 * Publishes the WorkflowMonitor metrics (workflow_running, task_queue_depth, task_in_progress) on
 * Scylla without loading every workflow definition.
 *
 * <p>Same metric names, labels and schedule as core WorkflowMonitor, which reads every version of
 * every definition on each metadata refresh and keeps them all until the next one. This monitor
 * keeps one small record per workflow name and per task definition:
 *
 * <ul>
 *   <li>workflows: name and ownerApp of the latest version, from the Scylla definition index and an
 *       ownerApp-only parse ({@link WorkflowMetricInfoSource})
 *   <li>tasks: name, ownerApp and concurrency limit from getAllTaskDefs() (the task-def cache)
 *   <li>task_in_progress is queried only where it is published: task defs with a concurrency limit
 *       and async system tasks
 *   <li>a failure for one workflow or task skips only that gauge for the run
 * </ul>
 *
 * <p>Enabled by conductor.scylla.workflow-monitor.enabled=true. Turn core WorkflowMonitor off on
 * the same pod (conductor.workflow-monitor.enabled=false), otherwise it still loads every
 * definition.
 */
public class ScyllaWorkflowMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaWorkflowMonitor.class);

    /** The task-definition fields the gauges need. */
    record TaskMetricInfo(String name, String ownerApp, int concurrencyLimit) {}

    private final MetadataDAO metadataDAO;
    private final ExecutionDAO executionDAO;
    private final QueueDAO queueDAO;
    private final int metadataRefreshInterval;
    private final Set<WorkflowSystemTask> asyncSystemTasks;

    private List<WorkflowMetricInfo> workflows = List.of();
    private List<TaskMetricInfo> tasks = List.of();
    private int refreshCounter = 0;

    public ScyllaWorkflowMonitor(
            MetadataDAO metadataDAO,
            ExecutionDAO executionDAO,
            QueueDAO queueDAO,
            int metadataRefreshInterval,
            Set<WorkflowSystemTask> asyncSystemTasks,
            boolean coreWorkflowMonitorEnabled) {
        this.metadataDAO = metadataDAO;
        this.executionDAO = executionDAO;
        this.queueDAO = queueDAO;
        this.metadataRefreshInterval = metadataRefreshInterval;
        this.asyncSystemTasks = asyncSystemTasks == null ? Set.of() : asyncSystemTasks;
        if (coreWorkflowMonitorEnabled) {
            LOGGER.warn(
                    "conductor.workflow-monitor.enabled is true next to {}: both publish the same"
                            + " metrics and core WorkflowMonitor still loads every workflow"
                            + " definition. Set conductor.workflow-monitor.enabled=false.",
                    ScyllaWorkflowMonitor.class.getSimpleName());
        }
        LOGGER.info("{} initialized.", ScyllaWorkflowMonitor.class.getSimpleName());
    }

    @Scheduled(
            initialDelayString = "${conductor.workflow-monitor.stats.initial-delay:120000}",
            fixedDelayString = "${conductor.workflow-monitor.stats.delay:60000}")
    public void reportMetrics() {
        if (refreshCounter <= 0) {
            try {
                workflows = loadWorkflows();
                tasks = loadTasks();
                refreshCounter = metadataRefreshInterval;
            } catch (Exception e) {
                // Keep publishing from what was read last time; retry the refresh next run.
                LOGGER.error("Error while refreshing metadata for workflow metrics", e);
            }
        }
        refreshCounter--;

        int failures = 0;
        for (WorkflowMetricInfo workflow : workflows) {
            try {
                long count = executionDAO.getPendingWorkflowCount(workflow.name());
                Monitors.recordRunningWorkflows(count, workflow.name(), workflow.ownerApp());
            } catch (Exception e) {
                failures++;
                LOGGER.debug("workflow_running failed for {}", workflow.name(), e);
            }
        }
        for (TaskMetricInfo task : tasks) {
            try {
                Monitors.recordQueueDepth(
                        task.name(), queueDAO.getSize(task.name()), task.ownerApp());
                if (task.concurrencyLimit() > 0) {
                    long inProgress = executionDAO.getInProgressTaskCount(task.name());
                    Monitors.recordTaskInProgress(task.name(), inProgress, task.ownerApp());
                }
            } catch (Exception e) {
                failures++;
                LOGGER.debug("task metrics failed for {}", task.name(), e);
            }
        }
        for (WorkflowSystemTask systemTask : asyncSystemTasks) {
            String taskType = systemTask.getTaskType();
            try {
                Monitors.recordQueueDepth(taskType, queueDAO.getSize(taskType), "system");
                Monitors.recordTaskInProgress(
                        taskType, executionDAO.getInProgressTaskCount(taskType), "system");
            } catch (Exception e) {
                failures++;
                LOGGER.debug("task metrics failed for system task {}", taskType, e);
            }
        }
        if (failures > 0) {
            LOGGER.warn(
                    "Workflow metrics: {} of {} gauges failed this run (debug log has each one)",
                    failures,
                    workflows.size() + tasks.size() + asyncSystemTasks.size());
        }
    }

    /** One entry per workflow name, without keeping definitions. */
    private List<WorkflowMetricInfo> loadWorkflows() {
        if (metadataDAO instanceof WorkflowMetricInfoSource source) {
            return source.getWorkflowMetricInfo();
        }
        // Not the Scylla metadata DAO: project the latest versions and drop them.
        List<WorkflowDef> latest = metadataDAO.getAllWorkflowDefsLatestVersions();
        List<WorkflowMetricInfo> out = new ArrayList<>(latest.size());
        latest.forEach(def -> out.add(new WorkflowMetricInfo(def.getName(), def.getOwnerApp())));
        return out;
    }

    private List<TaskMetricInfo> loadTasks() {
        List<TaskDef> taskDefs = metadataDAO.getAllTaskDefs();
        List<TaskMetricInfo> out = new ArrayList<>(taskDefs.size());
        taskDefs.forEach(
                def ->
                        out.add(
                                new TaskMetricInfo(
                                        def.getName(), def.getOwnerApp(), def.concurrencyLimit())));
        return out;
    }
}
