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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.TaskMetricInfo;
import com.netflix.conductor.metrics.Monitors;

/**
 * Publishes {@value #METRIC} — pending tasks per queue, including domain queues.
 *
 * <p>The core task_queue_depth gauge only measures the queue without a domain. When workflows are
 * started with taskToDomain (the tenant auth proxy forces {@code "*" -> <tenant>}), tasks sit in
 * "&lt;domain&gt;:&lt;taskType&gt;" queues and task_queue_depth stays at 0. This gauge reports
 * every queue with a {@code domain} tag ({@link Monitors#NO_DOMAIN} for the queue without one). It
 * is a separate metric name because Prometheus requires all series of one name to share tag keys.
 *
 * <p>Domains come from persisted poll data (shared by all servers) plus domain queues this server
 * has used (covers queues whose workers have never polled). Every server running it publishes the
 * same values, so aggregate with max() across pods.
 */
public class ScyllaQueueDepthMonitor {

    public static final String METRIC = "task_queue_depth_by_domain";

    private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaQueueDepthMonitor.class);

    private final MetadataDAO metadataDAO;
    private final PollDataDAO pollDataDAO;
    private final QueueDAO queueDAO;
    private final Set<WorkflowSystemTask> asyncSystemTasks;

    public ScyllaQueueDepthMonitor(
            MetadataDAO metadataDAO,
            PollDataDAO pollDataDAO,
            QueueDAO queueDAO,
            Set<WorkflowSystemTask> asyncSystemTasks) {
        this.metadataDAO = metadataDAO;
        this.pollDataDAO = pollDataDAO;
        this.queueDAO = queueDAO;
        this.asyncSystemTasks = asyncSystemTasks == null ? Set.of() : asyncSystemTasks;
    }

    @Scheduled(
            initialDelayString = "${conductor.workflow-monitor.stats.initial-delay:120000}",
            fixedDelayString = "${conductor.workflow-monitor.stats.delay:60000}")
    public void reportQueueDepth() {
        Map<String, Set<String>> domainsByTask = getQueueDomainsByTaskType();
        try {
            for (TaskMetricInfo task : metadataDAO.getTaskMetricInfo()) {
                report(task.name(), task.ownerApp(), domainsByTask);
            }
        } catch (Exception e) {
            LOGGER.error("Error while publishing {} for task definitions", METRIC, e);
        }
        for (WorkflowSystemTask systemTask : asyncSystemTasks) {
            report(systemTask.getTaskType(), "system", domainsByTask);
        }
    }

    private void report(String taskType, String ownerApp, Map<String, Set<String>> domainsByTask) {
        try {
            record(taskType, Monitors.NO_DOMAIN, queueDAO.getSize(taskType), ownerApp);
            for (String domain : domainsByTask.getOrDefault(taskType, Set.of())) {
                String queueName = QueueUtils.getQueueName(taskType, domain, null, null);
                record(taskType, domain, queueDAO.getSize(queueName), ownerApp);
            }
        } catch (Exception e) {
            LOGGER.error("Error while publishing {} for {}", METRIC, taskType, e);
        }
    }

    private static void record(String taskType, String domain, long size, String ownerApp) {
        Monitors.getGauge(
                        METRIC,
                        "taskType",
                        taskType,
                        "domain",
                        domain,
                        "ownerApp",
                        StringUtils.defaultIfBlank(ownerApp, "unknown"))
                .set(size);
    }

    /** taskType -> domains with a queue. A failure in either source only drops that source. */
    Map<String, Set<String>> getQueueDomainsByTaskType() {
        Map<String, Set<String>> domainsByTask = new HashMap<>();
        try {
            for (PollData pollData : pollDataDAO.getAllPollData()) {
                if (pollData != null
                        && StringUtils.isNotBlank(pollData.getQueueName())
                        && StringUtils.isNotBlank(pollData.getDomain())) {
                    domainsByTask
                            .computeIfAbsent(pollData.getQueueName(), k -> new HashSet<>())
                            .add(pollData.getDomain());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Could not read poll data for {}", METRIC, e);
        }
        try {
            for (String queueName : queueDAO.queuesDetail().keySet()) {
                if (queueName == null || queueName.contains("@")) {
                    continue; // execution-namespace queues are not domain queues
                }
                int sep = queueName.indexOf(QueueUtils.DOMAIN_SEPARATOR);
                if (sep > 0 && sep < queueName.length() - 1) {
                    domainsByTask
                            .computeIfAbsent(queueName.substring(sep + 1), k -> new HashSet<>())
                            .add(queueName.substring(0, sep));
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Could not list queues for {}", METRIC, e);
        }
        return domainsByTask;
    }
}
