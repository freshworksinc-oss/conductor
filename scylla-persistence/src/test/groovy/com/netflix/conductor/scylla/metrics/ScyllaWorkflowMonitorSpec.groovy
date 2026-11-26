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
package com.netflix.conductor.scylla.metrics

import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask
import com.netflix.conductor.dao.ExecutionDAO
import com.netflix.conductor.dao.MetadataDAO
import com.netflix.conductor.dao.QueueDAO
import com.netflix.conductor.metrics.Monitors

import spock.lang.Specification

class ScyllaWorkflowMonitorSpec extends Specification {

    /** What the Scylla metadata DAOs are: a MetadataDAO that can also project metric info. */
    interface ScyllaMetadata extends MetadataDAO, WorkflowMetricInfoSource {}

    ScyllaMetadata metadataDAO = Mock()
    ExecutionDAO executionDAO = Mock()
    QueueDAO queueDAO = Mock()

    private ScyllaWorkflowMonitor monitor(MetadataDAO dao = metadataDAO, Set<WorkflowSystemTask> system = Set.of()) {
        new ScyllaWorkflowMonitor(dao, executionDAO, queueDAO, 1000, system, false)
    }

    private static TaskDef taskDef(String name, String owner, int limit) {
        def def1 = new TaskDef(name)
        def1.ownerApp = owner
        def1.concurrentExecLimit = limit
        def1
    }

    private static double gauge(String name, String... tags) {
        Monitors.getGauge(name, tags).get()
    }

    def "one running count per workflow name; full definitions are never loaded"() {
        given:
        metadataDAO.getWorkflowMetricInfo() >> [new WorkflowMetricInfo("swm_wf1", "owner1"),
                                                new WorkflowMetricInfo("swm_wf2", "owner2")]
        metadataDAO.getAllTaskDefs() >> []

        when:
        monitor().reportMetrics()

        then:
        1 * executionDAO.getPendingWorkflowCount("swm_wf1") >> 3
        1 * executionDAO.getPendingWorkflowCount("swm_wf2") >> 0
        0 * metadataDAO.getAllWorkflowDefs()
        0 * metadataDAO.getAllWorkflowDefsLatestVersions()
        gauge("workflow_running", "workflowName", "swm_wf1", "ownerApp", "owner1") == 3
        gauge("workflow_running", "workflowName", "swm_wf2", "ownerApp", "owner2") == 0
    }

    def "queue depth for every task def; in-progress only where a concurrency limit is set"() {
        given:
        metadataDAO.getWorkflowMetricInfo() >> []
        metadataDAO.getAllTaskDefs() >> [taskDef("swm_limited", "o", 5), taskDef("swm_unlimited", "o", 0)]
        queueDAO.getSize("swm_limited") >> 4
        queueDAO.getSize("swm_unlimited") >> 7

        when:
        monitor().reportMetrics()

        then:
        1 * executionDAO.getInProgressTaskCount("swm_limited") >> 2
        0 * executionDAO.getInProgressTaskCount("swm_unlimited")
        gauge("task_queue_depth", "taskType", "swm_limited", "ownerApp", "o") == 4
        gauge("task_queue_depth", "taskType", "swm_unlimited", "ownerApp", "o") == 7
        gauge("task_in_progress", "taskType", "swm_limited", "ownerApp", "o") == 2
    }

    def "async system tasks: queue depth and in-progress with ownerApp=system"() {
        given:
        metadataDAO.getWorkflowMetricInfo() >> []
        metadataDAO.getAllTaskDefs() >> []
        WorkflowSystemTask systemTask = Mock() { getTaskType() >> "SWM_SYS" }
        queueDAO.getSize("SWM_SYS") >> 9
        executionDAO.getInProgressTaskCount("SWM_SYS") >> 1

        when:
        monitor(metadataDAO, Set.of(systemTask)).reportMetrics()

        then:
        gauge("task_queue_depth", "taskType", "SWM_SYS", "ownerApp", "system") == 9
        gauge("task_in_progress", "taskType", "SWM_SYS", "ownerApp", "system") == 1
    }

    def "metadata is read once per refresh interval"() {
        given:
        def m = monitor()

        when:
        m.reportMetrics()
        m.reportMetrics()

        then: "metadataRefreshInterval is 1000, so the second run reuses the first read"
        1 * metadataDAO.getWorkflowMetricInfo() >> []
        1 * metadataDAO.getAllTaskDefs() >> []
    }

    def "an empty catalog publishes nothing"() {
        given:
        metadataDAO.getWorkflowMetricInfo() >> []
        metadataDAO.getAllTaskDefs() >> []

        when:
        monitor().reportMetrics()

        then:
        0 * executionDAO.getPendingWorkflowCount(_)
        0 * queueDAO.getSize(_)
    }

    def "one failing count only skips that gauge"() {
        given:
        metadataDAO.getWorkflowMetricInfo() >> [new WorkflowMetricInfo("swm_bad", "o"),
                                                new WorkflowMetricInfo("swm_good", "o")]
        metadataDAO.getAllTaskDefs() >> [taskDef("swm_task_bad", "o", 0), taskDef("swm_task_good", "o", 0)]
        executionDAO.getPendingWorkflowCount("swm_bad") >> { throw new RuntimeException("scylla timeout") }
        executionDAO.getPendingWorkflowCount("swm_good") >> 5
        queueDAO.getSize("swm_task_bad") >> { throw new RuntimeException("redis timeout") }
        queueDAO.getSize("swm_task_good") >> 6

        when:
        monitor().reportMetrics()

        then:
        noExceptionThrown()
        gauge("workflow_running", "workflowName", "swm_good", "ownerApp", "o") == 5
        gauge("task_queue_depth", "taskType", "swm_task_good", "ownerApp", "o") == 6
    }

    def "a failed metadata refresh is retried on the next run"() {
        given:
        def m = monitor()

        when:
        m.reportMetrics()
        m.reportMetrics()

        then:
        2 * metadataDAO.getWorkflowMetricInfo() >> { throw new RuntimeException("down") } >> []
        _ * metadataDAO.getAllTaskDefs() >> []
        noExceptionThrown()
    }

    def "a metadata DAO that is not the Scylla one: latest versions are projected"() {
        given:
        MetadataDAO plain = Mock()
        def wf = new WorkflowDef(name: "swm_plain_wf", version: 3, ownerApp: "plain_owner")
        plain.getAllTaskDefs() >> []

        when:
        monitor(plain).reportMetrics()

        then:
        1 * plain.getAllWorkflowDefsLatestVersions() >> [wf]
        0 * plain.getAllWorkflowDefs()
        1 * executionDAO.getPendingWorkflowCount("swm_plain_wf") >> 2
        gauge("workflow_running", "workflowName", "swm_plain_wf", "ownerApp", "plain_owner") == 2
    }
}
