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

import com.netflix.conductor.common.metadata.tasks.PollData
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask
import com.netflix.conductor.dao.MetadataDAO
import com.netflix.conductor.dao.PollDataDAO
import com.netflix.conductor.dao.QueueDAO
import com.netflix.conductor.metrics.Monitors

import spock.lang.Specification

class ScyllaQueueDepthMonitorSpec extends Specification {

    MetadataDAO metadataDAO = Mock()
    PollDataDAO pollDataDAO = Mock()
    QueueDAO queueDAO = Mock()

    private static Double depth(String taskType, String domain) {
        Monitors.registry.find(ScyllaQueueDepthMonitor.METRIC)
                .tags("taskType", taskType, "domain", domain)
                .gauge()?.value()
    }

    private static PollData pollData(String taskType, String domain) {
        new PollData(taskType, domain, "worker", 1L)
    }

    def "publishes the no-domain queue and every tenant domain queue"() {
        given:
        def taskDef = new TaskDef("qd_email_task")
        taskDef.ownerApp = "freshservice"
        metadataDAO.getAllTaskDefs() >> [taskDef]
        pollDataDAO.getAllPollData() >> [
                pollData("qd_email_task", "freshservice"),
                pollData("qd_email_task", null),
        ]
        // a domain queue with pending tasks but no poller yet; hyphenated tenant domain
        queueDAO.queuesDetail() >> ["workflow-automator-service:qd_email_task": 3L,
                                    "qd_ns_task@namespace"                  : 1L,
                                    "_deciderQueue"                         : 9L]
        queueDAO.getSize("qd_email_task") >> 0
        queueDAO.getSize("freshservice:qd_email_task") >> 42
        queueDAO.getSize("workflow-automator-service:qd_email_task") >> 3

        when:
        new ScyllaQueueDepthMonitor(metadataDAO, pollDataDAO, queueDAO, Set.of()).reportQueueDepth()

        then:
        depth("qd_email_task", Monitors.NO_DOMAIN) == 0d
        depth("qd_email_task", "freshservice") == 42d
        depth("qd_email_task", "workflow-automator-service") == 3d
        Monitors.registry.find(ScyllaQueueDepthMonitor.METRIC)
                .tags("domain", "freshservice", "ownerApp", "freshservice").gauge() != null
    }

    def "async system tasks are reported with ownerApp=system"() {
        given:
        def systemTask = new WorkflowSystemTask("QD_SYS_TASK") {}
        metadataDAO.getAllTaskDefs() >> []
        pollDataDAO.getAllPollData() >> []
        queueDAO.queuesDetail() >> ["sip:QD_SYS_TASK": 5L]
        queueDAO.getSize("QD_SYS_TASK") >> 1
        queueDAO.getSize("sip:QD_SYS_TASK") >> 5

        when:
        new ScyllaQueueDepthMonitor(metadataDAO, pollDataDAO, queueDAO, Set.of(systemTask))
                .reportQueueDepth()

        then:
        depth("QD_SYS_TASK", Monitors.NO_DOMAIN) == 1d
        Monitors.registry.find(ScyllaQueueDepthMonitor.METRIC)
                .tags("taskType", "QD_SYS_TASK", "domain", "sip", "ownerApp", "system")
                .gauge()?.value() == 5d
    }

    def "a failing poll data source still reports queues known to this server"() {
        given:
        def taskDef = new TaskDef("qd_fallback_task")
        metadataDAO.getAllTaskDefs() >> [taskDef]
        pollDataDAO.getAllPollData() >> { throw new RuntimeException("scylla down") }
        queueDAO.queuesDetail() >> ["sip:qd_fallback_task": 2L]
        queueDAO.getSize("sip:qd_fallback_task") >> 2

        when:
        new ScyllaQueueDepthMonitor(metadataDAO, pollDataDAO, queueDAO, Set.of()).reportQueueDepth()

        then:
        noExceptionThrown()
        depth("qd_fallback_task", "sip") == 2d
        depth("qd_fallback_task", Monitors.NO_DOMAIN) == 0d
    }
}
