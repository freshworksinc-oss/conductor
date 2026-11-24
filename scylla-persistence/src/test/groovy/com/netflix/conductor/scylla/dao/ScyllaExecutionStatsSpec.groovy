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
package com.netflix.conductor.scylla.dao

import java.time.Duration

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.spock.Testcontainers

import com.netflix.conductor.common.config.ObjectMapperProvider
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import com.netflix.conductor.model.TaskModel
import com.netflix.conductor.model.WorkflowModel
import com.netflix.conductor.scylla.config.ScyllaProperties
import com.netflix.conductor.scylla.util.Statements

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.Session
import spock.lang.Shared
import spock.lang.Specification

/**
 * getPendingWorkflowCount / getInProgressTaskCount against a real Scylla node: the counts behind
 * the workflow_running and task_in_progress metrics, maintained like the Postgres DAO.
 */
@Testcontainers
class ScyllaExecutionStatsSpec extends Specification {

    static final String KEYSPACE = "stats_junit"

    @Shared
    GenericContainer scylla = new GenericContainer("scylladb/scylla:latest")
            .withCommand("--smp", "1", "--memory", "512M", "--overprovisioned", "1",
                    "--developer-mode", "1")
            .withExposedPorts(9042)
            .waitingFor(Wait.forLogMessage(".*Starting listening for CQL clients.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)))

    @Shared
    Session session

    @Shared
    ScyllaProperties properties

    @Shared
    ScyllaExecutionDAO dao

    def setupSpec() {
        Cluster cluster = Cluster.builder()
                .withoutJMXReporting()
                .addContactPoint(scylla.host)
                .withPort(scylla.getMappedPort(9042))
                .build()
        session = cluster.connect()
        properties = new ScyllaProperties()
        properties.keyspace = KEYSPACE
        // tablets-enabled Scylla rejects SimpleStrategy
        properties.replicationStrategy = "NetworkTopologyStrategy"
        properties.replicationFactorKey = "datacenter1"
        properties.replicationFactorValue = 1
        properties.readConsistencyLevel = ConsistencyLevel.ONE
        properties.writeConsistencyLevel = ConsistencyLevel.ONE
        dao = new ScyllaExecutionDAO(session, new ObjectMapperProvider().objectMapper,
                properties, new Statements(KEYSPACE))
    }

    def cleanupSpec() {
        session?.cluster?.close()
    }

    def setup() {
        properties.queueMetricsEnabled = true
    }

    private static WorkflowModel workflow(String type, WorkflowModel.Status status) {
        WorkflowModel workflow = new WorkflowModel()
        workflow.workflowId = UUID.randomUUID().toString()
        workflow.workflowDefinition = new WorkflowDef(name: type, version: 1)
        workflow.status = status
        workflow.input = [:]
        return workflow
    }

    private static TaskModel task(String taskDefName) {
        TaskModel task = new TaskModel()
        task.taskId = UUID.randomUUID().toString()
        task.taskDefName = taskDefName
        task.taskType = taskDefName
        task.workflowInstanceId = UUID.randomUUID().toString()
        task.status = TaskModel.Status.IN_PROGRESS
        return task
    }

    def "pending workflow count follows create, terminal update, terminate and remove"() {
        given:
        def first = workflow("stats_wf", WorkflowModel.Status.RUNNING)
        def second = workflow("stats_wf", WorkflowModel.Status.RUNNING)
        def third = workflow("stats_wf", WorkflowModel.Status.RUNNING)
        def otherType = workflow("stats_other_wf", WorkflowModel.Status.RUNNING)

        when: "workflows are created"
        [first, second, third, otherType].each { dao.createWorkflow(it) }

        then:
        dao.getPendingWorkflowCount("stats_wf") == 3
        dao.getPendingWorkflowCount("stats_other_wf") == 1

        when: "a running update is idempotent and a terminal update removes the row"
        dao.updateWorkflow(first)
        first.status = WorkflowModel.Status.COMPLETED
        dao.updateWorkflow(first)

        then:
        dao.getPendingWorkflowCount("stats_wf") == 2

        when: "terminate path and workflow removal"
        dao.removeFromPendingWorkflow("stats_wf", second.workflowId)
        dao.removeWorkflow(third.workflowId)

        then:
        dao.getPendingWorkflowCount("stats_wf") == 0
        dao.getPendingWorkflowCount("stats_other_wf") == 1
        dao.getPendingWorkflowCount("stats_unknown_wf") == 0
    }

    def "workflow created before tracking existed is counted on its next running update"() {
        given:
        def legacy = workflow("stats_legacy_wf", WorkflowModel.Status.RUNNING)
        properties.queueMetricsEnabled = false
        dao.createWorkflow(legacy)
        properties.queueMetricsEnabled = true

        expect:
        dao.getPendingWorkflowCount("stats_legacy_wf") == 0

        when:
        dao.updateWorkflow(legacy)

        then:
        dao.getPendingWorkflowCount("stats_legacy_wf") == 1
    }

    def "in-progress task count is tracked without enforcing concurrency limits"() {
        given:
        def a = task("stats_task")
        def b = task("stats_task")

        expect:
        !dao.isConcurrencyLimitEnabled()

        when:
        dao.addTaskToLimit(a)
        dao.addTaskToLimit(b)
        dao.addTaskToLimit(a)

        then:
        dao.getInProgressTaskCount("stats_task") == 2

        when:
        dao.removeTaskFromLimit(a)

        then:
        dao.getInProgressTaskCount("stats_task") == 1
        !dao.exceedsLimit(b)
    }

    def "tracking disabled restores the previous behaviour"() {
        given:
        properties.queueMetricsEnabled = false

        when:
        dao.getPendingWorkflowCount("stats_wf")

        then:
        thrown(UnsupportedOperationException)
        dao.getInProgressTaskCount("stats_task") == 0
    }

    def "removing a workflow with an IN_PROGRESS task clears its in-progress row"() {
        given:
        def running = workflow("stats_removed_wf", WorkflowModel.Status.RUNNING)
        dao.createWorkflow(running)
        def task = task("stats_removed_task")
        task.workflowInstanceId = running.workflowId
        task.referenceTaskName = "t1"
        task.workflowType = "stats_removed_wf"
        dao.createTasks([task])
        dao.addTaskToLimit(task)

        expect:
        dao.getInProgressTaskCount("stats_removed_task") == 1

        when:
        dao.removeWorkflow(running.workflowId)

        then:
        dao.getInProgressTaskCount("stats_removed_task") == 0
        dao.getPendingWorkflowCount("stats_removed_wf") == 0
    }

    def "workflow metric info has the latest owner of each name, read from Scylla"() {
        given: "two versions of one workflow with different owners"
        def metadataDAO = new ScyllaMetadataDAO(session, new ObjectMapperProvider().objectMapper,
                properties, new Statements(KEYSPACE))
        def v1 = new WorkflowDef(name: "stats_metric_wf", version: 1, ownerApp: "owner_v1",
                tasks: [new WorkflowTask(name: "t", taskReferenceName: "t_ref")])
        def v2 = new WorkflowDef(name: "stats_metric_wf", version: 2, ownerApp: "owner_v2",
                tasks: [new WorkflowTask(name: "t", taskReferenceName: "t_ref")])
        metadataDAO.createWorkflowDef(v1)
        metadataDAO.createWorkflowDef(v2)

        when:
        def byName = metadataDAO.getWorkflowMetricInfo().collectEntries { [(it.name()): it] }

        then: "one entry for the name, with the owner of the latest version"
        byName["stats_metric_wf"].ownerApp() == "owner_v2"
        metadataDAO.getWorkflowMetricInfo().count { it.name() == "stats_metric_wf" } == 1

        when: "the latest definitions were already read and cached with the index"
        metadataDAO.getAllWorkflowDefsLatestVersions()

        then: "the cached definitions give the same answer"
        metadataDAO.getWorkflowMetricInfo().find { it.name() == "stats_metric_wf" }.ownerApp() == "owner_v2"

        cleanup:
        metadataDAO?.removeWorkflowDef("stats_metric_wf", 2)
        metadataDAO?.removeWorkflowDef("stats_metric_wf", 1)
    }
}
