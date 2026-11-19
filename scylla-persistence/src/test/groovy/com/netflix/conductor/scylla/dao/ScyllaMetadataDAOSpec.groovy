/*
 * Copyright 2022 Netflix, Inc.
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

import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef

import spock.lang.Subject

class ScyllaMetadataDAOSpec extends ScyllaSpec {

    @Subject
    ScyllaMetadataDAO metadataDAO

    def setup() {
        metadataDAO = new ScyllaMetadataDAO(session, objectMapper, cassandraProperties, statements)
    }

    def cleanup() {

    }

    def "CRUD on WorkflowDef"() throws Exception {
        given:
        String name = "workflow_def_1"
        int version = 1

        WorkflowDef workflowDef = new WorkflowDef()
        workflowDef.setName(name)
        workflowDef.setVersion(version)
        workflowDef.setOwnerEmail("test@junit.com")

        when: 'create workflow definition'
        metadataDAO.createWorkflowDef(workflowDef)

        then: // fetch the workflow definition
        def defOptional = metadataDAO.getWorkflowDef(name, version)
        defOptional.present
        defOptional.get() == workflowDef

        and: // register a higher version
        int higherVersion = 2
        workflowDef.setVersion(higherVersion)
        workflowDef.setDescription("higher version")

        when: // register the higher version definition
        metadataDAO.createWorkflowDef(workflowDef)
        defOptional = metadataDAO.getWorkflowDef(name, higherVersion)

        then: // fetch the higher version
        defOptional.present
        defOptional.get() == workflowDef

        when: // fetch latest version
        defOptional = metadataDAO.getLatestWorkflowDef(name)

        then:
        defOptional && defOptional.present
        defOptional.get() == workflowDef

        when: // modify the definition
        workflowDef.setOwnerEmail("test@junit.com")
        metadataDAO.updateWorkflowDef(workflowDef)
        defOptional = metadataDAO.getWorkflowDef(name, higherVersion)

        then: // fetch the workflow definition
        defOptional.present
        defOptional.get() == workflowDef

        when: // delete workflow def
        metadataDAO.removeWorkflowDef(name, higherVersion)
        defOptional = metadataDAO.getWorkflowDef(name, higherVersion)

        then:
        defOptional.empty
    }

    private static WorkflowDef ownedDef(String name, int version, String owner, String description) {
        WorkflowDef wd = new WorkflowDef()
        wd.setName(name)
        wd.setVersion(version)
        wd.setOwnerApp(owner)
        wd.setDescription(description)
        return wd
    }

    def "tenant (ownerApp) listings follow the latest version and page without reading other tenants"() {
        given: 'definitions of two tenants; ownership of a name follows its latest version'
        String a = "spec-tenant-a"
        String b = "spec-tenant-b"
        metadataDAO.createWorkflowDef(ownedDef("own_a_flow", 1, a, null))
        metadataDAO.createWorkflowDef(ownedDef("own_a_flow", 2, a, null))
        metadataDAO.createWorkflowDef(ownedDef("own_b_flow", 1, b, null)) // older version: b's
        metadataDAO.createWorkflowDef(ownedDef("own_b_flow", 2, a, null)) // latest: a's
        metadataDAO.createWorkflowDef(ownedDef("own_c_flow", 1, a, null)) // older version: a's
        metadataDAO.createWorkflowDef(ownedDef("own_c_flow", 2, b, null)) // latest: b's
        metadataDAO.createWorkflowDef(ownedDef("own_d_flow", 1, a, "Billing sync"))

        expect: 'names whose latest version is the tenant own, sorted'
        metadataDAO.getWorkflowNamesByOwner(a) == ["own_a_flow", "own_b_flow", "own_d_flow"]
        metadataDAO.getWorkflowNamesByOwner(b) == ["own_c_flow"]
        metadataDAO.getWorkflowNamesByOwner("nobody").isEmpty()
        metadataDAO.getWorkflowNamesByOwner(null).isEmpty()

        and: 'the latest version of each'
        metadataDAO.getWorkflowDefsLatestVersionsByOwner(a).collect { "${it.name}/${it.version}" as String } ==
                ["own_a_flow/2", "own_b_flow/2", "own_d_flow/1"]

        and: 'pages, with the total of the whole tenant list'
        def first = metadataDAO.searchWorkflowDefsLatestVersionsByOwner(a, 0, 2, null, null)
        first.totalHits == 3
        first.results*.name == ["own_a_flow", "own_b_flow"]
        def second = metadataDAO.searchWorkflowDefsLatestVersionsByOwner(a, 2, 2, null, null)
        second.totalHits == 3
        second.results*.name == ["own_d_flow"]
        metadataDAO.searchWorkflowDefsLatestVersionsByOwner(a, 10, 2, null, null).results.isEmpty()

        and: 'a filter narrows the total too (case-insensitive substring)'
        def billing = metadataDAO.searchWorkflowDefsLatestVersionsByOwner(a, 0, 10, "description", "BILLING")
        billing.totalHits == 1
        billing.results*.name == ["own_d_flow"]
        def byName = metadataDAO.searchWorkflowDefsLatestVersionsByOwner(a, 0, 10, "name", "_b_")
        byName.totalHits == 1
        byName.results*.name == ["own_b_flow"]

        and: 'all versions: only the tenant own versions of the tenant names'
        metadataDAO.getWorkflowDefsByOwner(a).collect { "${it.name}/${it.version}" as String } ==
                ["own_a_flow/1", "own_a_flow/2", "own_b_flow/2", "own_d_flow/1"]
        metadataDAO.getWorkflowDefsByOwner(b).collect { "${it.name}/${it.version}" as String } ==
                ["own_c_flow/2"]

        when: 'a new definition is registered'
        metadataDAO.createWorkflowDef(ownedDef("own_e_flow", 1, a, null))

        then: 'it is listed at once (the per-owner view is rebuilt with the index)'
        metadataDAO.getWorkflowNamesByOwner(a) == ["own_a_flow", "own_b_flow", "own_d_flow", "own_e_flow"]

        when: 'the latest version of a name is removed, ownership falls back to the version before'
        metadataDAO.removeWorkflowDef("own_c_flow", 2)

        then:
        metadataDAO.getWorkflowNamesByOwner(a).contains("own_c_flow")
        metadataDAO.getWorkflowNamesByOwner(b).isEmpty()
    }

    def "CRUD on TaskDef"() {
        given:
        String task1Name = "task1"
        String task2Name = "task2"

        when: // fetch all task defs
        def taskDefList = metadataDAO.getAllTaskDefs()

        then:
        taskDefList.empty

        when: // register a task definition
        TaskDef taskDef = new TaskDef()
        taskDef.setName(task1Name)
        metadataDAO.createTaskDef(taskDef)
        taskDefList = metadataDAO.getAllTaskDefs()

        then: // fetch all task defs
        taskDefList && taskDefList.size() == 1

        when: // fetch the task def
        def returnTaskDef = metadataDAO.getTaskDef(task1Name)

        then:
        returnTaskDef == taskDef

        when: // register another task definition
        TaskDef taskDef1 = new TaskDef()
        taskDef1.setName(task2Name)
        metadataDAO.createTaskDef(taskDef1)
        // fetch all task defs
        taskDefList = metadataDAO.getAllTaskDefs()

        then:
        taskDefList && taskDefList.size() == 2

        when: // update task def
        taskDef.setOwnerEmail("juni@test.com")
        metadataDAO.updateTaskDef(taskDef)
        returnTaskDef = metadataDAO.getTaskDef(task1Name)

        then:
        returnTaskDef == taskDef

        when: // delete task def
        metadataDAO.removeTaskDef(task2Name)
        taskDefList = metadataDAO.getAllTaskDefs()

        then:
        taskDefList && taskDefList.size() == 1
        // fetch deleted task def
        metadataDAO.getTaskDef(task2Name) == null
    }

    def "set default response timeout when not set"() {
        given:
        String task1Name = "task1"

        when: // register a task definition
        TaskDef taskDef = new TaskDef()
        taskDef.setName(task1Name)
        taskDef.setResponseTimeoutSeconds(0)
        metadataDAO.createTaskDef(taskDef)
        def returnTaskDef = metadataDAO.getTaskDef(task1Name)

        then:
        returnTaskDef.getResponseTimeoutSeconds() == 3600

        when: // register another task definition
        taskDef.setTimeoutSeconds(200)
        taskDef.setResponseTimeoutSeconds(0)
        metadataDAO.updateTaskDef(taskDef)
        // fetch all task defs
        def taskDefList = metadataDAO.getAllTaskDefs()

        then:
        taskDefList && taskDefList.size() == 1
        taskDefList.get(0).getResponseTimeoutSeconds() == 199

    }

    def "parse index string"() {
        expect:
        def pair = metadataDAO.getWorkflowNameAndVersion(nameVersionStr)
        pair.left == workflowName
        pair.right == version

        where:
        nameVersionStr << ['name/1', 'namespace/name/3', '/namespace/name_with_lodash/2', 'name//4', 'name-with$%/895']
        workflowName << ['name', 'namespace/name', '/namespace/name_with_lodash', 'name/', 'name-with$%']
        version << [1, 3, 2, 4, 895]
    }

    def "parse index string - incorrect values"() {
        when:
        metadataDAO.getWorkflowNameAndVersion("name_with_no_version")

        then:
        def ex = thrown(IllegalStateException.class)
        println(ex.message)

        when:
        metadataDAO.getWorkflowNameAndVersion("name_with_no_version/")

        then:
        ex = thrown(IllegalStateException.class)
        println(ex.message)

        when:
        metadataDAO.getWorkflowNameAndVersion("name/non_number_version")

        then:
        ex = thrown(IllegalStateException.class)
        println(ex.message)
    }
}
