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

import com.netflix.conductor.common.config.ObjectMapperProvider
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask

import spock.lang.Specification
import spock.lang.Unroll

/** ScyllaMetadataDAO.ownerAppOf: the only field the workflow monitor needs from a definition. */
class WorkflowMetricInfoSpec extends Specification {

    def "reads ownerApp from a definition serialized the way it is stored"() {
        given:
        def task = new WorkflowTask(name: "t", taskReferenceName: "t_ref",
                inputParameters: [ownerApp: "nested-not-this-one"])
        def def1 = new WorkflowDef(name: "wf", version: 3, tasks: [task], ownerApp: "freshservice")
        def json = new ObjectMapperProvider().objectMapper.writeValueAsString(def1)

        expect:
        ScyllaMetadataDAO.ownerAppOf("wf", json) == "freshservice"
    }

    @Unroll
    def "ownerApp of #json is #expected"() {
        expect:
        ScyllaMetadataDAO.ownerAppOf("wf", json) == expected

        where:
        json                                                          || expected
        '{"name":"wf","ownerApp":"a"}'                                || "a"
        '{"tasks":[{"ownerApp":"nested"}],"ownerApp":"top"}'          || "top"
        '{"inputTemplate":{"ownerApp":"nested"},"name":"wf"}'         || null
        '{"name":"wf"}'                                               || null
        '{"ownerApp":null}'                                           || null
        '{"name":"wf",'                                               || null
        '[]'                                                          || null
        null                                                          || null
    }
}
