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
package com.netflix.conductor.tenant;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WorkflowDefPagesTest {

    private static WorkflowDef def(String name, String description, Map<String, Object> metadata) {
        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setVersion(1);
        def.setDescription(description);
        def.setMetadata(metadata);
        return def;
    }

    private static final WorkflowDef A = def("a_flow", "Billing sync", null);
    private static final WorkflowDef B = def("b_agent", "billing helper", Map.of("agent_sdk", "x"));
    private static final WorkflowDef C = def("c_flow", "other", null);
    private static final List<WorkflowDef> ALL = List.of(A, B, C);

    private static List<String> names(SearchResult<WorkflowDef> r) {
        return r.getResults().stream().map(WorkflowDef::getName).toList();
    }

    @Test
    public void pagesKeepOrderAndReportTheTotal() {
        SearchResult<WorkflowDef> first =
                WorkflowDefPages.filterAndPage(ALL, 0, 2, null, null, null);
        SearchResult<WorkflowDef> last =
                WorkflowDefPages.filterAndPage(ALL, 2, 2, null, null, null);
        SearchResult<WorkflowDef> beyond =
                WorkflowDefPages.filterAndPage(ALL, 9, 2, null, null, null);

        assertEquals(3, first.getTotalHits());
        assertEquals(List.of("a_flow", "b_agent"), names(first));
        assertEquals(List.of("c_flow"), names(last));
        assertEquals(3, beyond.getTotalHits());
        assertTrue(beyond.getResults().isEmpty());
    }

    @Test
    public void classifierSeparatesWorkflowsFromAgents() {
        SearchResult<WorkflowDef> workflows =
                WorkflowDefPages.filterAndPage(ALL, 0, 10, null, null, "workflow");
        SearchResult<WorkflowDef> agents =
                WorkflowDefPages.filterAndPage(ALL, 0, 10, null, null, " AGENT ");

        assertEquals(2, workflows.getTotalHits());
        assertEquals(List.of("a_flow", "c_flow"), names(workflows));
        assertEquals(List.of("b_agent"), names(agents));
    }

    @Test
    public void classifierAndFieldFilterCombine() {
        SearchResult<WorkflowDef> r =
                WorkflowDefPages.filterAndPage(ALL, 0, 10, "description", "BILL", "workflow");

        assertEquals(1, r.getTotalHits());
        assertEquals(List.of("a_flow"), names(r));
    }

    @Test
    public void anUnknownFieldMeansNameAndEmptyValuesAreIgnored() {
        assertEquals(
                List.of("c_flow"),
                names(WorkflowDefPages.filterAndPage(ALL, 0, 10, "bogus", "c_", null)));
        assertEquals(3, WorkflowDefPages.filterAndPage(ALL, 0, 10, "name", "", " ").getTotalHits());
    }

    @Test
    public void nullListAndNegativeBoundsAreSafe() {
        assertEquals(
                0, WorkflowDefPages.filterAndPage(null, 0, 10, null, null, null).getTotalHits());
        SearchResult<WorkflowDef> r = WorkflowDefPages.filterAndPage(ALL, -5, -1, null, null, null);
        assertEquals(3, r.getTotalHits());
        assertTrue(r.getResults().isEmpty());
    }
}
