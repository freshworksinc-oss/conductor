/*
 * Copyright 2020 Conductor Authors.
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
package com.netflix.conductor.rest.controllers;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.tenant.TenantMetadataDAO;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TenantMetadataResourceTest {

    private static final String TENANT_ID = "tenant-1";

    private TenantMetadataResource tenantMetadataResource;

    private TenantMetadataDAO mockTenantMetadataDAO;

    @Before
    public void before() {
        this.mockTenantMetadataDAO = mock(TenantMetadataDAO.class);
        this.tenantMetadataResource = new TenantMetadataResource(this.mockTenantMetadataDAO);
    }

    @Test
    public void testGetTaskDefReturnsDefinitionWhenPresent() {
        TaskDef taskDef = new TaskDef();
        taskDef.setName("data_arrangement_11");

        when(mockTenantMetadataDAO.getTaskDef(TENANT_ID, "data_arrangement_11"))
                .thenReturn(Optional.of(taskDef));

        ResponseEntity<TaskDef> response =
                tenantMetadataResource.getTaskDef(TENANT_ID, "data_arrangement_11");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(taskDef, response.getBody());
    }

    @Test
    public void testGetTaskDefThrowsNotFoundWhenMissing() {
        when(mockTenantMetadataDAO.getTaskDef(TENANT_ID, "data_arrangement_11"))
                .thenReturn(Optional.empty());

        NotFoundException ex =
                assertThrows(
                        NotFoundException.class,
                        () -> tenantMetadataResource.getTaskDef(TENANT_ID, "data_arrangement_11"));

        // Same message the standard /api/metadata/taskdefs/{name} endpoint produces,
        // so the shared ApplicationExceptionMapper renders the identical error envelope.
        assertEquals("No such taskType found by name: data_arrangement_11", ex.getMessage());
    }

    @Test
    public void testGetWorkflowDefReturnsDefinitionWhenPresent() {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("data_arrangement_11");
        workflowDef.setVersion(1);

        when(mockTenantMetadataDAO.getWorkflowDef(TENANT_ID, "data_arrangement_11", 1))
                .thenReturn(Optional.of(workflowDef));

        ResponseEntity<WorkflowDef> response =
                tenantMetadataResource.getWorkflowDef(TENANT_ID, "data_arrangement_11", 1);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(workflowDef, response.getBody());
    }

    @Test
    public void testGetWorkflowDefThrowsNotFoundWhenMissing() {
        when(mockTenantMetadataDAO.getWorkflowDef(TENANT_ID, "data_arrangement_11", 1))
                .thenReturn(Optional.empty());

        NotFoundException ex =
                assertThrows(
                        NotFoundException.class,
                        () ->
                                tenantMetadataResource.getWorkflowDef(
                                        TENANT_ID, "data_arrangement_11", 1));

        assertEquals(
                "No such workflow found by name: data_arrangement_11, version: 1", ex.getMessage());
    }

    @Test
    public void testGetWorkflowDefThrowsNotFoundWhenMissingAndVersionOmitted() {
        when(mockTenantMetadataDAO.getWorkflowDef(TENANT_ID, "data_arrangement_11", null))
                .thenReturn(Optional.empty());

        NotFoundException ex =
                assertThrows(
                        NotFoundException.class,
                        () ->
                                tenantMetadataResource.getWorkflowDef(
                                        TENANT_ID, "data_arrangement_11", null));

        assertEquals(
                "No such workflow found by name: data_arrangement_11, version: null",
                ex.getMessage());
    }

    // ---- /workflow: short and classifier ----

    private static WorkflowDef def(String name, Map<String, Object> metadata) {
        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setVersion(1);
        def.setMetadata(metadata);
        return def;
    }

    @Test
    public void shortListsLatestVersionsAndFullListsEveryVersion() {
        List<WorkflowDef> latest = List.of(def("a", null));
        List<WorkflowDef> all = List.of(def("a", null), def("a", null));
        when(mockTenantMetadataDAO.getWorkflowDefsLatestVersionsByTenant(TENANT_ID))
                .thenReturn(latest);
        when(mockTenantMetadataDAO.getWorkflowDefsByTenant(TENANT_ID)).thenReturn(all);

        assertEquals(latest, tenantMetadataResource.getWorkflowDefs(TENANT_ID, null, true));
        assertEquals(all, tenantMetadataResource.getWorkflowDefs(TENANT_ID, null, null));
    }

    @Test
    public void classifierFiltersAgentsFromWorkflows() {
        WorkflowDef plain = def("plain", null);
        WorkflowDef agent = def("agent", Map.of("agent_sdk", "x"));
        when(mockTenantMetadataDAO.getWorkflowDefsLatestVersionsByTenant(TENANT_ID))
                .thenReturn(List.of(plain, agent));

        assertEquals(
                List.of(plain),
                tenantMetadataResource.getWorkflowDefs(TENANT_ID, "workflow", true));
        assertEquals(
                List.of(agent), tenantMetadataResource.getWorkflowDefs(TENANT_ID, " AGENT ", true));
        assertEquals(
                List.of(plain, agent), tenantMetadataResource.getWorkflowDefs(TENANT_ID, "", true));
    }

    // ---- /workflow/latest-versions: same contract as /api/metadata ----

    @Test
    public void latestVersionsWithoutParamsAreEveryLatestVersionAsASearchResult() {
        List<WorkflowDef> latest = List.of(def("a", null), def("b", null));
        when(mockTenantMetadataDAO.getWorkflowDefsLatestVersionsByTenant(TENANT_ID))
                .thenReturn(latest);

        SearchResult<WorkflowDef> result =
                tenantMetadataResource.getWorkflowDefsLatestVersions(
                        TENANT_ID, null, null, null, null);

        assertEquals(2, result.getTotalHits());
        assertEquals(latest, result.getResults());
        verify(mockTenantMetadataDAO, never())
                .searchWorkflowDefsLatestVersionsByTenant(any(), anyInt(), anyInt(), any(), any());
    }

    @Test
    public void latestVersionsPageIsAskedOfTheDaoWithDefaults() {
        SearchResult<WorkflowDef> page = new SearchResult<>(40, List.of(def("a", null)));
        when(mockTenantMetadataDAO.searchWorkflowDefsLatestVersionsByTenant(
                        TENANT_ID, 0, 15, null, null))
                .thenReturn(page);
        when(mockTenantMetadataDAO.searchWorkflowDefsLatestVersionsByTenant(
                        TENANT_ID, 30, 100, null, null))
                .thenReturn(page);

        assertEquals(
                page,
                tenantMetadataResource.getWorkflowDefsLatestVersions(TENANT_ID, 0, 15, null, null));
        assertEquals( // size defaults to 100, as /api/metadata
                page,
                tenantMetadataResource.getWorkflowDefsLatestVersions(
                        TENANT_ID, 30, null, null, null));
    }

    @Test
    public void latestVersionsFilterIsPassedOnlyWhenFieldAndValueAreBothGiven() {
        SearchResult<WorkflowDef> page = new SearchResult<>(1, List.of(def("a", null)));
        when(mockTenantMetadataDAO.searchWorkflowDefsLatestVersionsByTenant(
                        TENANT_ID, 0, 100, "description", "bill"))
                .thenReturn(page);
        when(mockTenantMetadataDAO.searchWorkflowDefsLatestVersionsByTenant(
                        TENANT_ID, 0, 100, null, null))
                .thenReturn(new SearchResult<>(0, List.of()));

        assertEquals(
                page,
                tenantMetadataResource.getWorkflowDefsLatestVersions(
                        TENANT_ID, null, null, "description", "bill"));
        assertEquals( // value without field: unfiltered page
                0,
                tenantMetadataResource
                        .getWorkflowDefsLatestVersions(TENANT_ID, null, null, null, "bill")
                        .getTotalHits());
    }
}
