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

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.tenant.TenantMetadataDAO;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
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
}
