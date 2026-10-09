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
package com.netflix.conductor.rest.controllers;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.tenant.TenantMetadataDAO;

@RestController
@RequestMapping("/api/tenant/metadata")
public class TenantMetadataResource {

    private final TenantMetadataDAO tenantMetadataDAO;

    public TenantMetadataResource(TenantMetadataDAO tenantMetadataDAO) {
        this.tenantMetadataDAO = tenantMetadataDAO;
    }

    // ---- Workflow Definition Endpoints ----

    @GetMapping("/workflow")
    public List<WorkflowDef> getWorkflowDefs(@RequestHeader("X-Tenant-ID") String tenantId) {
        return tenantMetadataDAO.getWorkflowDefsByTenant(tenantId);
    }

    @GetMapping("/workflow/latest-versions")
    public List<WorkflowDef> getWorkflowDefsLatestVersions(
            @RequestHeader("X-Tenant-ID") String tenantId) {
        return tenantMetadataDAO.getWorkflowDefsLatestVersionsByTenant(tenantId);
    }

    @GetMapping("/workflow/names")
    public List<String> getWorkflowNames(@RequestHeader("X-Tenant-ID") String tenantId) {
        return tenantMetadataDAO.getWorkflowNamesByTenant(tenantId);
    }

    @GetMapping("/workflow/names-and-versions")
    public Map<String, List<WorkflowDefSummary>> getWorkflowNamesAndVersions(
            @RequestHeader("X-Tenant-ID") String tenantId) {
        return tenantMetadataDAO.getWorkflowNamesAndVersionsByTenant(tenantId);
    }

    @GetMapping("/workflow/{name}/versions")
    public List<WorkflowDefSummary> getWorkflowVersions(
            @RequestHeader("X-Tenant-ID") String tenantId, @PathVariable("name") String name) {
        return tenantMetadataDAO.getWorkflowVersionsByTenant(tenantId, name);
    }

    @GetMapping("/workflow/{name}")
    public ResponseEntity<WorkflowDef> getWorkflowDef(
            @RequestHeader("X-Tenant-ID") String tenantId,
            @PathVariable("name") String name,
            @RequestParam(value = "version", required = false) Integer version) {
        return tenantMetadataDAO
                .getWorkflowDef(tenantId, name, version)
                .map(ResponseEntity::ok)
                .orElseThrow(
                        () ->
                                new NotFoundException(
                                        "No such workflow found by name: %s, version: %d",
                                        name, version));
    }

    // ---- Task Definition Endpoints ----

    @GetMapping("/taskdefs")
    public List<TaskDef> getTaskDefs(@RequestHeader("X-Tenant-ID") String tenantId) {
        return tenantMetadataDAO.getTaskDefsByTenant(tenantId);
    }

    @GetMapping("/taskdefs/{tasktype}")
    public ResponseEntity<TaskDef> getTaskDef(
            @RequestHeader("X-Tenant-ID") String tenantId,
            @PathVariable("tasktype") String taskType) {
        return tenantMetadataDAO
                .getTaskDef(tenantId, taskType)
                .map(ResponseEntity::ok)
                .orElseThrow(
                        () ->
                                new NotFoundException(
                                        "No such taskType found by name: %s", taskType));
    }
}
