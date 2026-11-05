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
import java.util.stream.Collectors;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.service.TaskService;
import com.netflix.conductor.tenant.TenantMetadataConditions;
import com.netflix.conductor.tenant.TenantMetadataDAO;

@RestController
@RequestMapping("/api/tenant/metadata")
// Same switch as the tenant metadata configurations (not @ConditionalOnBean, which is unreliable on
// scanned components): on unless conductor.tenant-metadata.enabled=false.
@ConditionalOnExpression(TenantMetadataConditions.SERVED)
public class TenantMetadataResource {

    private final TenantMetadataDAO tenantMetadataDAO;
    private final TaskService taskService;

    public TenantMetadataResource(TenantMetadataDAO tenantMetadataDAO, TaskService taskService) {
        this.tenantMetadataDAO = tenantMetadataDAO;
        this.taskService = taskService;
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

    // ---- Poll Data Endpoints ----

    /**
     * Tenant-scoped counterpart of {@code GET /api/tasks/queue/polldata/all}.
     *
     * <p>Poll data has no owner field, so it is scoped by queue domain, matching the auth proxy's
     * routing: workflow start forces {@code taskToDomain["*"] = <tenantId>} and prefixes custom
     * domains with {@code <tenantId>_}, and task poll injects the same domain. An entry belongs to
     * the tenant iff its domain is {@code <tenantId>} or starts with {@code <tenantId>_}. Entries
     * with no domain (polls that bypassed the proxy) belong to no tenant.
     */
    @GetMapping("/queue/polldata/all")
    public List<PollData> getAllPollData(@RequestHeader("X-Tenant-ID") String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            return List.of();
        }
        String prefix = tenantId + "_";
        return taskService.getAllPollData().stream()
                .filter(
                        pd ->
                                pd.getDomain() != null
                                        && (pd.getDomain().equals(tenantId)
                                                || pd.getDomain().startsWith(prefix)))
                .collect(Collectors.toList());
    }
}
