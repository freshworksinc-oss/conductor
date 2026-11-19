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

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowClassifier;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.tenant.TenantMetadataConditions;
import com.netflix.conductor.tenant.TenantMetadataDAO;

@RestController
@RequestMapping("/api/tenant/metadata")
// Same switch as the tenant metadata configurations (not @ConditionalOnBean, which is unreliable on
// scanned components): on unless conductor.tenant-metadata.enabled=false.
@ConditionalOnExpression(TenantMetadataConditions.SERVED)
public class TenantMetadataResource {

    /** Page size when a page is asked for without {@code size}, as /api/metadata. */
    private static final int DEFAULT_PAGE_SIZE = 100;

    private final TenantMetadataDAO tenantMetadataDAO;

    public TenantMetadataResource(TenantMetadataDAO tenantMetadataDAO) {
        this.tenantMetadataDAO = tenantMetadataDAO;
    }

    // ---- Workflow Definition Endpoints ----

    @GetMapping("/workflow")
    public List<WorkflowDef> getWorkflowDefs(
            @RequestHeader("X-Tenant-ID") String tenantId,
            @RequestParam(value = "classifier", required = false) String classifier,
            @RequestParam(value = "short", required = false) Boolean shortList) {
        // short=true (the UI's definitions list): latest versions only, as
        // /workflow/latest-versions
        List<WorkflowDef> defs =
                Boolean.TRUE.equals(shortList)
                        ? tenantMetadataDAO.getWorkflowDefsLatestVersionsByTenant(tenantId)
                        : tenantMetadataDAO.getWorkflowDefsByTenant(tenantId);
        // Same classifier filter as /api/metadata/workflow (agent vs workflow views): "workflow"
        // matches untagged defs, any other value the derived tag literally.
        if (classifier == null || classifier.isBlank()) {
            return defs;
        }
        String wanted = classifier.trim();
        return defs.stream()
                .filter(wd -> wanted.equalsIgnoreCase(WorkflowClassifier.classifierOf(wd)))
                .toList();
    }

    /**
     * Same contract as {@code GET /api/metadata/workflow/latest-versions} (the proxy rewrites that
     * call here for tenants): with {@code start}, {@code size} or a filter, one page (size 100 when
     * omitted); with none of them, every latest version. Always a {@link SearchResult}.
     */
    @GetMapping("/workflow/latest-versions")
    public SearchResult<WorkflowDef> getWorkflowDefsLatestVersions(
            @RequestHeader("X-Tenant-ID") String tenantId,
            @RequestParam(value = "start", required = false) Integer start,
            @RequestParam(value = "size", required = false) Integer size,
            @RequestParam(value = "filterField", required = false) String filterField,
            @RequestParam(value = "filterValue", required = false) String filterValue) {
        if (start != null || size != null || filterField != null || filterValue != null) {
            boolean filtered =
                    filterField != null
                            && filterValue != null
                            && !filterField.isEmpty()
                            && !filterValue.isEmpty();
            return tenantMetadataDAO.searchWorkflowDefsLatestVersionsByTenant(
                    tenantId,
                    start != null ? start : 0,
                    size != null ? size : DEFAULT_PAGE_SIZE,
                    filtered ? filterField : null,
                    filtered ? filterValue : null);
        }
        List<WorkflowDef> all = tenantMetadataDAO.getWorkflowDefsLatestVersionsByTenant(tenantId);
        return new SearchResult<>(all.size(), all);
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
