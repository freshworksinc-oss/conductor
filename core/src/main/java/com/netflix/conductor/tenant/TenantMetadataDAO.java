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
import java.util.Optional;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.common.run.SearchResult;

/**
 * Read-only, tenant-scoped view of workflow and task definitions: a definition belongs to the
 * tenant in its {@code ownerApp} field (the auth proxy writes it on registration). Served under
 * {@code /api/tenant/metadata}, where the proxy sends tenant GETs of {@code /api/metadata}.
 *
 * <p>Implementations: {@link PostgresTenantMetadataDAO} (Postgres {@code meta_*} tables, the
 * default) and {@link DaoTenantMetadataDAO} (any persistence, through {@code MetadataDAO}; use it
 * with Scylla). Chosen by {@code conductor.db.type}, see {@link TenantMetadataConditions}.
 */
public interface TenantMetadataDAO {

    /** All versions of the tenant's workflow definitions, ordered by name and version. */
    List<WorkflowDef> getWorkflowDefsByTenant(String tenantId);

    /** The latest version of each workflow definition owned by the tenant, ordered by name. */
    List<WorkflowDef> getWorkflowDefsLatestVersionsByTenant(String tenantId);

    /**
     * One page of {@link #getWorkflowDefsLatestVersionsByTenant}, same contract as {@code GET
     * /api/metadata/workflow/latest-versions}: when {@code filterField} and {@code filterValue} are
     * both non-empty, only definitions whose field ({@code name}, {@code description}, {@code
     * ownerEmail}, {@code ownerApp}; anything else means {@code name}) contains the value
     * (case-insensitive) are counted and returned. {@code totalHits} is the filtered count.
     *
     * <p>The default pages the full tenant list in memory; implementations with a cheaper way
     * override it.
     */
    default SearchResult<WorkflowDef> searchWorkflowDefsLatestVersionsByTenant(
            String tenantId, int start, int size, String filterField, String filterValue) {
        return WorkflowDefPages.filterAndPage(
                getWorkflowDefsLatestVersionsByTenant(tenantId),
                start,
                size,
                filterField,
                filterValue,
                null);
    }

    /** The tenant's workflow names with a summary of each version. */
    Map<String, List<WorkflowDefSummary>> getWorkflowNamesAndVersionsByTenant(String tenantId);

    /**
     * One definition of the tenant; the latest version when {@code version} is null. Empty when it
     * does not exist or belongs to another tenant.
     */
    Optional<WorkflowDef> getWorkflowDef(String tenantId, String name, Integer version);

    /** Version summaries of one of the tenant's workflows, ordered by version. */
    List<WorkflowDefSummary> getWorkflowVersionsByTenant(String tenantId, String name);

    /** Distinct names of the tenant's workflow definitions, sorted. */
    List<String> getWorkflowNamesByTenant(String tenantId);

    /** The tenant's task definitions, ordered by name. */
    List<TaskDef> getTaskDefsByTenant(String tenantId);

    /** One task definition of the tenant; empty when missing or owned by another tenant. */
    Optional<TaskDef> getTaskDef(String tenantId, String name);
}
