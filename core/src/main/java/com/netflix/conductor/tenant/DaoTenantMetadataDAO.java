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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.conductor.common.metadata.Auditable;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.dao.MetadataDAO;

/**
 * Tenant-scoped definitions on top of the persistence's own {@link MetadataDAO}, so it works with
 * any database (Scylla) and needs no Postgres. A definition is the tenant's when its {@code
 * ownerApp} equals the tenant id exactly, the same rule as {@link PostgresTenantMetadataDAO}. Used
 * when {@code conductor.db.type} is not {@code postgres}.
 *
 * <p>Definitions are read through {@link MetadataDAO} on each call and filtered in memory, the same
 * cost as the unfiltered {@code /api/metadata} listing.
 */
public class DaoTenantMetadataDAO implements TenantMetadataDAO {

    private static final Comparator<WorkflowDef> BY_NAME_THEN_VERSION =
            Comparator.comparing(WorkflowDef::getName).thenComparingInt(WorkflowDef::getVersion);

    private final MetadataDAO metadataDAO;

    public DaoTenantMetadataDAO(MetadataDAO metadataDAO) {
        this.metadataDAO = metadataDAO;
    }

    @Override
    public List<WorkflowDef> getWorkflowDefsByTenant(String tenantId) {
        return owned(tenantId, metadataDAO.getAllWorkflowDefs()).stream()
                .sorted(BY_NAME_THEN_VERSION)
                .collect(Collectors.toList());
    }

    @Override
    public List<WorkflowDef> getWorkflowDefsLatestVersionsByTenant(String tenantId) {
        return owned(tenantId, metadataDAO.getAllWorkflowDefsLatestVersions()).stream()
                .sorted(Comparator.comparing(WorkflowDef::getName))
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, List<WorkflowDefSummary>> getWorkflowNamesAndVersionsByTenant(
            String tenantId) {
        Map<String, List<WorkflowDefSummary>> result = new LinkedHashMap<>();
        for (WorkflowDef def : getWorkflowDefsByTenant(tenantId)) {
            result.computeIfAbsent(def.getName(), k -> new ArrayList<>()).add(summary(def));
        }
        return result;
    }

    @Override
    public Optional<WorkflowDef> getWorkflowDef(String tenantId, String name, Integer version) {
        Optional<WorkflowDef> def =
                version != null
                        ? metadataDAO.getWorkflowDef(name, version)
                        : metadataDAO.getLatestWorkflowDef(name);
        return def.filter(d -> isOwnedBy(tenantId, d));
    }

    @Override
    public List<WorkflowDefSummary> getWorkflowVersionsByTenant(String tenantId, String name) {
        return getWorkflowDefsByTenant(tenantId).stream()
                .filter(d -> d.getName().equals(name))
                .map(DaoTenantMetadataDAO::summary)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getWorkflowNamesByTenant(String tenantId) {
        return owned(tenantId, metadataDAO.getAllWorkflowDefs()).stream()
                .map(WorkflowDef::getName)
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    @Override
    public List<TaskDef> getTaskDefsByTenant(String tenantId) {
        return owned(tenantId, metadataDAO.getAllTaskDefs()).stream()
                .sorted(Comparator.comparing(TaskDef::getName))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<TaskDef> getTaskDef(String tenantId, String name) {
        try {
            return Optional.ofNullable(metadataDAO.getTaskDef(name))
                    .filter(d -> isOwnedBy(tenantId, d));
        } catch (NotFoundException e) {
            // some persistences throw instead of returning null
            return Optional.empty();
        }
    }

    private static boolean isOwnedBy(String tenantId, Auditable def) {
        return tenantId != null && tenantId.equals(def.getOwnerApp());
    }

    private static <T extends Auditable> List<T> owned(String tenantId, List<T> defs) {
        if (defs == null) {
            return List.of();
        }
        return defs.stream().filter(d -> isOwnedBy(tenantId, d)).collect(Collectors.toList());
    }

    private static WorkflowDefSummary summary(WorkflowDef def) {
        WorkflowDefSummary summary = new WorkflowDefSummary();
        summary.setName(def.getName());
        summary.setVersion(def.getVersion());
        summary.setCreateTime(def.getCreateTime());
        return summary;
    }
}
