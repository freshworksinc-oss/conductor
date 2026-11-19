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
package com.netflix.conductor.dao;

import java.util.List;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;

/**
 * Optional interface for {@link MetadataDAO} implementations that can list workflow definitions of
 * one owner ({@code ownerApp}, the tenant) without filtering every definition on each call.
 *
 * <p>A name belongs to the owner when the {@code ownerApp} of its <b>latest</b> version equals the
 * owner exactly. Without this interface the tenant listing falls back to reading the latest
 * definition of every name (or every version of every name) and filtering in memory.
 */
public interface OwnerScopedMetadataDAO {

    /** Names whose latest version belongs to the owner, sorted. */
    List<String> getWorkflowNamesByOwner(String ownerApp);

    /** The latest version of each of the owner's names, sorted by name. */
    List<WorkflowDef> getWorkflowDefsLatestVersionsByOwner(String ownerApp);

    /**
     * One page of {@link #getWorkflowDefsLatestVersionsByOwner}. When {@code filterField} and
     * {@code filterValue} are both non-empty only definitions whose field contains the value
     * (case-insensitive) are counted and returned, as {@link
     * PaginatedMetadataDAO#searchWorkflowDefsLatestVersions(int, int, String, String)}.
     */
    SearchResult<WorkflowDef> searchWorkflowDefsLatestVersionsByOwner(
            String ownerApp, int start, int size, String filterField, String filterValue);

    /**
     * Every version owned by the owner of each of the owner's names, ordered by name and version.
     * Only the owner's names are read, not every definition.
     */
    List<WorkflowDef> getWorkflowDefsByOwner(String ownerApp);
}
