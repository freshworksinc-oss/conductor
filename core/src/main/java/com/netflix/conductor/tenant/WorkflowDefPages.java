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
import java.util.List;
import java.util.Locale;

import com.netflix.conductor.common.metadata.workflow.WorkflowClassifier;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;

/**
 * Filtering and paging of an in-memory list of workflow definitions, shared by the definition
 * tenant listings ({@code /api/tenant/metadata/workflow/latest-versions}) so they all follow the
 * same rules.
 */
public final class WorkflowDefPages {

    private WorkflowDefPages() {}

    /**
     * One page of {@code defs} (kept in their order) after the filters:
     *
     * <ul>
     *   <li>{@code classifier}: as {@code GET /api/metadata/workflow?classifier=} — {@code
     *       workflow} matches untagged definitions, any other value the derived tag; trimmed,
     *       case-insensitive. Ignored when blank.
     *   <li>{@code filterField}/{@code filterValue}: case-insensitive substring of {@code name},
     *       {@code description}, {@code ownerEmail} or {@code ownerApp} (anything else means {@code
     *       name}). Ignored unless the value is non-empty.
     * </ul>
     *
     * {@code totalHits} is the filtered count. {@code start} below 0 counts as 0, {@code size}
     * below 0 as 0.
     */
    public static SearchResult<WorkflowDef> filterAndPage(
            List<WorkflowDef> defs,
            int start,
            int size,
            String filterField,
            String filterValue,
            String classifier) {
        List<WorkflowDef> matches = defs == null ? List.of() : defs;
        boolean byValue = filterValue != null && !filterValue.isEmpty();
        boolean byClassifier = classifier != null && !classifier.isBlank();
        if (byValue || byClassifier) {
            String wanted = byValue ? filterValue.toLowerCase(Locale.ROOT) : null;
            String wantedClassifier = byClassifier ? classifier.trim() : null;
            List<WorkflowDef> kept = new ArrayList<>();
            for (WorkflowDef def : matches) {
                if (byClassifier
                        && !wantedClassifier.equalsIgnoreCase(
                                WorkflowClassifier.classifierOf(def))) {
                    continue;
                }
                if (byValue) {
                    String value = fieldOf(def, filterField);
                    if (value == null || !value.toLowerCase(Locale.ROOT).contains(wanted)) {
                        continue;
                    }
                }
                kept.add(def);
            }
            matches = kept;
        }
        int from = Math.min(Math.max(start, 0), matches.size());
        int to = Math.min(from + Math.max(size, 0), matches.size());
        return new SearchResult<>(matches.size(), new ArrayList<>(matches.subList(from, to)));
    }

    /** The value of a filterable field; anything but the known fields means {@code name}. */
    public static String fieldOf(WorkflowDef def, String field) {
        switch (field == null ? "" : field) {
            case "description":
                return def.getDescription();
            case "ownerEmail":
                return def.getOwnerEmail();
            case "ownerApp":
                return def.getOwnerApp();
            default:
                return def.getName();
        }
    }
}
