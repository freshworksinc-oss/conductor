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
package com.netflix.conductor.migrator.tree;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.migrator.rest.SourceClient;

/**
 * Builds a {@link WorkflowTree} from a root workflow id by following {@code SUB_WORKFLOW} tasks'
 * {@code subWorkflowId} down the tree (F9), exporting each workflow from the source. Produces a
 * children-first (post-order) ordering and guards against cycles.
 */
@Component
public class TreeResolver {

    private final SourceClient source;

    public TreeResolver(SourceClient source) {
        this.source = source;
    }

    public WorkflowTree resolve(String rootWorkflowId) {
        List<Workflow> childrenFirst = new ArrayList<>();
        visit(rootWorkflowId, new LinkedHashSet<>(), childrenFirst);
        return new WorkflowTree(rootWorkflowId, childrenFirst);
    }

    /**
     * Walks {@code parentWorkflowId} up from any workflow id to the tree root. A search enumeration
     * returns sub-workflow children too; this lets the caller collapse them to their owning root so
     * each tree is synced once, as a unit (F9).
     */
    public String findRootId(String workflowId) {
        Set<String> onPath = new LinkedHashSet<>();
        String current = workflowId;
        while (true) {
            if (!onPath.add(current)) {
                throw new IllegalStateException(
                        "Cycle detected walking to root from "
                                + workflowId
                                + " (path="
                                + onPath
                                + ")");
            }
            Workflow workflow = source.getWorkflow(current);
            if (workflow == null) {
                throw new IllegalStateException("Source returned no workflow for id " + current);
            }
            String parent = workflow.getParentWorkflowId();
            if (StringUtils.isBlank(parent)) {
                return current;
            }
            current = parent;
        }
    }

    /** Post-order DFS: append a workflow only after all of its sub-workflows are appended. */
    private void visit(String workflowId, Set<String> onPath, List<Workflow> out) {
        if (out.stream().anyMatch(w -> w.getWorkflowId().equals(workflowId))) {
            return; // already collected (a workflow reached via two paths)
        }
        if (!onPath.add(workflowId)) {
            throw new IllegalStateException(
                    "Cycle detected in workflow tree at " + workflowId + " (path=" + onPath + ")");
        }

        Workflow workflow = source.getWorkflow(workflowId);
        if (workflow == null) {
            throw new IllegalStateException("Source returned no workflow for id " + workflowId);
        }

        for (Task task : workflow.getTasks()) {
            String childId = task.getSubWorkflowId();
            if (StringUtils.isNotBlank(childId)) {
                visit(childId, onPath, out);
            }
        }

        onPath.remove(workflowId);
        out.add(workflow);
    }
}
