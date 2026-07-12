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

import java.util.List;

import com.netflix.conductor.common.run.Workflow;

/**
 * A workflow tree — a root workflow plus all sub-workflows reachable from it (F9). This is the
 * atomic migration unit; it is imported and bootstrapped children-first so a parent never
 * references a child that does not yet exist on the destination.
 */
public class WorkflowTree {

    private final String rootWorkflowId;

    /** All workflows in the tree, ordered children-first (root last). */
    private final List<Workflow> childrenFirst;

    public WorkflowTree(String rootWorkflowId, List<Workflow> childrenFirst) {
        this.rootWorkflowId = rootWorkflowId;
        this.childrenFirst = childrenFirst;
    }

    public String getRootWorkflowId() {
        return rootWorkflowId;
    }

    public List<Workflow> getChildrenFirst() {
        return childrenFirst;
    }

    public int size() {
        return childrenFirst.size();
    }
}
