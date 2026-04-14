/*
 * Copyright 2022 Conductor Authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;

/** Data access layer for the workflow metadata - task definitions and workflow definitions */
public interface MetadataDAO {

    /**
     * @param taskDef task definition to be created
     */
    TaskDef createTaskDef(TaskDef taskDef);

    /**
     * @param taskDef task definition to be updated.
     * @return name of the task definition
     */
    TaskDef updateTaskDef(TaskDef taskDef);

    /**
     * @param name Name of the task
     * @return Task Definition
     */
    TaskDef getTaskDef(String name);

    /**
     * @return All the task definitions
     */
    List<TaskDef> getAllTaskDefs();

    /**
     * @param name Name of the task
     */
    void removeTaskDef(String name);

    /**
     * @param def workflow definition
     */
    void createWorkflowDef(WorkflowDef def);

    /**
     * @param def workflow definition
     */
    void updateWorkflowDef(WorkflowDef def);

    /**
     * @param name Name of the workflow
     * @return Workflow Definition
     */
    Optional<WorkflowDef> getLatestWorkflowDef(String name);

    /**
     * @param name Name of the workflow
     * @param version version
     * @return workflow definition
     */
    Optional<WorkflowDef> getWorkflowDef(String name, int version);

    /**
     * @param name Name of the workflow definition to be removed
     * @param version Version of the workflow definition to be removed
     */
    void removeWorkflowDef(String name, Integer version);

    /**
     * @return List of all the workflow definitions
     */
    List<WorkflowDef> getAllWorkflowDefs();

    /**
     * @return List the latest versions of the workflow definitions
     */
    List<WorkflowDef> getAllWorkflowDefsLatestVersions();

    /**
     * Returns lightweight summaries (name, version, timestamps) for all workflow definitions,
     * without loading the full definition bodies. Persistence modules can override this with an
     * optimized query that skips reading json_data.
     *
     * @return List of workflow definition summaries
     */
    default List<WorkflowDefSummary> getWorkflowNamesAndVersions() {
        List<WorkflowDefSummary> summaries = new ArrayList<>();
        for (WorkflowDef def : getAllWorkflowDefs()) {
            WorkflowDefSummary summary = new WorkflowDefSummary();
            summary.setName(def.getName());
            summary.setVersion(def.getVersion());
            summary.setCreateTime(def.getCreateTime());
            summary.setUpdateTime(def.getUpdateTime());
            summaries.add(summary);
        }
        return summaries;
    }
}
