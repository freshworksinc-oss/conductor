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
package com.netflix.conductor.migrator.convert;

import java.util.stream.Collectors;

import org.springframework.beans.BeanUtils;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * Converts the REST/common model ({@link Workflow}/{@link Task}) that we export from the source
 * into the engine model ({@link WorkflowModel}/{@link TaskModel}) that {@code PostgresExecutionDAO}
 * persists.
 *
 * <p>Conductor only ships the forward direction ({@code WorkflowModel#toWorkflow()} / {@code
 * TaskModel#toTask()}) because the engine never imports execution state. This class is the reverse
 * of that logic and mirrors it exactly: {@link BeanUtils#copyProperties} for the same-named fields,
 * then a few things by hand.
 *
 * <p>Things that {@code copyProperties} cannot do and we handle explicitly:
 *
 * <ul>
 *   <li><b>status</b> — different enum types ({@code Workflow.WorkflowStatus} vs {@code
 *       WorkflowModel.Status}); mapped by name.
 *   <li><b>tasks</b> — {@code List<Task>} vs {@code List<TaskModel>}; type erasure would let {@code
 *       copyProperties} shove {@code Task} objects into a {@code List<TaskModel>}, so it is
 *       excluded and converted element-by-element.
 *   <li><b>updateTime → updatedTime</b> — the field is renamed between the two models.
 *   <li><b>workflowDefinition</b> — copied by name, but REQUIRED: the DAO calls {@code
 *       getWorkflowName()} (which reads the definition) on both the create and pending-workflow
 *       paths and NPEs if it is null.
 * </ul>
 */
public final class ModelConverter {

    private static final String[] WORKFLOW_IGNORE = {"tasks", "status"};
    private static final String[] TASK_IGNORE = {"status"};

    private ModelConverter() {}

    public static WorkflowModel toWorkflowModel(Workflow workflow) {
        if (workflow.getWorkflowDefinition() == null) {
            throw new IllegalArgumentException(
                    "Workflow "
                            + workflow.getWorkflowId()
                            + " has no workflowDefinition; cannot import (the destination DAO"
                            + " resolves the workflow name from the definition). Ensure the source"
                            + " export includes it.");
        }

        WorkflowModel model = new WorkflowModel();
        BeanUtils.copyProperties(workflow, model, WORKFLOW_IGNORE);

        if (workflow.getStatus() != null) {
            model.setStatus(WorkflowModel.Status.valueOf(workflow.getStatus().name()));
        }
        // Auditable exposes updateTime; the engine model calls it updatedTime.
        model.setUpdatedTime(workflow.getUpdateTime());

        model.setTasks(
                workflow.getTasks().stream()
                        .map(ModelConverter::toTaskModel)
                        .collect(Collectors.toCollection(java.util.LinkedList::new)));

        return model;
    }

    public static TaskModel toTaskModel(Task task) {
        TaskModel model = new TaskModel();
        BeanUtils.copyProperties(task, model, TASK_IGNORE);

        if (task.getStatus() != null) {
            model.setStatus(TaskModel.Status.valueOf(task.getStatus().name()));
        }
        return model;
    }
}
