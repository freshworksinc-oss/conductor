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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ModelConverterTest {

    private Workflow sampleWorkflow() {
        WorkflowDef def = new WorkflowDef();
        def.setName("sample_wf");
        def.setVersion(2);

        Task task = new Task();
        task.setTaskId("task-1");
        task.setReferenceTaskName("ref_1");
        task.setStatus(Task.Status.SCHEDULED);
        task.setTaskType("SIMPLE");
        task.setWorkflowInstanceId("wf-1");
        task.setRetryCount(0);
        task.setInputData(Map.of("a", "b"));

        Workflow wf = new Workflow();
        wf.setWorkflowId("wf-1");
        wf.setStatus(Workflow.WorkflowStatus.RUNNING);
        wf.setWorkflowDefinition(def);
        wf.setCorrelationId("corr-1");
        wf.setPriority(5);
        wf.setCreateTime(1000L);
        wf.setUpdateTime(2000L);
        wf.setInput(Map.of("k", "v"));
        wf.setTasks(List.of(task));
        return wf;
    }

    @Test
    void mapsCoreFieldsIncludingRenamedUpdateTimeAndEnums() {
        WorkflowModel model = ModelConverter.toWorkflowModel(sampleWorkflow());

        assertEquals("wf-1", model.getWorkflowId());
        assertEquals(WorkflowModel.Status.RUNNING, model.getStatus());
        assertEquals("corr-1", model.getCorrelationId());
        assertEquals(5, model.getPriority());
        assertEquals(Long.valueOf(1000L), model.getCreateTime());
        // updateTime (common) must land on updatedTime (engine model).
        assertEquals(Long.valueOf(2000L), model.getUpdatedTime());
        // getWorkflowName() reads the definition — this is what the DAO calls and would NPE on.
        assertNotNull(model.getWorkflowDefinition());
        assertEquals("sample_wf", model.getWorkflowName());
        assertEquals("v", model.getInput().get("k"));

        assertEquals(1, model.getTasks().size());
        TaskModel tm = model.getTasks().get(0);
        assertEquals("task-1", tm.getTaskId());
        assertEquals(TaskModel.Status.SCHEDULED, tm.getStatus());
        assertEquals("ref_1", tm.getReferenceTaskName());
        assertEquals("wf-1", tm.getWorkflowInstanceId());
        assertEquals("b", tm.getInputData().get("a"));
    }

    @Test
    void roundTripsBackToCommonModel() {
        Workflow original = sampleWorkflow();
        Workflow back = ModelConverter.toWorkflowModel(original).toWorkflow();

        assertEquals(original.getWorkflowId(), back.getWorkflowId());
        assertEquals(original.getStatus(), back.getStatus());
        assertEquals(original.getCorrelationId(), back.getCorrelationId());
        assertEquals(original.getTasks().size(), back.getTasks().size());
        assertEquals(original.getTasks().get(0).getStatus(), back.getTasks().get(0).getStatus());
        assertEquals(original.getTasks().get(0).getTaskId(), back.getTasks().get(0).getTaskId());
    }

    @Test
    void failsLoudlyWhenWorkflowDefinitionMissing() {
        Workflow wf = new Workflow();
        wf.setWorkflowId("no-def");
        wf.setStatus(Workflow.WorkflowStatus.RUNNING);

        assertThrows(IllegalArgumentException.class, () -> ModelConverter.toWorkflowModel(wf));
    }
}
