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
package com.netflix.conductor.tracing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WorkflowExecutionTracingTest {

    @Test
    public void noopFacadeDoesNotInjectOrRecordSpans() {
        RecordingTracingFacade tracing = new RecordingTracingFacade(false);
        WorkflowExecutionTracing workflowTracing = new WorkflowExecutionTracing(tracing);
        Map<String, Object> variables = new HashMap<>();

        workflowTracing.injectTraceContext(variables);
        workflowTracing.decide(workflow("wf-1", variables), () -> {});
        workflowTracing.enqueueDecider(workflow("wf-1", variables), () -> {});
        workflowTracing.enqueueTask(task("task-1", "wf-1"), () -> {});

        assertTrue(variables.isEmpty());
        assertTrue(tracing.getSpanNames().isEmpty());
    }

    @Test
    public void injectTraceContextStoresW3cCarrierUnderTraceContextKey() {
        RecordingTracingFacade tracing = new RecordingTracingFacade(true);
        WorkflowExecutionTracing workflowTracing = new WorkflowExecutionTracing(tracing);
        Map<String, Object> variables = new HashMap<>();

        workflowTracing.injectTraceContext(variables);

        assertTrue(variables.containsKey(TracingFacade.TRACE_CONTEXT_VARIABLE_KEY));
        Object stored = variables.get(TracingFacade.TRACE_CONTEXT_VARIABLE_KEY);
        assertTrue(stored instanceof Map<?, ?>);
        assertTrue(((Map<?, ?>) stored).containsKey("traceparent"));
    }

    @Test
    public void decideRestoresStoredContextAndRecordsSpan() {
        RecordingTracingFacade tracing = new RecordingTracingFacade(true);
        WorkflowExecutionTracing workflowTracing = new WorkflowExecutionTracing(tracing);
        Map<String, Object> variables = new HashMap<>();
        workflowTracing.injectTraceContext(variables);

        workflowTracing.decide(workflow("wf-2", variables), () -> {});

        List<String> spans = tracing.getSpanNames();
        assertEquals(List.of("withContext", "workflow.decide"), spans);
    }

    @Test
    public void enqueueDeciderAndEnqueueTaskRecordExecutionSpans() {
        RecordingTracingFacade tracing = new RecordingTracingFacade(true);
        WorkflowExecutionTracing workflowTracing = new WorkflowExecutionTracing(tracing);
        Map<String, Object> variables = new HashMap<>();

        workflowTracing.enqueueDecider(workflow("wf-3", variables), () -> {});
        workflowTracing.enqueueTask(task("task-3", "wf-3"), () -> {});

        assertEquals(List.of("workflow.enqueue_decider", "task.enqueue"), tracing.getSpanNames());
    }

    @Test
    public void decideTagsWorkflowStatusWhenTerminalAfterDecide() {
        RecordingTracingFacade tracing = new RecordingTracingFacade(true);
        WorkflowExecutionTracing workflowTracing = new WorkflowExecutionTracing(tracing);
        WorkflowModel workflow = workflow("wf-5", new HashMap<>());
        workflow.setStatus(WorkflowModel.Status.RUNNING);

        workflowTracing.decide(workflow, () -> workflow.setStatus(WorkflowModel.Status.COMPLETED));

        assertEquals("COMPLETED", tracing.getTag("workflow.status"));
    }

    @Test
    public void decideDoesNotTagWorkflowStatusWhileStillRunning() {
        RecordingTracingFacade tracing = new RecordingTracingFacade(true);
        WorkflowExecutionTracing workflowTracing = new WorkflowExecutionTracing(tracing);
        WorkflowModel workflow = workflow("wf-6", new HashMap<>());
        workflow.setStatus(WorkflowModel.Status.RUNNING);

        workflowTracing.decide(workflow, () -> {});

        assertNull(tracing.getTag("workflow.status"));
    }

    @Test
    public void noTraceContextStillRecordsDecideSpan() {
        RecordingTracingFacade tracing = new RecordingTracingFacade(true);
        WorkflowExecutionTracing workflowTracing = new WorkflowExecutionTracing(tracing);

        workflowTracing.decide(workflow("wf-4", new HashMap<>()), () -> {});

        assertFalse(tracing.getSpanNames().contains("withContext"));
        assertEquals(List.of("workflow.decide"), tracing.getSpanNames());
    }

    private static WorkflowModel workflow(String workflowId, Map<String, Object> variables) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("pilot_workflow");
        workflowDef.setVersion(1);

        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId(workflowId);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setCorrelationId("corr-1");
        workflow.setVariables(variables);
        return workflow;
    }

    private static TaskModel task(String taskId, String workflowId) {
        TaskModel task = new TaskModel();
        task.setTaskId(taskId);
        task.setWorkflowInstanceId(workflowId);
        task.setTaskType("SIMPLE");
        return task;
    }
}
