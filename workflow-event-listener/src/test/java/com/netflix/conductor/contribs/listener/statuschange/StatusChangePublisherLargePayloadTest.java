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
package com.netflix.conductor.contribs.listener.statuschange;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.utils.SummaryUtil;
import com.netflix.conductor.contribs.listener.RestClientManager;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Verifies StatusChangePublisher can parse a large real workflow JSON and reach the Central POST
 * step (serialization + enqueue + consumer), without failing earlier.
 */
public class StatusChangePublisherLargePayloadTest {

    private static final Path WORKFLOW_JSON =
            Path.of(
                            "..",
                            "docs",
                            "central-workflow-completed-input-only-aa5598c7.json")
                    .normalize()
                    .toAbsolutePath();

    private RestClientManager restClientManager;
    private ExecutionDAOFacade executionDAOFacade;
    private ObjectMapper objectMapper;

    @Before
    public void setUp() throws Exception {
        restClientManager = mock(RestClientManager.class);
        executionDAOFacade = mock(ExecutionDAOFacade.class);
        objectMapper = new ObjectMapper();

        Field jsonSerialization =
                SummaryUtil.class.getDeclaredField("isSummaryInputOutputJsonSerializationEnabled");
        jsonSerialization.setAccessible(true);
        jsonSerialization.set(null, true);
    }

    @After
    public void tearDown() throws Exception {
        Field jsonSerialization =
                SummaryUtil.class.getDeclaredField("isSummaryInputOutputJsonSerializationEnabled");
        jsonSerialization.setAccessible(true);
        jsonSerialization.set(null, false);
    }

    @Test
    public void testLargeWorkflowJson_reachesCentralPostWithFullInput() throws Exception {
        File workflowFile = WORKFLOW_JSON.toFile();
        assertTrue(
                "Workflow fixture missing at " + WORKFLOW_JSON,
                workflowFile.exists());

        long fileBytes = workflowFile.length();
        System.out.println("=== Fixture file size: " + (fileBytes / 1024) + " KB ===");

        // Step 1: parse workflow JSON (API export). WorkflowDef has protobuf types that break
        // full WorkflowModel deserialization, so map only fields StatusChangePublisher uses.
        JsonNode root = objectMapper.readTree(workflowFile);
        WorkflowModel workflow = toWorkflowModel(root);
        assertNotNull(workflow.getWorkflowId());
        assertEquals(WorkflowModel.Status.COMPLETED, workflow.getStatus());
        assertNotNull(workflow.getWorkflowDefinition());
        assertFalse("workflow.input must be present", workflow.getInput().isEmpty());

        long inputJsonBytes =
                objectMapper.writeValueAsString(workflow.getInput()).getBytes(StandardCharsets.UTF_8).length;
        System.out.println(
                "=== Parsed workflowId="
                        + workflow.getWorkflowId()
                        + ", tasks="
                        + workflow.getTasks().size()
                        + ", input="
                        + (inputJsonBytes / 1024)
                        + " KB ===");

        // Step 2: serialization path used before HTTP (StatusChangeNotification)
        StatusChangeNotification notification =
                new StatusChangeNotification(workflow.toWorkflow());
        String summaryJson = notification.toJsonStringWithInputOutput();
        assertTrue(summaryJson.contains("\"input\""));
        System.out.println(
                "=== WorkflowSummary JSON (with input/output): "
                        + (summaryJson.getBytes(StandardCharsets.UTF_8).length / 1024)
                        + " KB ===");

        // Step 3: Central envelope build (same as publishStatusChangeNotification)
        Object accountId = workflow.getInput().get("accountId");
        JsonNode existingPayload = objectMapper.readTree(summaryJson);
        assertTrue(existingPayload instanceof ObjectNode);

        ObjectNode centralMessage = objectMapper.createObjectNode();
        centralMessage.put("account_id", String.valueOf(accountId));
        centralMessage.put("payload_type", "conductor_workflow_status");
        centralMessage.put("payload_version", "1.0");
        centralMessage.set("payload", existingPayload);

        String wrappedJson = centralMessage.toString();
        int wrappedKb = wrappedJson.getBytes(StandardCharsets.UTF_8).length / 1024;
        System.out.println("=== Central envelope size: " + wrappedKb + " KB ===");
        assertTrue("Central envelope should be large but buildable", wrappedKb > 100);

        // Step 4: StatusChangePublisher consumer thread -> RestClientManager.postNotification
        AtomicReference<String> postedBody = new AtomicReference<>();
        AtomicReference<Throwable> postError = new AtomicReference<>();
        CountDownLatch posted = new CountDownLatch(1);

        doAnswer(
                invocation -> {
                    try {
                        postedBody.set(invocation.getArgument(1));
                        posted.countDown();
                    } catch (Throwable t) {
                        postError.set(t);
                        posted.countDown();
                    }
                    return null;
                })
                .when(restClientManager)
                .postNotification(
                        eq(RestClientManager.NotificationType.WORKFLOW),
                        anyString(),
                        eq(workflow.getWorkflowId()),
                        any());

        List<String> subscribedStatuses = Arrays.asList("COMPLETED");
        StatusChangePublisher publisher =
                new StatusChangePublisher(
                        restClientManager, executionDAOFacade, subscribedStatuses);

        publisher.onWorkflowCompleted(workflow);

        assertTrue(
                "Timed out waiting for Central POST; failure likely before postNotification",
                posted.await(30, TimeUnit.SECONDS));
        assertNull("postNotification callback error: " + postError.get(), postError.get());

        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(restClientManager, timeout(30_000).times(1))
                .postNotification(
                        eq(RestClientManager.NotificationType.WORKFLOW),
                        bodyCaptor.capture(),
                        eq(workflow.getWorkflowId()),
                        any());

        String postedPayload = postedBody.get();
        assertNotNull(postedPayload);
        int postedKb = postedPayload.getBytes(StandardCharsets.UTF_8).length / 1024;
        System.out.println("=== POST reached mock Central, body size: " + postedKb + " KB ===");

        JsonNode postedNode = objectMapper.readTree(postedPayload);
        assertEquals("conductor_workflow_status", postedNode.get("payload_type").asText());
        assertEquals(String.valueOf(accountId), postedNode.get("account_id").asText());
        assertTrue(postedNode.get("payload").get("input").asText().contains("eventId"));

        // Tasks from fixture are NOT part of Central workflow payload
        assertFalse(postedPayload.contains("\"taskType\""));
    }

    /** Mirrors what Conductor has in memory when completeWorkflow() notifies the listener. */
    private static WorkflowModel toWorkflowModel(JsonNode root) {
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId(root.get("workflowId").asText());
        workflow.setStatus(WorkflowModel.Status.valueOf(root.get("status").asText()));
        workflow.setCreateTime(root.get("createTime").asLong());
        workflow.setEndTime(root.get("endTime").asLong());
        if (root.hasNonNull("updateTime")) {
            workflow.setUpdatedTime(root.get("updateTime").asLong());
        }
        if (root.hasNonNull("correlationId")) {
            workflow.setCorrelationId(root.get("correlationId").asText());
        }
        if (root.has("taskToDomain")) {
            workflow.setTaskToDomain(
                    new ObjectMapper()
                            .convertValue(
                                    root.get("taskToDomain"),
                                    new TypeReference<java.util.Map<String, String>>() {}));
        }
        if (root.has("priority")) {
            workflow.setPriority(root.get("priority").asInt());
        }

        ObjectMapper mapper = new ObjectMapper();
        workflow.setInput(
                mapper.convertValue(
                        root.get("input"), new TypeReference<java.util.Map<String, Object>>() {}));
        if (root.has("output")) {
            workflow.setOutput(
                    mapper.convertValue(
                            root.get("output"),
                            new TypeReference<java.util.Map<String, Object>>() {}));
        }

        JsonNode defNode = root.get("workflowDefinition");
        WorkflowDef def = new WorkflowDef();
        def.setName(defNode.get("name").asText());
        def.setVersion(defNode.get("version").asInt());
        if (defNode.has("workflowStatusListenerEnabled")) {
            def.setWorkflowStatusListenerEnabled(
                    defNode.get("workflowStatusListenerEnabled").asBoolean());
        } else {
            def.setWorkflowStatusListenerEnabled(true);
        }
        workflow.setWorkflowDefinition(def);
        return workflow;
    }
}
