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
package com.netflix.conductor.migrator.rest;

import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.model.BulkResponse;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.migrator.config.MigratorProperties;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * REST client for the DESTINATION Conductor. Used to register definitions, bootstrap an imported
 * (dormant) workflow — the "wake-up" call — and to read it back for verification.
 */
@Component
public class DestClient {

    private final WebClient client;
    private final ObjectMapper mapper;

    public DestClient(MigratorProperties props, ObjectMapper mapper) {
        this.client = ConductorWebClient.forEndpoint(props.getDest());
        this.mapper = mapper;
    }

    /**
     * The bootstrap call (F6/F7): recreates the decider-queue entry AND the task-queue entries so
     * the destination engine starts owning execution of this workflow.
     */
    public String verifyAndRepair(String workflowId) {
        return client.post()
                .uri("/api/admin/consistency/verifyAndRepair/{id}", workflowId)
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }

    /** Read the imported workflow back for post-import verification. */
    public Workflow getWorkflow(String workflowId) {
        String json =
                client.get()
                        .uri(
                                uriBuilder ->
                                        uriBuilder
                                                .path("/api/workflow/{id}")
                                                .queryParam("includeTasks", true)
                                                .build(workflowId))
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();
        try {
            return mapper.readValue(json, Workflow.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse dest workflow response", e);
        }
    }

    // ---- metadata (definitions) ----

    public List<TaskDef> getTaskDefs() {
        return getList("/api/metadata/taskdefs", new TypeReference<List<TaskDef>>() {});
    }

    /** POST bulk create — fails if any already exists, so only send new ones. */
    public void createTaskDefs(List<TaskDef> defs) {
        client.post()
                .uri("/api/metadata/taskdefs")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(write(defs))
                .retrieve()
                .toBodilessEntity()
                .block();
    }

    /** PUT single update — requires the def to already exist. */
    public void updateTaskDef(TaskDef def) {
        client.put()
                .uri("/api/metadata/taskdefs")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(write(def))
                .retrieve()
                .toBodilessEntity()
                .block();
    }

    /** PUT list — bulk upsert of workflow defs; per-def result in the BulkResponse. */
    public BulkResponse<String> updateWorkflowDefs(List<WorkflowDef> defs) {
        String json =
                client.put()
                        .uri("/api/metadata/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(write(defs))
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();
        return read(json, new TypeReference<BulkResponse<String>>() {});
    }

    public List<EventHandler> getEventHandlers() {
        return getList("/api/event", new TypeReference<List<EventHandler>>() {});
    }

    public void createEventHandler(EventHandler handler) {
        client.post()
                .uri("/api/event")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(write(handler))
                .retrieve()
                .toBodilessEntity()
                .block();
    }

    public void updateEventHandler(EventHandler handler) {
        client.put()
                .uri("/api/event")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(write(handler))
                .retrieve()
                .toBodilessEntity()
                .block();
    }

    private <T> T getList(String path, TypeReference<T> type) {
        String json = client.get().uri(path).retrieve().bodyToMono(String.class).block();
        return read(json, type);
    }

    private <T> T read(String json, TypeReference<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse dest response as " + type, e);
        }
    }

    private String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize request body", e);
        }
    }
}
