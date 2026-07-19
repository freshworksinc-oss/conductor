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
import java.util.stream.Collectors;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.model.BulkResponse;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.migrator.config.MigratorProperties;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * REST client for the SOURCE Conductor. Read-only export here, plus the bulk pause/resume/terminate
 * calls that the (deferred) flip phase will use — the core run does not pause the source.
 */
@Component
public class SourceClient {

    private final WebClient client;
    private final ObjectMapper mapper;

    public SourceClient(MigratorProperties props, ObjectMapper mapper) {
        this.client = ConductorWebClient.forEndpoint(props.getSource());
        this.mapper = mapper;
    }

    /** Full workflow export including tasks, parentWorkflowId and the workflowDefinition. */
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
        return readValue(json, Workflow.class);
    }

    /** Enumerate non-terminal (RUNNING/PAUSED) workflow ids via the search index. */
    public List<String> searchNonTerminalIds(int start, int size) {
        return searchIds("status IN (RUNNING,PAUSED)", start, size);
    }

    /**
     * Enumerate workflow ids matching an arbitrary Conductor search query (one page). Used by
     * history mode to enumerate terminal statuses; the query is passed through to
     * {@code /api/workflow/search}.
     */
    public List<String> searchIds(String query, int start, int size) {
        String json =
                client.get()
                        .uri(
                                uriBuilder ->
                                        uriBuilder
                                                .path("/api/workflow/search")
                                                .queryParam("start", start)
                                                .queryParam("size", size)
                                                .queryParam("query", query)
                                                .build())
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();
        SearchResult<WorkflowSummary> result =
                readValue(json, new TypeReference<SearchResult<WorkflowSummary>>() {});
        if (result == null || result.getResults() == null) {
            return List.of();
        }
        return result.getResults().stream()
                .map(WorkflowSummary::getWorkflowId)
                .collect(Collectors.toList());
    }

    // ---- metadata (definitions) ----

    public List<TaskDef> getTaskDefs() {
        String json =
                client.get()
                        .uri("/api/metadata/taskdefs")
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();
        return readValue(json, new TypeReference<List<TaskDef>>() {});
    }

    public List<WorkflowDef> getWorkflowDefs() {
        String json =
                client.get()
                        .uri("/api/metadata/workflow")
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();
        return readValue(json, new TypeReference<List<WorkflowDef>>() {});
    }

    public List<EventHandler> getEventHandlers() {
        String json = client.get().uri("/api/event").retrieve().bodyToMono(String.class).block();
        return readValue(json, new TypeReference<List<EventHandler>>() {});
    }

    public BulkResponse<String> bulkPause(List<String> workflowIds) {
        return bulk("/api/workflow/bulk/pause", "PUT", workflowIds);
    }

    public BulkResponse<String> bulkResume(List<String> workflowIds) {
        return bulk("/api/workflow/bulk/resume", "PUT", workflowIds);
    }

    public BulkResponse<String> bulkTerminate(List<String> workflowIds, String reason) {
        String json =
                client.post()
                        .uri(
                                uriBuilder ->
                                        uriBuilder
                                                .path("/api/workflow/bulk/terminate")
                                                .queryParam("reason", reason)
                                                .build())
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(writeValue(workflowIds))
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();
        return readValue(json, new TypeReference<BulkResponse<String>>() {});
    }

    private BulkResponse<String> bulk(String path, String method, List<String> workflowIds) {
        WebClient.RequestBodySpec spec =
                ("PUT".equals(method) ? client.put() : client.post()).uri(path);
        String json =
                spec.contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(writeValue(workflowIds))
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();
        return readValue(json, new TypeReference<BulkResponse<String>>() {});
    }

    private <T> T readValue(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse source response as " + type, e);
        }
    }

    private <T> T readValue(String json, TypeReference<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse source response as " + type, e);
        }
    }

    private String writeValue(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize request body", e);
        }
    }
}
