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
package com.netflix.conductor.migrator.metadata;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.model.BulkResponse;
import com.netflix.conductor.migrator.rest.DestClient;

/**
 * Writes definitions through the destination Conductor's REST API. Runs inside the destination
 * server's request path, so its in-memory metadata caches stay consistent immediately. This is the
 * default writer.
 */
@Component
@ConditionalOnProperty(
        name = "migrator.metadata.writer",
        havingValue = "rest",
        matchIfMissing = true)
public class RestMetadataWriter implements MetadataWriter {

    private static final Logger log = LoggerFactory.getLogger(RestMetadataWriter.class);

    private final DestClient dest;

    public RestMetadataWriter(DestClient dest) {
        this.dest = dest;
    }

    @Override
    public void writeTaskDefs(List<TaskDef> taskDefs) {
        // POST creates (fails if exists), PUT updates (fails if absent) — split by what's on dest.
        Set<String> existing =
                dest.getTaskDefs().stream().map(TaskDef::getName).collect(Collectors.toSet());
        List<TaskDef> toCreate =
                taskDefs.stream()
                        .filter(d -> !existing.contains(d.getName()))
                        .collect(Collectors.toList());
        List<TaskDef> toUpdate =
                taskDefs.stream()
                        .filter(d -> existing.contains(d.getName()))
                        .collect(Collectors.toList());
        if (!toCreate.isEmpty()) {
            dest.createTaskDefs(toCreate);
        }
        toUpdate.forEach(dest::updateTaskDef);
        log.info(
                "Task defs (REST): {} created, {} updated ({} on source)",
                toCreate.size(),
                toUpdate.size(),
                taskDefs.size());
    }

    @Override
    public void writeWorkflowDefs(List<WorkflowDef> workflowDefs) {
        if (workflowDefs.isEmpty()) {
            log.info("Workflow defs (REST): none on source");
            return;
        }
        // PUT /workflow is a bulk upsert across all versions; per-def outcome in the BulkResponse.
        BulkResponse<String> response = dest.updateWorkflowDefs(workflowDefs);
        log.info(
                "Workflow defs (REST): {} registered, {} failed ({} on source)",
                response.getBulkSuccessfulResults().size(),
                response.getBulkErrorResults().size(),
                workflowDefs.size());
        if (!response.getBulkErrorResults().isEmpty()) {
            log.warn("Workflow def failures: {}", response.getBulkErrorResults());
        }
    }

    @Override
    public void writeEventHandlers(List<EventHandler> eventHandlers) {
        Set<String> existing =
                dest.getEventHandlers().stream()
                        .map(EventHandler::getName)
                        .collect(Collectors.toSet());
        int created = 0;
        int updated = 0;
        for (EventHandler handler : eventHandlers) {
            if (existing.contains(handler.getName())) {
                dest.updateEventHandler(handler);
                updated++;
            } else {
                dest.createEventHandler(handler);
                created++;
            }
        }
        log.info(
                "Event handlers (REST): {} created, {} updated ({} on source)",
                created,
                updated,
                eventHandlers.size());
    }
}
