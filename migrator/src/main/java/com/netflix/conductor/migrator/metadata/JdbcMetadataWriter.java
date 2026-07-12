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
import com.netflix.conductor.postgres.dao.PostgresMetadataDAO;

/**
 * Writes definitions directly to the destination {@code meta_*} tables via the embedded {@link
 * PostgresMetadataDAO} — uniform with how execution state is written, and needs no destination REST
 * endpoint.
 *
 * <p>Two caveats vs. the REST writer, both acceptable for an up-front bulk definition load before
 * the destination serves traffic:
 *
 * <ul>
 *   <li>A running destination server caches task defs and refreshes on a ≤60s interval, so it sees
 *       task defs written here only after that refresh (workflow defs and event handlers are
 *       uncached → visible immediately).
 *   <li>This bypasses {@code MetadataService} validation and its create-time/updated-by stamping,
 *       so we set {@code createTime}/{@code updateTime} here to avoid zero timestamps.
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "migrator.metadata.writer", havingValue = "jdbc")
public class JdbcMetadataWriter implements MetadataWriter {

    private static final Logger log = LoggerFactory.getLogger(JdbcMetadataWriter.class);

    private final PostgresMetadataDAO metadataDao;

    public JdbcMetadataWriter(PostgresMetadataDAO destMetadataDao) {
        this.metadataDao = destMetadataDao;
    }

    @Override
    public void writeTaskDefs(List<TaskDef> taskDefs) {
        int created = 0;
        int updated = 0;
        long now = System.currentTimeMillis();
        for (TaskDef def : taskDefs) {
            boolean exists = metadataDao.getTaskDef(def.getName()) != null;
            if (exists) {
                def.setUpdateTime(now);
                updated++;
            } else {
                def.setCreateTime(now);
                created++;
            }
            // createTaskDef is an upsert (insert-or-update) on the task-def name.
            metadataDao.createTaskDef(def);
        }
        log.info(
                "Task defs (JDBC): {} created, {} updated ({} on source)",
                created,
                updated,
                taskDefs.size());
    }

    @Override
    public void writeWorkflowDefs(List<WorkflowDef> workflowDefs) {
        int created = 0;
        int updated = 0;
        long now = System.currentTimeMillis();
        for (WorkflowDef def : workflowDefs) {
            // Workflow defs are versioned — key the create-vs-update decision on (name, version).
            boolean exists =
                    metadataDao.getWorkflowDef(def.getName(), def.getVersion()).isPresent();
            if (exists) {
                def.setUpdateTime(now);
                metadataDao.updateWorkflowDef(def);
                updated++;
            } else {
                def.setCreateTime(now);
                metadataDao.createWorkflowDef(def);
                created++;
            }
        }
        log.info(
                "Workflow defs (JDBC): {} created, {} updated ({} on source)",
                created,
                updated,
                workflowDefs.size());
    }

    @Override
    public void writeEventHandlers(List<EventHandler> eventHandlers) {
        Set<String> existing =
                metadataDao.getAllEventHandlers().stream()
                        .map(EventHandler::getName)
                        .collect(Collectors.toSet());
        int created = 0;
        int updated = 0;
        for (EventHandler handler : eventHandlers) {
            if (existing.contains(handler.getName())) {
                metadataDao.updateEventHandler(handler);
                updated++;
            } else {
                metadataDao.addEventHandler(handler);
                created++;
            }
        }
        log.info(
                "Event handlers (JDBC): {} created, {} updated ({} on source)",
                created,
                updated,
                eventHandlers.size());
    }
}
