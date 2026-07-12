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

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

/**
 * Writes definitions to the destination. Two implementations exist, selected by {@code
 * migrator.metadata.writer}: {@code rest} (via the destination Conductor's REST API, cache
 * consistent on a live server) and {@code jdbc} (direct to the {@code meta_*} tables via the
 * embedded DAO). Each implementation owns its own idempotency handling (the REST and JDBC APIs have
 * different create-vs-update semantics). Source reads always go through REST regardless.
 */
public interface MetadataWriter {

    void writeTaskDefs(List<TaskDef> taskDefs);

    void writeWorkflowDefs(List<WorkflowDef> workflowDefs);

    void writeEventHandlers(List<EventHandler> eventHandlers);
}
