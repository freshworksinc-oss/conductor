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
package com.netflix.conductor.migrator.payload;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.migrator.config.MigratorProperties;

/**
 * Default payload handler. When {@code migrator.payload-enabled=false} it is a no-op but warns if
 * it ever sees an external payload pointer (so a misconfiguration surfaces loudly rather than
 * silently importing a dangling pointer). When enabled it fails fast, because real resolution is
 * not yet implemented (P0 spike item).
 */
@Component
public class NoopPayloadHandler implements PayloadHandler {

    private static final Logger log = LoggerFactory.getLogger(NoopPayloadHandler.class);

    private final boolean enabled;

    public NoopPayloadHandler(MigratorProperties props) {
        this.enabled = props.isPayloadEnabled();
    }

    @Override
    public void handle(Workflow workflow) {
        boolean hasExternal = hasExternalPayload(workflow);
        if (!hasExternal) {
            return;
        }
        if (enabled) {
            throw new UnsupportedOperationException(
                    "Workflow "
                            + workflow.getWorkflowId()
                            + " carries external payload pointers but real PayloadHandler resolution"
                            + " is not implemented yet (P0 spike item).");
        }
        log.warn(
                "Workflow {} carries external payload pointers but migrator.payload-enabled=false;"
                        + " the destination copy will reference the SOURCE payload store. Enable and"
                        + " implement PayloadHandler before migrating such workflows.",
                workflow.getWorkflowId());
    }

    private boolean hasExternalPayload(Workflow workflow) {
        if (StringUtils.isNotBlank(workflow.getExternalInputPayloadStoragePath())
                || StringUtils.isNotBlank(workflow.getExternalOutputPayloadStoragePath())) {
            return true;
        }
        for (Task task : workflow.getTasks()) {
            if (StringUtils.isNotBlank(task.getExternalInputPayloadStoragePath())
                    || StringUtils.isNotBlank(task.getExternalOutputPayloadStoragePath())) {
                return true;
            }
        }
        return false;
    }
}
