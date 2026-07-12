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
package com.netflix.conductor.migrator.run;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.migrator.metadata.MetadataWriter;
import com.netflix.conductor.migrator.rest.SourceClient;

/**
 * Copies definitions from source to destination (§5.4 precondition): task defs → workflow defs →
 * event handlers, in that order (task defs first, since workflow defs reference them).
 *
 * <p>Source reads always go through REST ({@link SourceClient}); the write channel is pluggable via
 * {@link MetadataWriter} (REST or JDBC, selected by {@code migrator.metadata.writer}). Each writer
 * is idempotent, so this is safe to run before every execution-migration batch.
 */
@Component
public class MetadataMigrator {

    private static final Logger log = LoggerFactory.getLogger(MetadataMigrator.class);

    private final SourceClient source;
    private final MetadataWriter writer;

    public MetadataMigrator(SourceClient source, MetadataWriter writer) {
        this.source = source;
        this.writer = writer;
    }

    public void migrate() {
        writer.writeTaskDefs(source.getTaskDefs());
        writer.writeWorkflowDefs(source.getWorkflowDefs());
        writer.writeEventHandlers(source.getEventHandlers());
        log.info("Definition migration complete");
    }
}
