/*
 * Copyright 2024 Conductor Authors.
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
package com.netflix.conductor.contribs.listener;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Shared payload shaping for task and workflow notifications published to Central. Lives here
 * (task-status-listener) rather than workflow-event-listener because workflow-event-listener
 * already depends on this module, not the other way around.
 */
public final class CentralPayloadUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CentralPayloadUtils.class);

    private CentralPayloadUtils() {}

    /**
     * The summary {@code input}/{@code output} fields are Strings that already contain serialized
     * JSON, so they show up double-encoded (an escaped JSON string) in the payload. Central expects
     * real nested objects, so this replaces the String node with the parsed JSON when the content
     * is valid JSON. If it is not valid JSON (e.g. Java {@code toString()} when {@code
     * conductor.app.summary-input-output-json-serialization.enabled=false}) the original string is
     * kept so publishing never fails.
     */
    public static void inlineJsonString(
            ObjectMapper objectMapper, ObjectNode payload, String field, String idField) {
        JsonNode value = payload.get(field);
        if (value != null && value.isTextual()) {
            String text = value.asText();
            if (text != null && !text.isEmpty()) {
                try {
                    payload.set(field, objectMapper.readTree(text));
                } catch (IOException e) {
                    LOGGER.debug(
                            "Field '{}' is not valid JSON; leaving as string for {}",
                            field,
                            payload.path(idField).asText());
                }
            }
        }
    }

    /**
     * Central consumers need tenant identity as a top-level field alongside {@code correlationId}
     * and {@code domain}, not buried inside {@code input}. Copy (rather than move) it so existing
     * consumers reading {@code input._tenantContext} keep working.
     */
    public static void exposeTenantContextAtRoot(ObjectNode payload) {
        JsonNode inputNode = payload.get("input");
        if (inputNode instanceof ObjectNode) {
            JsonNode tenantContext = inputNode.get("_tenantContext");
            if (tenantContext != null) {
                payload.set("_tenantContext", tenantContext);
            }
        }
    }
}
