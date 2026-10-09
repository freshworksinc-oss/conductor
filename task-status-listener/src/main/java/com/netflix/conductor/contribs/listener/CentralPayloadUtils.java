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
     * Central consumers need tenant identity as a top-level field alongside {@code correlationId}
     * and {@code domain}, not buried inside {@code input}. {@code input} is left untouched (it
     * stays whatever shape the summary serialization produced) — this only reads {@code
     * _tenantContext} out of it and copies (not moves) it to the payload root, so existing
     * consumers reading {@code input._tenantContext} keep working.
     */
    public static void exposeTenantContextAtRoot(
            ObjectMapper objectMapper, ObjectNode payload, String idField) {
        JsonNode inputNode = payload.get("input");
        JsonNode tenantContext = null;
        if (inputNode instanceof ObjectNode) {
            tenantContext = inputNode.get("_tenantContext");
        } else if (inputNode != null && inputNode.isTextual()) {
            try {
                tenantContext = objectMapper.readTree(inputNode.asText()).get("_tenantContext");
            } catch (IOException e) {
                LOGGER.debug(
                        "input is not valid JSON; cannot extract _tenantContext for {}",
                        payload.path(idField).asText());
            }
        }
        if (tenantContext != null) {
            payload.set("_tenantContext", tenantContext);
        }
    }
}
