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

import org.springframework.http.HttpHeaders;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.WebClient;

import com.netflix.conductor.migrator.config.MigratorProperties;

/**
 * Builds a plain {@link WebClient} for a Conductor endpoint. We deliberately transfer bodies as raw
 * JSON strings and (de)serialize with the shared Conductor-parity {@code ObjectMapper} in the
 * clients, rather than relying on WebClient's Jackson codec — the codec does not reliably register
 * for the common model types inside the shaded boot jar. Adds the optional Authorization header and
 * (when set) the {@code x-tenant-id} header — required when going through the edge/auth-proxy, which
 * uses it to tenant-scope search, metadata listing and workflow access.
 */
public final class ConductorWebClient {

    private ConductorWebClient() {}

    public static WebClient forEndpoint(MigratorProperties.Endpoint endpoint) {
        WebClient.Builder builder =
                WebClient.builder()
                        .baseUrl(endpoint.getUrl())
                        // Workflow exports can be large; lift the 256 KB in-memory default.
                        .codecs(config -> config.defaultCodecs().maxInMemorySize(64 * 1024 * 1024));

        if (StringUtils.hasText(endpoint.getAuthHeader())) {
            builder.defaultHeader(HttpHeaders.AUTHORIZATION, endpoint.getAuthHeader());
        }
        if (StringUtils.hasText(endpoint.getTenantId())) {
            builder.defaultHeader("x-tenant-id", endpoint.getTenantId());
        }
        return builder.build();
    }
}
