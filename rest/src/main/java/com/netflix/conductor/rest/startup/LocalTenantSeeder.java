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
package com.netflix.conductor.rest.startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.exception.ConflictException;

/**
 * TEMPORARY, local-dev-only: registers one workflow definition owned by "local-tenant" so
 * TenantMetadataDAO's ownerApp-scoped queries have something to return for the tenant that
 * UserInfoResource fakes. Delete both once real tenant data (or a real login flow) is in place -
 * this exists purely to unblock local UI testing.
 */
@Component
@Profile("local")
public class LocalTenantSeeder {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTenantSeeder.class);
    private static final String LOCAL_TENANT = "local-tenant";

    private final RestTemplate restTemplate;

    @Value("${server.port:8080}")
    private int port;

    public LocalTenantSeeder(RestTemplateBuilder restTemplateBuilder) {
        this.restTemplate = restTemplateBuilder.build();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void seedLocalTenantWorkflow() {
        WorkflowDef def = new WorkflowDef();
        def.setName("local_tenant_test_wf");
        def.setVersion(1);
        def.setOwnerApp(LOCAL_TENANT);
        def.setSchemaVersion(2);

        try {
            restTemplate.postForEntity(
                    "http://localhost:" + port + "/api/metadata/workflow", def, Object.class);
            LOGGER.info("Seeded workflow def for tenant '{}'", LOCAL_TENANT);
        } catch (ConflictException ignored) {
            // already seeded from a previous run
        } catch (Exception e) {
            LOGGER.warn("Failed to seed local tenant workflow def", e);
        }
    }
}
