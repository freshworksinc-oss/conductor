/*
 * Copyright 2020 Conductor Authors.
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
package com.netflix.conductor.rest.controllers;

import org.junit.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.netflix.conductor.service.TaskService;
import com.netflix.conductor.tenant.TenantMetadataDAO;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * The controller is served for every persistence unless conductor.tenant-metadata.enabled=false.
 */
public class TenantMetadataResourceConditionTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withBean(TenantMetadataDAO.class, () -> mock(TenantMetadataDAO.class))
                    .withBean(TaskService.class, () -> mock(TaskService.class))
                    .withUserConfiguration(TenantMetadataResource.class);

    @Test
    public void servedByDefaultForEveryPersistence() {
        runner.run(ctx -> assertTrue(ctx.containsBean("tenantMetadataResource")));
        runner.withPropertyValues("conductor.db.type=postgres")
                .run(ctx -> assertTrue(ctx.containsBean("tenantMetadataResource")));
        runner.withPropertyValues("conductor.db.type=scylla")
                .run(ctx -> assertTrue(ctx.containsBean("tenantMetadataResource")));
    }

    @Test
    public void notServedWhenEnabledIsFalse() {
        runner.withPropertyValues("conductor.tenant-metadata.enabled=false")
                .run(ctx -> assertFalse(ctx.containsBean("tenantMetadataResource")));
    }
}
