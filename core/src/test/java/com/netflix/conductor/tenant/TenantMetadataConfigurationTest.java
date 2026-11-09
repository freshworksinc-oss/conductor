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
package com.netflix.conductor.tenant;

import javax.sql.DataSource;

import org.junit.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.netflix.conductor.dao.MetadataDAO;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/** The tenant metadata implementation follows conductor.db.type; enabled=false switches it off. */
public class TenantMetadataConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withBean(MetadataDAO.class, () -> mock(MetadataDAO.class))
                    .withUserConfiguration(
                            DaoTenantMetadataConfiguration.class,
                            TenantMetadataConfiguration.class);

    private ApplicationContextRunner withPostgres() {
        return runner.withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(ObjectMapper.class, ObjectMapper::new);
    }

    @Test
    public void postgresPersistenceUsesThePostgresTables() {
        withPostgres()
                .withPropertyValues("conductor.db.type=postgres")
                .run(
                        ctx -> {
                            assertEquals(1, ctx.getBeansOfType(TenantMetadataDAO.class).size());
                            assertTrue(
                                    ctx.getBean(TenantMetadataDAO.class)
                                            instanceof PostgresTenantMetadataDAO);
                        });
    }

    @Test
    public void scyllaPersistenceUsesItsOwnDefinitionsWithoutPostgres() {
        // no DataSource bean and no tenant-metadata property: Scylla just works
        runner.withPropertyValues("conductor.db.type=scylla")
                .run(
                        ctx -> {
                            assertEquals(1, ctx.getBeansOfType(TenantMetadataDAO.class).size());
                            assertTrue(
                                    ctx.getBean(TenantMetadataDAO.class)
                                            instanceof DaoTenantMetadataDAO);
                        });
    }

    @Test
    public void everyOtherPersistenceUsesItsOwnDefinitions() {
        for (String type : new String[] {"redis_standalone", "cassandra", "memory"}) {
            runner.withPropertyValues("conductor.db.type=" + type)
                    .run(
                            ctx ->
                                    assertTrue(
                                            ctx.getBean(TenantMetadataDAO.class)
                                                    instanceof DaoTenantMetadataDAO));
        }
        // db.type not set at all
        runner.run(
                ctx ->
                        assertTrue(
                                ctx.getBean(TenantMetadataDAO.class)
                                        instanceof DaoTenantMetadataDAO));
    }

    @Test
    public void enabledFalseServesNothingForAnyPersistence() {
        for (String type : new String[] {"postgres", "scylla"}) {
            withPostgres()
                    .withPropertyValues(
                            "conductor.db.type=" + type, "conductor.tenant-metadata.enabled=false")
                    .run(ctx -> assertFalse(ctx.containsBean("tenantMetadataDAO")));
        }
    }
}
