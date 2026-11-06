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

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.databind.ObjectMapper;

// Postgres-backed tenant metadata, used when conductor.db.type=postgres (reads the meta_* tables).
// Other persistences (Scylla) use DaoTenantMetadataConfiguration and need no Postgres.
// conductor.tenant-metadata.enabled=false drops /api/tenant/metadata.
@Configuration
@Import(DataSourceAutoConfiguration.class)
@ConditionalOnExpression(TenantMetadataConditions.POSTGRES)
public class TenantMetadataConfiguration {

    @Bean
    public TenantMetadataDAO tenantMetadataDAO(DataSource dataSource, ObjectMapper objectMapper) {
        return new PostgresTenantMetadataDAO(dataSource, objectMapper);
    }
}
