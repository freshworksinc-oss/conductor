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

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.netflix.conductor.dao.MetadataDAO;

/**
 * Tenant metadata served from the persistence in use, through {@link MetadataDAO}: no Postgres and
 * no DataSource. Used automatically for every {@code conductor.db.type} except {@code postgres}
 * (Scylla); Postgres is {@link TenantMetadataConfiguration}.
 */
@Configuration
@ConditionalOnExpression(TenantMetadataConditions.METADATA_DAO)
public class DaoTenantMetadataConfiguration {

    @Bean
    public TenantMetadataDAO tenantMetadataDAO(MetadataDAO metadataDAO) {
        return new DaoTenantMetadataDAO(metadataDAO);
    }
}
