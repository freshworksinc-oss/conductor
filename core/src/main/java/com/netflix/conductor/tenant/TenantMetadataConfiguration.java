package com.netflix.conductor.tenant;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;

// Conductor excludes DataSourceAutoConfiguration and only postgres persistence re-imports it;
// tenant metadata always reads from Postgres, so import it here for non-SQL db types (e.g. scylla).
// Set conductor.tenant-metadata.enabled=false to run without Postgres (drops /api/tenant/metadata).
@Configuration
@Import(DataSourceAutoConfiguration.class)
@ConditionalOnProperty(
        name = "conductor.tenant-metadata.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class TenantMetadataConfiguration {

    @Bean
    public TenantMetadataDAO tenantMetadataDAO(DataSource dataSource, ObjectMapper objectMapper) {
        return new TenantMetadataDAO(dataSource, objectMapper);
    }
}
