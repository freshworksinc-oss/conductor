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
package com.netflix.conductor.migrator.config;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.retry.backoff.NoBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.netflix.conductor.common.config.ObjectMapperProvider;
import com.netflix.conductor.postgres.config.PostgresProperties;
import com.netflix.conductor.postgres.dao.PostgresExecutionDAO;
import com.netflix.conductor.postgres.dao.PostgresIndexDAO;
import com.netflix.conductor.postgres.dao.PostgresMetadataDAO;
import com.netflix.conductor.postgres.dao.PostgresPollDataDAO;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Constructs the embedded destination persistence stack by hand (D2). We deliberately instantiate
 * {@link PostgresExecutionDAO} / {@link PostgresIndexDAO} directly rather than going through {@code
 * ExecutionDAOFacade}, which would drag in
 * QueueDAO/IndexDAO/PollDataDAO/ExternalPayloadStorageUtils we do not want.
 *
 * <p>No Flyway here — the destination schema is owned and migrated by the destination Conductor.
 * Defining our own {@link DataSource} bean also switches off Spring Boot's DataSource
 * auto-configuration, so no {@code spring.datasource.*} is required.
 */
@Configuration
public class DestPersistenceConfig {

    /**
     * Conductor's own fully-configured mapper (registers {@code JsonProtoModule} for protobuf
     * {@code Any} fields, JavaTime, Afterburner). {@link Primary} so it is used everywhere — both
     * to (de)serialize REST bodies and as the DAO's {@code json_data} mapper, matching Conductor.
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return new ObjectMapperProvider().getObjectMapper();
    }

    /** Destination Postgres pool. {@link Primary} so the DAOs and any autoconfig pick it up. */
    @Bean
    @Primary
    public DataSource destDataSource(MigratorProperties props) {
        MigratorProperties.DataSource ds = props.getDestDatasource();
        HikariDataSource hikari = new HikariDataSource();
        hikari.setJdbcUrl(ds.getUrl());
        hikari.setUsername(ds.getUsername());
        hikari.setPassword(ds.getPassword());
        hikari.setSchema(ds.getSchema());
        hikari.setPoolName("migrator-dest");
        return hikari;
    }

    /**
     * Mirrors {@code PostgresConfiguration#postgresRetryTemplate}: a few immediate retries, no
     * backoff. Good enough for idempotent imports (F1/F2).
     */
    @Bean
    public RetryTemplate destRetryTemplate() {
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(new NoBackOffPolicy());
        return retryTemplate;
    }

    @Bean
    public PostgresExecutionDAO destExecutionDao(
            RetryTemplate destRetryTemplate, ObjectMapper objectMapper, DataSource destDataSource) {
        return new PostgresExecutionDAO(destRetryTemplate, objectMapper, destDataSource);
    }

    @Bean
    public PostgresIndexDAO destIndexDao(
            RetryTemplate destRetryTemplate,
            ObjectMapper objectMapper,
            DataSource destDataSource,
            PostgresProperties postgresProperties) {
        return new PostgresIndexDAO(
                destRetryTemplate, objectMapper, destDataSource, postgresProperties);
    }

    /**
     * Only constructed when metadata is written via JDBC — this DAO starts a background task-def
     * cache-refresh scheduler, which we do not want to spin up in the default REST mode.
     */
    @Bean
    @ConditionalOnProperty(name = "migrator.metadata.writer", havingValue = "jdbc")
    public PostgresMetadataDAO destMetadataDao(
            RetryTemplate destRetryTemplate,
            ObjectMapper objectMapper,
            DataSource destDataSource,
            PostgresProperties postgresProperties) {
        return new PostgresMetadataDAO(
                destRetryTemplate, objectMapper, destDataSource, postgresProperties);
    }

    /**
     * Only constructed when poll-data migration is enabled. With {@code pollDataFlushInterval=0}
     * (the default) this writes immediately and starts no background flush scheduler.
     */
    @Bean
    @ConditionalOnProperty(name = "migrator.poll-data.enabled", havingValue = "true")
    public PostgresPollDataDAO destPollDataDao(
            RetryTemplate destRetryTemplate,
            ObjectMapper objectMapper,
            DataSource destDataSource,
            PostgresProperties postgresProperties) {
        return new PostgresPollDataDAO(
                destRetryTemplate, objectMapper, destDataSource, postgresProperties);
    }
}
