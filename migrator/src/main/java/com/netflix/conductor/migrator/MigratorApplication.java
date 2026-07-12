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
package com.netflix.conductor.migrator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;

import com.netflix.conductor.migrator.config.MigratorProperties;
import com.netflix.conductor.migrator.run.MigrationCommand;
import com.netflix.conductor.postgres.config.PostgresProperties;

/**
 * Standalone, temporary migrator service.
 *
 * <p>Component scanning is intentionally limited to this package so we do NOT pull in Conductor's
 * server auto-configuration (queues, sweeper, reconciler, etc.). We only borrow the persistence
 * classes, wired by hand in {@code config/DestPersistenceConfig} — which also supplies the shared
 * {@code ObjectMapper} (Conductor's own {@code ObjectMapperProvider}, including the protobuf {@code
 * JsonProtoModule}) used both to parse REST responses and to serialize {@code json_data}.
 */
@SpringBootApplication(exclude = FlywayAutoConfiguration.class)
@EnableConfigurationProperties({MigratorProperties.class, PostgresProperties.class})
public class MigratorApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(MigratorApplication.class, args);
        // Long-running (sync) mode: leave the JVM alive (web server + delta-sync thread). One-shot
        // modes (metadata / ids): close the context and force exit — some embedded DAOs start
        // non-daemon threads (e.g. PostgresMetadataDAO's cache refresh) that would otherwise hang.
        if (!ctx.getBean(MigrationCommand.class).isLongRunning()) {
            System.exit(SpringApplication.exit(ctx));
        }
    }
}
