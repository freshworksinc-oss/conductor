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

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Optionally creates the destination Postgres schema by running Conductor's own {@code
 * postgres-persistence} Flyway migrations against the dest datasource — so the destination
 * Conductor does not need to have run first. Gated by {@code migrator.dest-datasource.init-schema}.
 * Uses the same migration scripts (and version) the dest Conductor ships, so there is no schema
 * drift and the dest Conductor's later Flyway run is a no-op/baseline.
 */
@Component
public class SchemaInitializer {

    private static final Logger log = LoggerFactory.getLogger(SchemaInitializer.class);

    private final DataSource destDataSource;
    private final MigratorProperties props;

    public SchemaInitializer(DataSource destDataSource, MigratorProperties props) {
        this.destDataSource = destDataSource;
        this.props = props;
    }

    public void initIfEnabled() {
        if (!props.getDestDatasource().isInitSchema()) {
            return;
        }
        String schema = props.getDestDatasource().getSchema();
        log.info("Initializing destination schema via Flyway (schema={})", schema);
        Flyway flyway =
                Flyway.configure()
                        .dataSource(destDataSource)
                        .schemas(schema)
                        .locations(
                                "classpath:db/migration_postgres",
                                "classpath:db/migration_postgres_data")
                        .baselineOnMigrate(true)
                        .outOfOrder(true)
                        .load();
        var result = flyway.migrate();
        log.info(
                "Destination schema ready: {} migration(s) applied, schema at version {}",
                result.migrationsExecuted,
                result.targetSchemaVersion);
    }
}
