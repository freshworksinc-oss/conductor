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

import java.util.Collections;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** All operator-tunable settings for a migration run. Bound from {@code migrator.*}. */
@ConfigurationProperties("migrator")
public class MigratorProperties {

    /**
     * Operations to run when no CLI operation flag is given (config-driven / k8s auto-start).
     * Values: {@code metadata}, {@code sync}. Example: {@code migrator.mode=metadata,sync}. CLI
     * flags ({@code --metadata}/{@code --sync}/{@code --ids}) still take precedence when present.
     */
    private List<String> mode = Collections.emptyList();

    public List<String> getMode() {
        return mode;
    }

    public void setMode(List<String> mode) {
        this.mode = mode;
    }

    /** SOURCE Conductor cluster (read + pause/terminate) — accessed via REST only (D3). */
    private Endpoint source = new Endpoint();

    /** DESTINATION Conductor cluster (bootstrap: verifyAndRepair / decide). */
    private Endpoint dest = new Endpoint();

    /** DESTINATION Postgres, written directly through the embedded DAO (D2). */
    private DataSource destDatasource = new DataSource();

    /** How many workflow trees to process per run/batch. */
    private int batchSize = 10;

    /** Whether to resolve external payload pointers on import (F10). Off = no-op handler. */
    private boolean payloadEnabled = false;

    /** Metadata (definitions) migration settings. */
    private Metadata metadata = new Metadata();

    /** Continuous delta-sync settings (--sync mode). */
    private Sync sync = new Sync();

    public Endpoint getSource() {
        return source;
    }

    public void setSource(Endpoint source) {
        this.source = source;
    }

    public Endpoint getDest() {
        return dest;
    }

    public void setDest(Endpoint dest) {
        this.dest = dest;
    }

    public DataSource getDestDatasource() {
        return destDatasource;
    }

    public void setDestDatasource(DataSource destDatasource) {
        this.destDatasource = destDatasource;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isPayloadEnabled() {
        return payloadEnabled;
    }

    public void setPayloadEnabled(boolean payloadEnabled) {
        this.payloadEnabled = payloadEnabled;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Sync getSync() {
        return sync;
    }

    public void setSync(Sync sync) {
        this.sync = sync;
    }

    /** Metadata write-channel selection. */
    public static class Metadata {
        /**
         * How definitions are written to the destination: {@code rest} (via the dest Conductor's
         * REST API — cache-consistent on a live server) or {@code jdbc} (direct to the meta_*
         * tables via the embedded DAO — uniform with execution writes, but a running dest server
         * sees new task defs only after its ≤60s cache refresh).
         */
        private String writer = "rest";

        public String getWriter() {
            return writer;
        }

        public void setWriter(String writer) {
            this.writer = writer;
        }
    }

    /** Continuous delta-sync (--sync) settings. */
    public static class Sync {
        /** Seconds between delta-sync passes. */
        private int intervalSeconds = 15;

        public int getIntervalSeconds() {
            return intervalSeconds;
        }

        public void setIntervalSeconds(int intervalSeconds) {
            this.intervalSeconds = intervalSeconds;
        }
    }

    /** A Conductor REST endpoint plus an optional pre-formed Authorization header value. */
    public static class Endpoint {
        private String url;
        private String authHeader = "";

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getAuthHeader() {
            return authHeader;
        }

        public void setAuthHeader(String authHeader) {
            this.authHeader = authHeader;
        }
    }

    /** JDBC coordinates for the destination Postgres. */
    public static class DataSource {
        private String url;
        private String username;
        private String password;
        private String schema = "public";

        /**
         * When true, run Flyway (Conductor's {@code postgres-persistence} migrations) against this
         * datasource before migrating — so the destination schema exists without needing the
         * destination Conductor to have run. Off by default (schema normally owned by dest
         * Conductor).
         */
        private boolean initSchema = false;

        public boolean isInitSchema() {
            return initSchema;
        }

        public void setInitSchema(boolean initSchema) {
            this.initSchema = initSchema;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }
    }
}
