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

    /** One-shot terminal-history back-fill settings (--history / mode=history). */
    private History history = new History();

    /** Source search scoping applied to enumeration (freeText, time window, tenant, status). */
    private Search search = new Search();

    /** Poll-data migration settings. */
    private PollData pollData = new PollData();

    /** Redis-repair (verifyAndRepair) settings — used by mode=repair. */
    private Repair repair = new Repair();

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

    public History getHistory() {
        return history;
    }

    public void setHistory(History history) {
        this.history = history;
    }

    public Search getSearch() {
        return search;
    }

    public void setSearch(Search search) {
        this.search = search;
    }

    public PollData getPollData() {
        return pollData;
    }

    public void setPollData(PollData pollData) {
        this.pollData = pollData;
    }

    public Repair getRepair() {
        return repair;
    }

    public void setRepair(Repair repair) {
        this.repair = repair;
    }

    /**
     * Redis-repair (mode=repair): re-arm non-terminal workflows on the destination's Redis via
     * verifyAndRepair, looping until a pass repairs nothing (converged). Repair only.
     */
    public static class Repair {
        /** Maximum re-arm passes before giving up (each pass re-enumerates the pending set). */
        private int maxPasses = 5;

        /** Delay between per-workflow verifyAndRepair calls (throttle the dest conductor). */
        private long throttleMs = 50;

        public int getMaxPasses() {
            return maxPasses;
        }

        public void setMaxPasses(int maxPasses) {
            this.maxPasses = maxPasses;
        }

        public long getThrottleMs() {
            return throttleMs;
        }

        public void setThrottleMs(long throttleMs) {
            this.throttleMs = throttleMs;
        }
    }

    /** Poll-data (last-poll-time per taskDefName/domain) migration. */
    public static class PollData {
        /**
         * When true, copy the source's poll data to the dest (one-shot). Written via the embedded
         * DAO's {@code updateLastPollData}, which stamps {@code lastPollTime=now} — acceptable, as
         * poll data is transient operational metadata that workers repopulate anyway.
         */
        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
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

    /** One-shot terminal-history back-fill (--history / mode=history). */
    public static class History {
        /**
         * Workflow statuses to enumerate and back-fill in history mode. Terminal statuses by
         * default. Comma-separated in properties, e.g. {@code
         * migrator.history.statuses=COMPLETED,FAILED,TERMINATED,TIMED_OUT}.
         */
        private List<String> statuses =
                List.of("COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT");

        /**
         * When true, walk the {@code search.start-time-from-ms}..{@code -to-ms} range (default 0..now)
         * in {@code window-days} slices, adaptively subdividing any slice that would exceed
         * OpenSearch's 10k max_result_window — so a large history migrates in one run.
         */
        private boolean autoChunk = false;

        /** Initial time-slice size (days) for auto-chunk; halved automatically when a slice caps. */
        private int windowDays = 7;

        public List<String> getStatuses() {
            return statuses;
        }

        public void setStatuses(List<String> statuses) {
            this.statuses = statuses;
        }

        public boolean isAutoChunk() {
            return autoChunk;
        }

        public void setAutoChunk(boolean autoChunk) {
            this.autoChunk = autoChunk;
        }

        public int getWindowDays() {
            return windowDays;
        }

        public void setWindowDays(int windowDays) {
            this.windowDays = windowDays;
        }
    }

    /**
     * Scoping applied to the source {@code /api/workflow/search} enumeration. Some backends (sip's
     * OpenSearch proxy) return nothing for {@code status IN (...)} but work with a {@code startTime}
     * range + {@code freeText=*} — hence these are all tunable.
     */
    public static class Search {
        /** {@code freeText} query param; {@code *} matches all. Some backends need it set. */
        private String freeText = "*";

        /** {@code sort} query param (e.g. {@code startTime:DESC}). */
        private String sort = "startTime:DESC";

        /**
         * When true, include the per-mode {@code status IN (...)} clause. Set false for backends
         * where status filtering doesn't translate (then enumerate by time window only).
         */
        private boolean statusFilter = true;

        /** If &gt; 0, restrict to {@code startTime > this} (epoch ms). */
        private long startTimeFromMs = 0;

        /** If &gt; 0, restrict to {@code startTime < this} (epoch ms) — use to chunk large ranges. */
        private long startTimeToMs = 0;

        public String getFreeText() {
            return freeText;
        }

        public void setFreeText(String freeText) {
            this.freeText = freeText;
        }

        public String getSort() {
            return sort;
        }

        public void setSort(String sort) {
            this.sort = sort;
        }

        public boolean isStatusFilter() {
            return statusFilter;
        }

        public void setStatusFilter(boolean statusFilter) {
            this.statusFilter = statusFilter;
        }

        public long getStartTimeFromMs() {
            return startTimeFromMs;
        }

        public void setStartTimeFromMs(long startTimeFromMs) {
            this.startTimeFromMs = startTimeFromMs;
        }

        public long getStartTimeToMs() {
            return startTimeToMs;
        }

        public void setStartTimeToMs(long startTimeToMs) {
            this.startTimeToMs = startTimeToMs;
        }
    }

    /**
     * A Conductor REST endpoint plus optional headers. {@code authHeader} is sent as
     * Authorization; {@code tenantId} is sent as {@code x-tenant-id} — required when the endpoint
     * is the edge/auth-proxy, which uses it to tenant-scope search/metadata/access.
     */
    public static class Endpoint {
        private String url;
        private String authHeader = "";
        private String tenantId = "";

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

        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(String tenantId) {
            this.tenantId = tenantId;
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
