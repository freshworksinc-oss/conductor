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
package com.netflix.conductor.pgmirror;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code conductor.pg-mirror.*}. Everything is off unless {@code enabled=true}. */
@ConfigurationProperties(prefix = "conductor.pg-mirror")
public class PgMirrorProperties {

    /** Turns the mirror on. Needs a persistence other than postgres (conductor.db.type). */
    private boolean enabled = false;

    /**
     * Redis list the messages are pushed to. Default {@code pg-mirror:<conductor.app.stack>:sql}: a
     * name no Conductor key uses (Conductor keys start with the queue and workflow prefixes).
     */
    private String queueKey;

    /** Stop pushing while the list is longer than this (the applier is behind or down). */
    private long maxQueueLength = 2_000_000;

    /**
     * Connect and read timeout of a push. A push that takes longer is dropped, never waited for.
     */
    private int timeoutMs = 200;

    /** Open the circuit breaker after this many failed pushes in a row. */
    private int breakerFailures = 5;

    /** While the breaker is open pushes are skipped, for this long. */
    private long breakerOpenMs = 10_000;

    /**
     * Poll data is written on every task poll; the latest poll of each queue + domain is sent as
     * one message every this many ms.
     */
    private long pollDataFlushMs = 10_000;

    private final Redis redis = new Redis();

    public static class Redis {
        private String host = "localhost";
        private int port = 6379;
        private String username;
        private String password;
        private boolean ssl = false;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
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

        public boolean isSsl() {
            return ssl;
        }

        public void setSsl(boolean ssl) {
            this.ssl = ssl;
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getQueueKey() {
        return queueKey;
    }

    public void setQueueKey(String queueKey) {
        this.queueKey = queueKey;
    }

    public long getMaxQueueLength() {
        return maxQueueLength;
    }

    public void setMaxQueueLength(long maxQueueLength) {
        this.maxQueueLength = maxQueueLength;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public int getBreakerFailures() {
        return breakerFailures;
    }

    public void setBreakerFailures(int breakerFailures) {
        this.breakerFailures = breakerFailures;
    }

    public long getBreakerOpenMs() {
        return breakerOpenMs;
    }

    public void setBreakerOpenMs(long breakerOpenMs) {
        this.breakerOpenMs = breakerOpenMs;
    }

    public long getPollDataFlushMs() {
        return pollDataFlushMs;
    }

    public void setPollDataFlushMs(long pollDataFlushMs) {
        this.pollDataFlushMs = pollDataFlushMs;
    }

    public Redis getRedis() {
        return redis;
    }
}
