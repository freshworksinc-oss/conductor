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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.metrics.Monitors;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Pushes messages to one Redis list with a single RPUSH. Protects the Conductor from the mirror:
 *
 * <ul>
 *   <li>short timeouts, so a slow Redis costs at most {@code timeoutMs} per write;
 *   <li>a circuit breaker: after a few failed pushes in a row the pushes are skipped for a while;
 *   <li>a backlog cap: while the list is too long (nobody is applying it) pushes are skipped.
 * </ul>
 *
 * A skipped or failed push loses that message. That is accepted: the reconciliation compares both
 * databases and repairs what was missed. Failures are counted and logged (once a minute), never
 * thrown.
 */
public class RedisPgMirrorPublisher implements PgMirrorPublisher, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisPgMirrorPublisher.class);
    private static final String CLASS_NAME = "PgMirror";

    private final RedisListClient redis;
    private final String key;
    private final long maxQueueLength;
    private final int breakerFailures;
    private final long breakerOpenMs;
    private final ObjectMapper json = new ObjectMapper();
    private final LongSupplier clock;
    private ScheduledExecutorService scheduler;

    private volatile boolean full = false;
    private volatile long breakerOpenUntil = 0;
    private int consecutiveFailures = 0;

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong skippedBreaker = new AtomicLong();
    private final AtomicLong skippedFull = new AtomicLong();
    private volatile long lastWarnAt = 0;

    public RedisPgMirrorPublisher(
            RedisListClient redis, String key, PgMirrorProperties p, LongSupplier clock) {
        this.redis = redis;
        this.key = key;
        this.maxQueueLength = p.getMaxQueueLength();
        this.breakerFailures = Math.max(1, p.getBreakerFailures());
        this.breakerOpenMs = p.getBreakerOpenMs();
        this.clock = clock;
    }

    /** Starts the background check of the list length and the once-a-minute summary. */
    public void start() {
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "pg-mirror-monitor");
                            t.setDaemon(true);
                            return t;
                        });
        scheduler.scheduleWithFixedDelay(this::checkLength, 5, 5, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::logSummary, 60, 60, TimeUnit.SECONDS);
    }

    @Override
    public void publish(List<PgMirrorMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        int n = messages.size();
        if (full) {
            skippedFull.addAndGet(n);
            return;
        }
        if (clock.getAsLong() < breakerOpenUntil) {
            skippedBreaker.addAndGet(n);
            return;
        }
        try {
            List<String> payloads = new ArrayList<>(n);
            for (PgMirrorMessage m : messages) {
                payloads.add(json.writeValueAsString(m));
            }
            redis.rpush(key, payloads);
            published.addAndGet(n);
            consecutiveFailures = 0;
        } catch (Throwable t) {
            failed.addAndGet(n);
            Monitors.error(CLASS_NAME, "publish");
            if (++consecutiveFailures >= breakerFailures) {
                breakerOpenUntil = clock.getAsLong() + breakerOpenMs;
                consecutiveFailures = 0;
                warn(
                        "pg-mirror: Redis push failed {} times in a row, skipping pushes for {} ms: {}",
                        breakerFailures,
                        breakerOpenMs,
                        t.toString());
            } else {
                warn(
                        "pg-mirror: Redis push failed (the change is not mirrored; reconciliation will repair it): {}",
                        t.toString());
            }
        }
    }

    private void checkLength() {
        try {
            boolean nowFull = redis.llen(key) > maxQueueLength;
            if (nowFull != full) {
                LOGGER.warn(
                        "pg-mirror: queue {} {} (cap {})",
                        key,
                        nowFull
                                ? "is over its cap, pushes are skipped"
                                : "is under its cap again, pushing again",
                        maxQueueLength);
            }
            full = nowFull;
        } catch (Throwable t) {
            // the push path reports Redis problems; this check just keeps the last answer
            LOGGER.debug("pg-mirror: could not read the queue length", t);
        }
    }

    private void logSummary() {
        if (failed.get() + skippedBreaker.get() + skippedFull.get() > 0) {
            LOGGER.warn(
                    "pg-mirror totals: published={} failed={} skippedBreaker={} skippedFull={}",
                    published.get(),
                    failed.get(),
                    skippedBreaker.get(),
                    skippedFull.get());
        } else {
            LOGGER.info("pg-mirror totals: published={}", published.get());
        }
    }

    private void warn(String format, Object... args) {
        long now = clock.getAsLong();
        if (now - lastWarnAt >= 10_000) { // do not flood the log during an outage
            lastWarnAt = now;
            LOGGER.warn(format, args);
        }
    }

    public long published() {
        return published.get();
    }

    public long failed() {
        return failed.get();
    }

    public long skippedBreaker() {
        return skippedBreaker.get();
    }

    public long skippedFull() {
        return skippedFull.get();
    }

    /** Runs the length check now (tests). */
    void checkLengthNow() {
        checkLength();
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        redis.close();
    }
}
