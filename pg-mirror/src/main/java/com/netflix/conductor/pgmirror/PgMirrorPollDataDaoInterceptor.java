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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.metrics.Monitors;

/**
 * Mirrors updateLastPollData. A poll happens on every task poll, so one message per call would be
 * far too much: the latest poll of each queue + domain is kept in memory and sent as ONE message
 * every flush interval. Poll data is informational (an older value is replaced by the next poll),
 * so losing the last interval when the process dies is acceptable.
 */
public class PgMirrorPollDataDaoInterceptor implements MethodInterceptor {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PgMirrorPollDataDaoInterceptor.class);
    private static final String CLASS_NAME = "PgMirror";

    private final PgMirrorSql sql;
    private final PgMirrorPublisher publisher;
    private final Map<String, PollData> pending = new ConcurrentHashMap<>();
    private final ScheduledExecutorService flusher;

    /** flushIntervalMs <= 0: no timer, call {@link #flush()} yourself (tests). */
    public PgMirrorPollDataDaoInterceptor(
            PgMirrorSql sql, PgMirrorPublisher publisher, long flushIntervalMs) {
        this.sql = sql;
        this.publisher = publisher;
        if (flushIntervalMs > 0) {
            flusher =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "pg-mirror-poll-data");
                                t.setDaemon(true);
                                return t;
                            });
            flusher.scheduleWithFixedDelay(
                    this::flush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        } else {
            flusher = null;
        }
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        if (!"updateLastPollData".equals(invocation.getMethod().getName())) {
            return invocation.proceed();
        }
        Object result = invocation.proceed();
        try {
            Object[] a = invocation.getArguments();
            String queue = (String) a[0];
            String domain = (String) a[1];
            PollData p = new PollData(queue, domain, (String) a[2], System.currentTimeMillis());
            pending.put(queue + "||" + (domain == null ? "DEFAULT" : domain), p);
        } catch (Throwable t) {
            Monitors.error(CLASS_NAME, "updateLastPollData");
            LOGGER.warn("pg-mirror: could not buffer poll data", t);
        }
        return result;
    }

    /** Sends what is buffered as one message. Never throws. */
    public void flush() {
        try {
            List<PollData> polls = new ArrayList<>();
            for (String key : new ArrayList<>(pending.keySet())) {
                PollData p = pending.remove(key);
                if (p != null) {
                    polls.add(p);
                }
            }
            if (!polls.isEmpty()) {
                publisher.publish(List.of(sql.pollData(polls)));
            }
        } catch (Throwable t) {
            Monitors.error(CLASS_NAME, "updateLastPollData");
            LOGGER.warn("pg-mirror: could not mirror poll data", t);
        }
    }
}
