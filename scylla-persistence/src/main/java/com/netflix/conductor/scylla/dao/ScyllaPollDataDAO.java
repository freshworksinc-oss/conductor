/*
 * Copyright 2022 Conductor Authors.
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
package com.netflix.conductor.scylla.dao;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.scylla.config.ScyllaProperties;
import com.netflix.conductor.scylla.util.Statements;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import jakarta.annotation.PreDestroy;

import static com.netflix.conductor.scylla.util.Constants.DEFAULT_POLL_DOMAIN;
import static com.netflix.conductor.scylla.util.Constants.JSON_DATA_KEY;

/**
 * Poll data in Scylla: table poll_data, partition per queue, one row per domain ("DEFAULT" when the
 * poll had no domain, as in Postgres).
 *
 * <p>updateLastPollData is called on every task poll, so writes are buffered in memory and flushed
 * every {@code conductor.scylla.poll-data-flush-interval} (only the latest value per queue + domain
 * is written). Reads see buffered values first, so they are never older than what this node
 * received.
 *
 * <p>Tenant scoping is by task domain (the auth proxy prefixes poll domains with the tenant and
 * filters poll-data responses), the same as with Postgres — no tenant column is needed.
 */
public class ScyllaPollDataDAO extends ScyllaBaseDAO implements PollDataDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaPollDataDAO.class);
    private static final String CLASS_NAME = ScyllaPollDataDAO.class.getSimpleName();

    private final PreparedStatement insertPollDataStatement;
    private final PreparedStatement selectPollDataStatement;
    private final PreparedStatement selectPollDataForQueueStatement;
    private final PreparedStatement selectAllPollDataStatement;

    // "queue\u0000domain" -> latest PollData not yet written
    private final Map<String, PollData> pending = new ConcurrentHashMap<>();
    private final ScheduledExecutorService flusher;

    public ScyllaPollDataDAO(
            Session session,
            ObjectMapper objectMapper,
            ScyllaProperties properties,
            Statements statements) {
        super(session, objectMapper, properties);
        this.insertPollDataStatement =
                session.prepare(statements.getInsertPollDataStatement())
                        .setConsistencyLevel(properties.getWriteConsistencyLevel());
        this.selectPollDataStatement =
                session.prepare(statements.getSelectPollDataStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectPollDataForQueueStatement =
                session.prepare(statements.getSelectPollDataForQueueStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());
        this.selectAllPollDataStatement =
                session.prepare(statements.getSelectAllPollDataStatement())
                        .setConsistencyLevel(properties.getReadConsistencyLevel());

        long intervalMs = properties.getPollDataFlushInterval().toMillis();
        if (intervalMs > 0) {
            flusher =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "scylla-polldata-flush");
                                t.setDaemon(true);
                                return t;
                            });
            flusher.scheduleWithFixedDelay(
                    this::flush, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        } else {
            flusher = null;
        }
    }

    @Override
    public void updateLastPollData(String taskDefName, String domain, String workerId) {
        Preconditions.checkNotNull(taskDefName, "taskDefName cannot be null");
        PollData pollData = new PollData(taskDefName, domain, workerId, System.currentTimeMillis());
        if (flusher == null) {
            write(pollData);
        } else {
            pending.put(key(taskDefName, effectiveDomain(domain)), pollData);
        }
    }

    @Override
    public PollData getPollData(String taskDefName, String domain) {
        Preconditions.checkNotNull(taskDefName, "taskDefName cannot be null");
        String d = effectiveDomain(domain);
        PollData buffered = pending.get(key(taskDefName, d));
        if (buffered != null) {
            return buffered;
        }
        try {
            recordCassandraDaoRequests("getPollData");
            Row row = session.execute(selectPollDataStatement.bind(taskDefName, d)).one();
            return row == null ? null : readValue(row.getString(JSON_DATA_KEY), PollData.class);
        } catch (DriverException e) {
            throw error("getPollData", "Failed to get poll data for " + taskDefName + "/" + d, e);
        }
    }

    @Override
    public List<PollData> getPollData(String taskDefName) {
        Preconditions.checkNotNull(taskDefName, "taskDefName cannot be null");
        try {
            recordCassandraDaoRequests("getPollDataForQueue");
            return withPending(
                    session.execute(selectPollDataForQueueStatement.bind(taskDefName)).all(),
                    taskDefName);
        } catch (DriverException e) {
            throw error("getPollDataForQueue", "Failed to get poll data for " + taskDefName, e);
        }
    }

    @Override
    public List<PollData> getAllPollData() {
        try {
            recordCassandraDaoRequests("getAllPollData");
            return withPending(session.execute(selectAllPollDataStatement.bind()).all(), null);
        } catch (DriverException e) {
            throw error("getAllPollData", "Failed to get all poll data", e);
        }
    }

    /** Writes buffered poll data; an entry is dropped only if it was not replaced meanwhile. */
    void flush() {
        for (Map.Entry<String, PollData> e : pending.entrySet()) {
            try {
                write(e.getValue());
                pending.remove(e.getKey(), e.getValue());
            } catch (RuntimeException ex) {
                LOGGER.warn("Poll data flush failed for {}, will retry", e.getKey(), ex);
            }
        }
    }

    @PreDestroy
    public void close() {
        if (flusher != null) {
            flusher.shutdown();
            flush();
        }
    }

    private void write(PollData pollData) {
        try {
            recordCassandraDaoRequests("updateLastPollData");
            session.execute(
                    insertPollDataStatement.bind(
                            pollData.getQueueName(),
                            effectiveDomain(pollData.getDomain()),
                            toJson(pollData)));
        } catch (DriverException e) {
            throw error(
                    "updateLastPollData",
                    "Failed to update poll data for " + pollData.getQueueName(),
                    e);
        }
    }

    /** Stored rows with buffered (newer) values laid over them; queue == null means all queues. */
    private List<PollData> withPending(List<Row> rows, String queue) {
        Map<String, PollData> byKey = new LinkedHashMap<>();
        for (Row row : rows) {
            PollData pd = readValue(row.getString(JSON_DATA_KEY), PollData.class);
            byKey.put(key(pd.getQueueName(), effectiveDomain(pd.getDomain())), pd);
        }
        pending.forEach(
                (k, pd) -> {
                    if (queue == null || queue.equals(pd.getQueueName())) {
                        byKey.put(k, pd);
                    }
                });
        return new ArrayList<>(byKey.values());
    }

    private TransientException error(String method, String message, DriverException e) {
        Monitors.error(CLASS_NAME, method);
        LOGGER.error(message, e);
        return new TransientException(message, e);
    }

    private static String effectiveDomain(String domain) {
        return domain == null ? DEFAULT_POLL_DOMAIN : domain;
    }

    private static String key(String queue, String domain) {
        return queue + '\u0000' + domain;
    }
}
