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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.retry.support.RetryTemplate;

import com.netflix.conductor.common.config.ObjectMapperProvider;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.postgres.config.PostgresProperties;
import com.netflix.conductor.postgres.dao.PostgresMetadataDAO;
import com.netflix.conductor.postgres.dao.PostgresPollDataDAO;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;

/**
 * Like {@link PgMirrorEquivalenceTest}, for the metadata (task and workflow definitions, event
 * handlers) and poll data: the real Postgres DAOs run on database A behind the real interceptors,
 * the statements they emit are applied to an empty database B, and the tables must be equal. Needs
 * PG_MIRROR_A_URL / PG_MIRROR_B_URL.
 */
public class PgMirrorMetadataEquivalenceTest {

    private static final String[][] TABLES = {
        {"meta_task_def", "name, json_data", "name"},
        {"meta_workflow_def", "name, version, latest_version, json_data", "name, version"},
        {"meta_event_handler", "name, event, active, json_data", "name"},
        // the poll time is taken by each side's own clock: compare everything but it
        {
            "poll_data",
            "queue_name, domain, (json_data::jsonb - 'lastPollTime')::text AS json_data",
            "queue_name, domain"
        },
    };

    private final ObjectMapper om = new ObjectMapperProvider().getObjectMapper();
    private DataSource a;
    private DataSource b;
    private PostgresMetadataDAO referenceMetadata;
    private PostgresPollDataDAO referencePollData;
    private MetadataDAO metadata;
    private EventHandlerDAO events;
    private PollDataDAO pollData;
    private PgMirrorPollDataDaoInterceptor pollInterceptor;
    private final List<PgMirrorMessage> captured = new ArrayList<>();
    private PgMirrorTestApplier applier;

    @Before
    public void setUp() throws Exception {
        String urlA = System.getenv("PG_MIRROR_A_URL");
        String urlB = System.getenv("PG_MIRROR_B_URL");
        Assume.assumeTrue(
                "PG_MIRROR_A_URL / PG_MIRROR_B_URL not set", urlA != null && urlB != null);
        a = ds(urlA);
        b = ds(urlB);
        for (DataSource d : new DataSource[] {a, b}) {
            try (Connection c = d.getConnection();
                    Statement s = c.createStatement()) {
                s.execute(
                        "TRUNCATE meta_task_def, meta_workflow_def, meta_event_handler, poll_data RESTART IDENTITY");
            }
        }
        PostgresProperties props = new PostgresProperties();
        referenceMetadata = new PostgresMetadataDAO(new RetryTemplate(), om, a, props);
        referencePollData = new PostgresPollDataDAO(new RetryTemplate(), om, a, props);
        PgMirrorSql sql = new PgMirrorSql(om);

        ProxyFactory m = new ProxyFactory(referenceMetadata);
        m.addAdvice(new PgMirrorMetadataDaoInterceptor(sql, captured::addAll));
        Object proxy = m.getProxy();
        metadata = (MetadataDAO) proxy;
        events = (EventHandlerDAO) proxy;

        pollInterceptor = new PgMirrorPollDataDaoInterceptor(sql, captured::addAll, 0);
        ProxyFactory p = new ProxyFactory(referencePollData);
        p.addAdvice(pollInterceptor);
        pollData = (PollDataDAO) p.getProxy();
        applier = new PgMirrorTestApplier(b);
    }

    @After
    public void tearDown() {
        if (referenceMetadata != null) {
            referenceMetadata.destroy();
        }
    }

    private static DataSource ds(String url) {
        PGSimpleDataSource d = new PGSimpleDataSource();
        d.setUrl(url);
        return d;
    }

    private void sync(String step) throws Exception {
        pollInterceptor.flush();
        for (PgMirrorMessage m : new ArrayList<>(captured)) {
            applier.apply(m);
        }
        captured.clear();
        for (String[] t : TABLES) {
            assertEquals(step + ": table " + t[0], rows(a, t), rows(b, t));
        }
    }

    private List<Map<String, String>> rows(DataSource d, String[] t) throws Exception {
        List<Map<String, String>> out = new ArrayList<>();
        try (Connection c = d.getConnection();
                Statement s = c.createStatement();
                ResultSet r =
                        s.executeQuery("SELECT " + t[1] + " FROM " + t[0] + " ORDER BY " + t[2])) {
            ResultSetMetaData md = r.getMetaData();
            while (r.next()) {
                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    row.put(md.getColumnLabel(i), r.getString(i));
                }
                out.add(row);
            }
        }
        return out;
    }

    private int count(DataSource d, String table) throws Exception {
        try (Connection c = d.getConnection();
                Statement s = c.createStatement();
                ResultSet r = s.executeQuery("SELECT count(*) FROM " + table)) {
            r.next();
            return r.getInt(1);
        }
    }

    private static TaskDef taskDef(String name, int retries) {
        TaskDef t = new TaskDef(name);
        t.setOwnerEmail("a@b.c");
        t.setRetryCount(retries);
        return t;
    }

    private static WorkflowDef workflowDef(String name, int version, String desc) {
        WorkflowDef w = new WorkflowDef();
        w.setName(name);
        w.setVersion(version);
        w.setOwnerEmail("a@b.c");
        w.setDescription(desc);
        WorkflowTask t = new WorkflowTask();
        t.setName("t1");
        t.setTaskReferenceName("t1");
        w.getTasks().add(t);
        return w;
    }

    private static EventHandler handler(String name, String event, boolean active) {
        EventHandler h = new EventHandler();
        h.setName(name);
        h.setEvent(event);
        h.setActive(active);
        return h;
    }

    @Test
    public void taskDefinitions() throws Exception {
        metadata.createTaskDef(taskDef("td_a", 1));
        metadata.createTaskDef(taskDef("td_b", 2));
        sync("create task defs");
        metadata.updateTaskDef(taskDef("td_a", 5));
        sync("update task def");
        metadata.removeTaskDef("td_b");
        sync("remove task def");
        assertEquals(1, count(b, "meta_task_def"));
    }

    @Test
    public void workflowDefinitionsKeepLatestVersion() throws Exception {
        metadata.createWorkflowDef(workflowDef("wd", 1, "v1"));
        sync("create v1");
        metadata.createWorkflowDef(workflowDef("wd", 2, "v2"));
        sync("create v2 (latest_version 2 on both rows)");
        metadata.updateWorkflowDef(workflowDef("wd", 1, "v1 changed"));
        sync("update v1 (latest stays 2)");
        metadata.createWorkflowDef(workflowDef("other", 1, "o"));
        sync("another workflow");
        metadata.removeWorkflowDef("wd", 2);
        sync("remove v2 (latest falls back to 1)");
        metadata.createWorkflowDef(workflowDef("wd", 3, "v3"));
        metadata.removeWorkflowDef("wd", 1);
        sync("create v3, remove v1");
        assertEquals(2, count(b, "meta_workflow_def"));
    }

    @Test
    public void eventHandlers() throws Exception {
        events.addEventHandler(handler("h1", "sqs:q1", true));
        events.addEventHandler(handler("h2", "sqs:q2", false));
        sync("add handlers");
        events.updateEventHandler(handler("h1", "sqs:q9", false));
        sync("update handler");
        events.removeEventHandler("h2");
        sync("remove handler");
        assertEquals(1, count(b, "meta_event_handler"));
    }

    @Test
    public void pollDataIsBufferedAndLatestWins() throws Exception {
        for (int i = 0; i < 50; i++) { // many polls, few rows
            pollData.updateLastPollData("q1", null, "worker-" + (i % 3));
            pollData.updateLastPollData("q1", "dom", "worker-x");
            pollData.updateLastPollData("q2", "dom", "worker-y");
        }
        sync("poll data");
        assertEquals(3, count(b, "poll_data"));
        pollData.updateLastPollData("q1", null, "worker-late");
        sync("another poll");
    }
}
