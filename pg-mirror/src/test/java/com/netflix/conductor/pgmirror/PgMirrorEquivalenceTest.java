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
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.postgres.dao.PostgresExecutionDAO;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * The proof that the mirror writes what the Postgres Conductor would have written itself.
 *
 * <p>Every operation is run through the real {@link PostgresExecutionDAO} on database A (the
 * reference), wrapped by the real interceptor. The statements the interceptor emits for those same
 * calls are applied to an empty database B. After each step every execution table of A and B must
 * hold the same rows (surrogate ids and timestamps aside).
 *
 * <p>Needs two Postgres databases with the Conductor's Postgres schema (all migrations of
 * postgres-persistence), given as JDBC URLs in {@code PG_MIRROR_A_URL} and {@code PG_MIRROR_B_URL};
 * skipped when they are not set.
 */
public class PgMirrorEquivalenceTest {

    private static final String[][] TABLES = {
        {"workflow", "workflow_id, correlation_id, json_data", "workflow_id"},
        {"task", "task_id, json_data", "task_id"},
        {"workflow_pending", "workflow_type, workflow_id", "workflow_type, workflow_id"},
        {"workflow_to_task", "workflow_id, task_id", "workflow_id, task_id"},
        {
            "workflow_def_to_workflow",
            "workflow_def, date_str, workflow_id",
            "workflow_def, date_str, workflow_id"
        },
        {"task_scheduled", "workflow_id, task_key, task_id", "workflow_id, task_key"},
        {
            "task_in_progress",
            "task_def_name, task_id, workflow_id, in_progress_status",
            "task_def_name, task_id"
        },
        {
            "event_execution",
            "event_handler_name, event_name, message_id, execution_id, json_data",
            "event_handler_name, event_name, execution_id"
        },
    };

    private final ObjectMapper om = new ObjectMapperProvider().getObjectMapper();
    private DataSource a;
    private DataSource b;
    private PostgresExecutionDAO reference;
    private ExecutionDAO dao; // the reference DAO behind the mirror interceptor
    private final List<PgMirrorMessage> captured = new ArrayList<>();
    private PgMirrorTestApplier applier;
    private PgMirrorSql sql;

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
                        "TRUNCATE workflow, task, workflow_pending, workflow_to_task, workflow_def_to_workflow, "
                                + "task_scheduled, task_in_progress, event_execution RESTART IDENTITY");
            }
        }
        reference = new PostgresExecutionDAO(new RetryTemplate(), om, a, mock(QueueDAO.class));
        sql = new PgMirrorSql(om);
        ProxyFactory factory = new ProxyFactory(reference);
        factory.addAdvice(new PgMirrorExecutionDaoInterceptor(reference, sql, captured::addAll));
        dao = (ExecutionDAO) factory.getProxy();
        applier = new PgMirrorTestApplier(b);
    }

    @After
    public void tearDown() {
        if (reference != null) {
            reference.destroy();
        }
    }

    private static DataSource ds(String url) {
        PGSimpleDataSource d = new PGSimpleDataSource();
        d.setUrl(url);
        return d;
    }

    // ---------------- helpers ----------------

    private WorkflowModel workflow(String id, String name) {
        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setVersion(1);
        WorkflowModel w = new WorkflowModel();
        w.setWorkflowDefinition(def);
        w.setWorkflowId(id);
        w.setCorrelationId("ceapps:");
        w.setStatus(WorkflowModel.Status.RUNNING);
        w.setCreateTime(1_760_000_000_000L);
        w.setUpdatedTime(1_760_000_001_000L);
        w.getInput().put("k", "v");
        return w;
    }

    private TaskModel task(String id, String wfId, String ref, int retry, int concurrencyLimit) {
        TaskModel t = new TaskModel();
        t.setTaskId(id);
        t.setWorkflowInstanceId(wfId);
        t.setReferenceTaskName(ref);
        t.setTaskType("SIMPLE");
        t.setTaskDefName("def_" + ref);
        t.setRetryCount(retry);
        t.setStatus(TaskModel.Status.SCHEDULED);
        t.setUpdateTime(1_760_000_002_000L);
        WorkflowTask wt = new WorkflowTask();
        wt.setName("def_" + ref);
        wt.setTaskReferenceName(ref);
        if (concurrencyLimit > 0) {
            TaskDef td = new TaskDef("def_" + ref);
            td.setConcurrentExecLimit(concurrencyLimit);
            wt.setTaskDefinition(td);
        }
        t.setWorkflowTask(wt);
        return t;
    }

    /** Applies everything captured so far to B, then requires A and B to be equal. */
    private void sync(String step) throws Exception {
        for (PgMirrorMessage m : new ArrayList<>(captured)) {
            applier.apply(m);
        }
        captured.clear();
        assertSame(step);
    }

    private void assertSame(String step) throws Exception {
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
                    row.put(md.getColumnName(i), r.getString(i));
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

    // ---------------- scenarios ----------------

    @Test
    public void workflowAndTasksFromCreationToCompletion() throws Exception {
        WorkflowModel w = workflow("wf-1", "order_flow");
        dao.createWorkflow(w);
        sync("createWorkflow");
        assertEquals(1, count(b, "workflow_pending"));

        TaskModel t1 = task("t-1", "wf-1", "first", 0, 0);
        TaskModel t2 =
                task("t-2", "wf-1", "limited", 0, 2); // task definition with a concurrency limit
        dao.createTasks(List.of(t1, t2));
        sync("createTasks");
        assertEquals(2, count(b, "task_in_progress"));

        t1.setStatus(TaskModel.Status.IN_PROGRESS);
        t1.setUpdateTime(1_760_000_003_000L);
        dao.updateTask(t1);
        sync("updateTask t1 IN_PROGRESS");

        t2.setStatus(TaskModel.Status.IN_PROGRESS);
        t2.setUpdateTime(1_760_000_003_500L);
        dao.updateTask(t2);
        sync("updateTask t2 IN_PROGRESS (limited: in_progress_status = true)");

        t1.setStatus(TaskModel.Status.COMPLETED);
        t1.setUpdateTime(1_760_000_004_000L);
        dao.updateTask(t1);
        sync("updateTask t1 COMPLETED (leaves task_in_progress)");
        assertEquals(1, count(b, "task_in_progress"));

        t2.setStatus(TaskModel.Status.COMPLETED);
        t2.setUpdateTime(1_760_000_004_500L);
        dao.updateTask(t2);
        sync("updateTask t2 COMPLETED");

        w.setStatus(WorkflowModel.Status.COMPLETED);
        w.setUpdatedTime(1_760_000_005_000L);
        w.setTasks(List.of(t1, t2));
        dao.updateWorkflow(w);
        sync("updateWorkflow COMPLETED (leaves workflow_pending)");
        assertEquals(0, count(b, "workflow_pending"));
        assertEquals(1, count(b, "workflow"));
    }

    @Test
    public void retryAddsANewTaskWithItsOwnKey() throws Exception {
        dao.createWorkflow(workflow("wf-2", "retry_flow"));
        TaskModel first = task("t-a", "wf-2", "step", 0, 0);
        dao.createTasks(List.of(first));
        first.setStatus(TaskModel.Status.FAILED);
        first.setUpdateTime(1_760_000_003_000L);
        dao.updateTask(first);
        TaskModel retry = task("t-b", "wf-2", "step", 1, 0); // same reference name, retryCount 1
        dao.createTasks(List.of(retry));
        sync("retry");
        assertEquals(2, count(b, "task_scheduled"));
        assertEquals(1, count(b, "task_in_progress"));
    }

    @Test
    public void removeTaskAndRemoveWorkflowLeaveNothingBehind() throws Exception {
        WorkflowModel w = workflow("wf-3", "remove_flow");
        dao.createWorkflow(w);
        TaskModel t1 = task("t-x", "wf-3", "one", 0, 0);
        TaskModel t2 = task("t-y", "wf-3", "two", 0, 3);
        dao.createTasks(List.of(t1, t2));
        sync("created");

        assertTrue(dao.removeTask("t-x"));
        sync("removeTask");
        assertEquals(1, count(b, "task"));

        assertTrue(dao.removeWorkflow("wf-3"));
        sync("removeWorkflow");
        for (String[] t : TABLES) {
            assertEquals("table " + t[0] + " is empty after removeWorkflow", 0, count(b, t[0]));
        }
    }

    @Test
    public void removeFromPendingWorkflow() throws Exception {
        dao.createWorkflow(workflow("wf-4", "pending_flow"));
        sync("created");
        assertEquals(1, count(b, "workflow_pending"));
        dao.removeFromPendingWorkflow("pending_flow", "wf-4");
        sync("removeFromPendingWorkflow");
        assertEquals(0, count(b, "workflow_pending"));
        assertEquals(1, count(b, "workflow"));
    }

    @Test
    public void eventExecutions() throws Exception {
        EventExecution e = new EventExecution("exec-1", "msg-1");
        e.setName("handler_a");
        e.setEvent("sqs:queue");
        e.setCreated(1_760_000_000_000L);
        e.setStatus(EventExecution.Status.IN_PROGRESS);
        assertTrue(dao.addEventExecution(e));
        sync("addEventExecution");

        assertFalse("a duplicate is not added, so nothing is mirrored", dao.addEventExecution(e));
        assertEquals(0, captured.size());

        e.setStatus(EventExecution.Status.COMPLETED);
        dao.updateEventExecution(e);
        sync("updateEventExecution");

        dao.removeEventExecution(e);
        sync("removeEventExecution");
        assertEquals(0, count(b, "event_execution"));
    }

    @Test
    public void aMessageAppliedTwiceChangesNothingAndAStaleOneIsSkipped() throws Exception {
        WorkflowModel w = workflow("wf-5", "order_flow");
        dao.createWorkflow(w);
        TaskModel t = task("t-s", "wf-5", "step", 0, 2);
        dao.createTasks(List.of(t));

        t.setStatus(TaskModel.Status.IN_PROGRESS);
        t.setUpdateTime(1_760_000_003_000L);
        dao.updateTask(t);
        PgMirrorMessage older = captured.get(captured.size() - 1);

        t.setStatus(TaskModel.Status.COMPLETED);
        t.setUpdateTime(1_760_000_004_000L);
        dao.updateTask(t);
        PgMirrorMessage newer = captured.get(captured.size() - 1);

        // deliver out of order: the newer update first, then the older one, then the newer one
        // again
        List<PgMirrorMessage> inOrder = new ArrayList<>(captured);
        captured.clear();
        for (PgMirrorMessage m :
                inOrder.subList(0, inOrder.size() - 2)) { // create workflow + createTasks
            applier.apply(m);
        }
        assertTrue("the newer update is applied", applier.apply(newer));
        assertFalse("the older update arrives late and is skipped", applier.apply(older));
        assertTrue("the newer one again is harmless", applier.apply(newer));
        assertSame("out of order and duplicated delivery");
        assertEquals(
                "the completed task is not back in task_in_progress",
                0,
                count(b, "task_in_progress"));
    }

    @Test
    public void everyStatementUsesItsPlaceholdersOnceInOrder() throws Exception {
        WorkflowModel w = workflow("wf-6", "order_flow");
        dao.createWorkflow(w);
        dao.createTasks(List.of(task("t-p1", "wf-6", "a", 0, 0), task("t-p2", "wf-6", "b", 0, 2)));
        TaskModel t = task("t-p3", "wf-6", "c", 0, 0);
        t.setStatus(TaskModel.Status.COMPLETED);
        dao.updateTask(t);
        dao.removeTask("t-p1");
        dao.removeWorkflow("wf-6");
        EventExecution e = new EventExecution("exec-p", "msg-p");
        e.setName("h");
        e.setEvent("ev");
        dao.addEventExecution(e);
        dao.updateEventExecution(e);
        dao.removeEventExecution(e);
        assertFalse(captured.isEmpty());
        for (PgMirrorMessage m : captured) {
            for (PgStatement s : m.statements()) {
                java.util.regex.Matcher mt =
                        java.util.regex.Pattern.compile("\\$(\\d+)").matcher(s.sql());
                int expected = 1;
                while (mt.find()) {
                    assertEquals(s.sql(), expected++, Integer.parseInt(mt.group(1)));
                }
                assertEquals(
                        s.sql() + " has one argument per placeholder",
                        expected - 1,
                        s.args().size());
            }
        }
    }
}
