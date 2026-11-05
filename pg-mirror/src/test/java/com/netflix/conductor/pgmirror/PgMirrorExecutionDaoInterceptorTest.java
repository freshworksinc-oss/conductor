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

import org.junit.Before;
import org.junit.Test;
import org.springframework.aop.framework.ProxyFactory;

import com.netflix.conductor.common.config.ObjectMapperProvider;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.dao.ConcurrentExecutionLimitDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class PgMirrorExecutionDaoInterceptorTest {

    private ExecutionDAO delegate;
    private final List<PgMirrorMessage> published = new ArrayList<>();
    private ExecutionDAO dao; // what the Conductor sees
    private PgMirrorPublisher publisher = published::addAll;

    @Before
    public void setUp() {
        delegate =
                mock(
                        ExecutionDAO.class,
                        withSettings().extraInterfaces(ConcurrentExecutionLimitDAO.class));
        dao = proxy(publisher);
    }

    private ExecutionDAO proxy(PgMirrorPublisher p) {
        ProxyFactory f = new ProxyFactory(delegate);
        f.addAdvice(
                new PgMirrorExecutionDaoInterceptor(
                        delegate,
                        new PgMirrorSql(new ObjectMapperProvider().getObjectMapper()),
                        p));
        return (ExecutionDAO) f.getProxy();
    }

    private static WorkflowModel workflow() {
        WorkflowDef def = new WorkflowDef();
        def.setName("flow");
        def.setVersion(1);
        WorkflowModel w = new WorkflowModel();
        w.setWorkflowDefinition(def);
        w.setWorkflowId("wf-1");
        w.setStatus(WorkflowModel.Status.RUNNING);
        w.setCreateTime(1L);
        return w;
    }

    private static TaskModel task(String id) {
        TaskModel t = new TaskModel();
        t.setTaskId(id);
        t.setWorkflowInstanceId("wf-1");
        t.setReferenceTaskName("ref");
        t.setTaskDefName("def");
        t.setStatus(TaskModel.Status.SCHEDULED);
        return t;
    }

    @Test
    public void everyInterfaceOfTheDaoSurvivesTheWrapper() {
        assertTrue(dao instanceof ConcurrentExecutionLimitDAO);
    }

    @Test
    public void aSuccessfulWritePublishesAfterTheRealCall() {
        WorkflowModel w = workflow();
        when(delegate.updateWorkflow(w)).thenReturn("wf-1");
        assertEquals("the caller gets the DAO's own result", "wf-1", dao.updateWorkflow(w));
        assertEquals(1, published.size());
        assertEquals("updateWorkflow", published.get(0).op());
        assertEquals("wf-1", published.get(0).workflowId());
    }

    @Test
    public void aFailingRealCallPassesThroughAndPublishesNothing() {
        WorkflowModel w = workflow();
        RuntimeException boom = new RuntimeException("scylla down");
        when(delegate.updateWorkflow(w)).thenThrow(boom);
        try {
            dao.updateWorkflow(w);
            fail("the DAO's exception must reach the caller");
        } catch (RuntimeException e) {
            assertSame(boom, e);
        }
        assertTrue(published.isEmpty());
    }

    @Test
    public void aPublisherThatThrowsDoesNotTouchTheCaller() {
        ExecutionDAO broken =
                proxy(
                        m -> {
                            throw new IllegalStateException("redis exploded");
                        });
        WorkflowModel w = workflow();
        when(delegate.updateWorkflow(w)).thenReturn("wf-1");
        assertEquals("wf-1", broken.updateWorkflow(w));
    }

    @Test
    public void aBuilderErrorDoesNotTouchTheCaller() {
        WorkflowModel w = workflow();
        w.setStatus(
                null); // the statement builder cannot handle this: it must stay inside the mirror
        when(delegate.updateWorkflow(w)).thenReturn("wf-1");
        assertEquals("wf-1", dao.updateWorkflow(w));
        assertTrue(published.isEmpty());
    }

    @Test
    public void createTasksMirrorsEveryTaskScyllaReturned() {
        List<TaskModel> tasks = List.of(task("t-1"), task("t-2"), task("t-3"));
        when(delegate.createTasks(tasks)).thenReturn(tasks);
        assertSame(tasks, dao.createTasks(tasks));
        assertEquals(3, published.size());
        assertEquals("createTasks", published.get(2).op());
    }

    @Test
    public void removeTaskReadsTheTaskBeforeAndMirrorsTheDeletes() {
        TaskModel t = task("t-1");
        when(delegate.getTask("t-1")).thenReturn(t);
        when(delegate.removeTask("t-1")).thenReturn(true);
        assertTrue(dao.removeTask("t-1"));
        assertEquals(1, published.size());
        assertEquals("removeTask", published.get(0).op());
        assertEquals(4, published.get(0).statements().size());
    }

    @Test
    public void removingATaskThatDoesNotExistMirrorsNothing() {
        when(delegate.getTask("nope")).thenReturn(null);
        when(delegate.removeTask("nope")).thenReturn(false);
        dao.removeTask("nope");
        assertTrue(published.isEmpty());
    }

    @Test
    public void removeWorkflowMirrorsTheWorkflowAndAllItsTasks() {
        WorkflowModel w = workflow();
        w.setTasks(List.of(task("t-1"), task("t-2")));
        when(delegate.getWorkflow("wf-1", true)).thenReturn(w);
        when(delegate.removeWorkflow("wf-1")).thenReturn(true);
        dao.removeWorkflow("wf-1");
        assertEquals(1, published.size());
        assertEquals(3 + 2 * 4, published.get(0).statements().size());
    }

    @Test
    public void aDuplicateEventExecutionIsNotMirrored() {
        EventExecution e = new EventExecution("exec-1", "msg-1");
        e.setName("h");
        e.setEvent("ev");
        when(delegate.addEventExecution(e)).thenReturn(false);
        dao.addEventExecution(e);
        assertTrue(published.isEmpty());
        when(delegate.addEventExecution(e)).thenReturn(true);
        dao.addEventExecution(e);
        assertEquals(1, published.size());
    }

    @Test
    public void readsAreNotMirroredAndNotTouched() {
        WorkflowModel w = workflow();
        when(delegate.getWorkflow("wf-1", false)).thenReturn(w);
        assertSame(w, dao.getWorkflow("wf-1", false));
        assertTrue(published.isEmpty());
        verify(delegate, never()).createWorkflow(any());
    }
}
