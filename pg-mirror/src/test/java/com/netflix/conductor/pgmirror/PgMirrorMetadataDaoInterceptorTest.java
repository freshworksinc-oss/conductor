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
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PollDataDAO;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class PgMirrorMetadataDaoInterceptorTest {

    private final List<PgMirrorMessage> published = new ArrayList<>();
    private final PgMirrorSql sql = new PgMirrorSql(new ObjectMapperProvider().getObjectMapper());
    private MetadataDAO delegate;
    private MetadataDAO dao;

    @Before
    public void setUp() {
        delegate = mock(MetadataDAO.class);
        dao = metadata(published::addAll);
    }

    private MetadataDAO metadata(PgMirrorPublisher p) {
        ProxyFactory f = new ProxyFactory(delegate);
        f.addAdvice(new PgMirrorMetadataDaoInterceptor(sql, p));
        return (MetadataDAO) f.getProxy();
    }

    @Test
    public void everyMetadataWriteIsPublishedAfterTheRealCall() {
        TaskDef td = new TaskDef("t");
        WorkflowDef wd = new WorkflowDef();
        wd.setName("w");
        wd.setVersion(2);
        dao.createTaskDef(td);
        dao.updateTaskDef(td);
        dao.removeTaskDef("t");
        dao.createWorkflowDef(wd);
        dao.updateWorkflowDef(wd);
        dao.removeWorkflowDef("w", 2);
        List<String> ops = new ArrayList<>();
        published.forEach(m -> ops.add(m.op()));
        assertEquals(
                List.of(
                        "createTaskDef",
                        "updateTaskDef",
                        "removeTaskDef",
                        "createWorkflowDef",
                        "updateWorkflowDef",
                        "removeWorkflowDef"),
                ops);
        // the workflow def messages end with the latest_version statement, placeholders used once
        PgMirrorMessage m = published.get(3);
        assertEquals(2, m.statements().size());
        assertTrue(m.statements().get(1).sql().contains("$2"));
        assertEquals(List.of("w", "w"), m.statements().get(1).args());
    }

    @Test
    public void readsAreNotMirrored() {
        dao.getTaskDef("t");
        dao.getAllWorkflowDefs();
        assertTrue(published.isEmpty());
    }

    @Test
    public void aFailedWriteIsNotPublishedAndItsExceptionPasses() {
        doThrow(new IllegalStateException("scylla down")).when(delegate).createTaskDef(null);
        try {
            dao.createTaskDef(null);
            fail("the exception must pass through");
        } catch (IllegalStateException expected) {
            assertTrue(published.isEmpty());
        }
    }

    @Test
    public void aBrokenPublisherNeverBreaksTheWrite() {
        MetadataDAO broken =
                metadata(
                        m -> {
                            throw new RuntimeException("redis down");
                        });
        broken.createTaskDef(new TaskDef("t")); // does not throw
    }

    @Test
    public void eventHandlersAreMirrored() {
        EventHandlerDAO handlers = mock(EventHandlerDAO.class);
        ProxyFactory f = new ProxyFactory(handlers);
        f.addAdvice(new PgMirrorMetadataDaoInterceptor(sql, published::addAll));
        EventHandlerDAO p = (EventHandlerDAO) f.getProxy();
        EventHandler h = new EventHandler();
        h.setName("h");
        h.setEvent("e");
        p.addEventHandler(h);
        p.updateEventHandler(h);
        p.removeEventHandler("h");
        assertEquals(3, published.size());
        assertEquals(2, published.get(0).statements().size()); // delete + insert in one transaction
    }

    @Test
    public void pollDataIsCoalescedIntoOneMessagePerFlush() {
        PollDataDAO pd = mock(PollDataDAO.class);
        PgMirrorPollDataDaoInterceptor interceptor =
                new PgMirrorPollDataDaoInterceptor(sql, published::addAll, 0);
        ProxyFactory f = new ProxyFactory(pd);
        f.addAdvice(interceptor);
        PollDataDAO p = (PollDataDAO) f.getProxy();
        for (int i = 0; i < 100; i++) {
            p.updateLastPollData("q", null, "w" + i);
            p.updateLastPollData("q", "d", "w");
        }
        assertTrue("nothing is sent before the flush", published.isEmpty());
        interceptor.flush();
        assertEquals(1, published.size());
        assertEquals(2, published.get(0).statements().size()); // q/DEFAULT and q/d
        assertEquals("DEFAULT", published.get(0).statements().get(0).args().get(1));
        interceptor.flush();
        assertEquals("an empty flush sends nothing", 1, published.size());
    }
}
