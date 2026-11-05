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

import java.util.List;

import org.junit.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.netflix.conductor.common.config.ObjectMapperProvider;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.dao.ConcurrentExecutionLimitDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** The Spring wiring, with the real Jedis client talking to a fake Redis. */
public class PgMirrorConfigurationTest {

    /**
     * What the persistence module provides: an ExecutionDAO that also is a
     * ConcurrentExecutionLimitDAO.
     */
    @Configuration
    static class Persistence {
        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapperProvider().getObjectMapper();
        }

        @Bean
        ExecutionDAO executionDAO() {
            ExecutionDAO dao =
                    mock(
                            ExecutionDAO.class,
                            withSettings().extraInterfaces(ConcurrentExecutionLimitDAO.class));
            when(dao.updateWorkflow(org.mockito.ArgumentMatchers.any())).thenReturn("wf-1");
            return dao;
        }
    }

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withUserConfiguration(Persistence.class, PgMirrorConfiguration.class);

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

    @Test
    public void enabledOnScyllaWrapsTheDaoAndPushesToTheUniqueQueue() throws Exception {
        try (FakeRedisServer redis = new FakeRedisServer()) {
            runner.withPropertyValues(
                            "conductor.pg-mirror.enabled=true",
                            "conductor.db.type=scylla",
                            "conductor.app.stack=test-stack",
                            "conductor.pg-mirror.redis.host=127.0.0.1",
                            "conductor.pg-mirror.redis.port=" + redis.port(),
                            "conductor.pg-mirror.redis.username=default",
                            "conductor.pg-mirror.redis.password=secret")
                    .run(
                            ctx -> {
                                ExecutionDAO dao = ctx.getBean(ExecutionDAO.class);
                                assertTrue("the DAO is wrapped", AopUtils.isAopProxy(dao));
                                assertTrue(
                                        "and keeps its other interface",
                                        dao instanceof ConcurrentExecutionLimitDAO);

                                assertEquals(
                                        "the caller gets the real result",
                                        "wf-1",
                                        dao.updateWorkflow(workflow()));

                                // not a Conductor key: it does not start with the queue or workflow
                                // prefixes
                                List<String> list = redis.list("pg-mirror:test-stack:sql");
                                assertEquals(1, list.size());
                                PgMirrorMessage m =
                                        new ObjectMapper()
                                                .readValue(list.get(0), PgMirrorMessage.class);
                                assertEquals("updateWorkflow", m.op());
                                assertTrue(m.gated());
                                assertEquals(3, m.statements().size());
                                assertTrue(
                                        "authenticated as configured",
                                        redis.commands.stream()
                                                .anyMatch(c -> c.get(0).equalsIgnoreCase("AUTH")));
                            });
        }
    }

    @Test
    public void theQueueNameCanBeSet() throws Exception {
        try (FakeRedisServer redis = new FakeRedisServer()) {
            runner.withPropertyValues(
                            "conductor.pg-mirror.enabled=true",
                            "conductor.db.type=scylla",
                            "conductor.pg-mirror.queue-key=my:own:queue",
                            "conductor.pg-mirror.redis.host=127.0.0.1",
                            "conductor.pg-mirror.redis.port=" + redis.port())
                    .run(
                            ctx -> {
                                ctx.getBean(ExecutionDAO.class).updateWorkflow(workflow());
                                assertEquals(1, redis.list("my:own:queue").size());
                            });
        }
    }

    @Test
    public void aDeadRedisNeverBreaksTheWrite() {
        runner.withPropertyValues(
                        "conductor.pg-mirror.enabled=true",
                        "conductor.db.type=scylla",
                        "conductor.pg-mirror.redis.host=127.0.0.1",
                        "conductor.pg-mirror.redis.port=1", // nothing listens there
                        "conductor.pg-mirror.timeout-ms=100")
                .run(
                        ctx -> {
                            ExecutionDAO dao = ctx.getBean(ExecutionDAO.class);
                            long start = System.currentTimeMillis();
                            for (int i = 0; i < 20; i++) {
                                assertEquals("wf-1", dao.updateWorkflow(workflow()));
                            }
                            long took = System.currentTimeMillis() - start;
                            assertTrue(
                                    "the breaker keeps the cost low: 20 writes took "
                                            + took
                                            + " ms",
                                    took < 3_000);
                            assertTrue(ctx.getBean(RedisPgMirrorPublisher.class).failed() > 0);
                        });
    }

    @Test
    public void offByDefault() {
        runner.run(
                ctx -> {
                    assertFalse(AopUtils.isAopProxy(ctx.getBean(ExecutionDAO.class)));
                    assertTrue(ctx.getBeansOfType(RedisPgMirrorPublisher.class).isEmpty());
                });
    }

    @Test
    public void notAppliedOnPostgresPersistence() {
        runner.withPropertyValues("conductor.pg-mirror.enabled=true", "conductor.db.type=postgres")
                .run(ctx -> assertFalse(AopUtils.isAopProxy(ctx.getBean(ExecutionDAO.class))));
    }
}
