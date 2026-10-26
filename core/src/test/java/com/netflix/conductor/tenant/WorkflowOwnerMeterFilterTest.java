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
package com.netflix.conductor.tenant;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class WorkflowOwnerMeterFilterTest {

    private static final long TTL_MILLIS = 1000;

    private final Map<String, WorkflowDef> definitions = new ConcurrentHashMap<>();
    private final AtomicInteger lookups = new AtomicInteger();
    private final AtomicLong now = new AtomicLong(0);
    private MeterRegistry registry;
    private Function<String, Optional<WorkflowDef>> lookup;

    @Before
    public void setUp() {
        lookup =
                name -> {
                    lookups.incrementAndGet();
                    return Optional.ofNullable(definitions.get(name));
                };
        registry = newRegistry(lookup);
    }

    private MeterRegistry newRegistry(Function<String, Optional<WorkflowDef>> fn) {
        MeterRegistry r = new SimpleMeterRegistry();
        r.config().meterFilter(new WorkflowOwnerMeterFilter(fn, now::get, TTL_MILLIS));
        return r;
    }

    private void define(String name, String owner) {
        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setOwnerApp(owner);
        definitions.put(name, def);
    }

    private Counter counter(MeterRegistry r, String name, String workflow, String owner) {
        return Counter.builder(name)
                .tags("workflowName", workflow, "ownerApp", owner, "status", "COMPLETED")
                .register(r);
    }

    private Counter find(MeterRegistry r, String name, String owner) {
        return r.find(name).tag("ownerApp", owner).counter();
    }

    @Test
    public void replacesUnknownOwnerWithDefinitionOwner() {
        define("wf", "tenantA");
        counter(registry, "workflow_execution", "wf", "unknown").increment();

        assertNotNull(find(registry, "workflow_execution", "tenantA"));
        assertNull(find(registry, "workflow_execution", "unknown"));
        // other tags are preserved
        assertNotNull(
                registry.find("workflow_execution")
                        .tag("workflowName", "wf")
                        .tag("status", "COMPLETED")
                        .counter());
    }

    @Test
    public void keepsExplicitOwner() {
        define("wf", "tenantA");
        counter(registry, "workflow_failure", "wf", "someApp");

        assertNotNull(find(registry, "workflow_failure", "someApp"));
        assertEquals(0, lookups.get());
    }

    @Test
    public void ignoresMetersWithoutWorkflowName() {
        Counter.builder("task_timeout").tags("ownerApp", "unknown").register(registry);

        assertNotNull(find(registry, "task_timeout", "unknown"));
        assertEquals(0, lookups.get());
    }

    @Test
    public void keepsUnknownWhenDefinitionMissingOrOwnerBlank() {
        define("noOwner", " ");
        counter(registry, "workflow_start_success", "missing", "unknown");
        counter(registry, "workflow_start_success", "noOwner", "unknown");

        assertEquals(
                2,
                registry.find("workflow_start_success")
                        .tag("ownerApp", "unknown")
                        .counters()
                        .size());
    }

    @Test
    public void keepsUnknownWhenLookupThrows() {
        MeterRegistry r =
                newRegistry(
                        name -> {
                            throw new IllegalStateException("db down");
                        });
        counter(r, "workflow_start_success", "wf", "unknown");

        assertNotNull(find(r, "workflow_start_success", "unknown"));
    }

    @Test
    public void cachesLookupsUntilTtlExpires() {
        define("wf", "tenantA");
        counter(registry, "workflow_start_success", "wf", "unknown");
        counter(registry, "workflow_execution", "wf", "unknown");
        assertEquals(1, lookups.get());

        // definition re-owned; cached value still used inside the TTL
        define("wf", "tenantB");
        counter(registry, "workflow_failure", "wf", "unknown");
        assertEquals(1, lookups.get());
        assertNotNull(find(registry, "workflow_failure", "tenantA"));

        now.set(TTL_MILLIS + 1);
        Counter.builder("workflow_start_error")
                .tags("workflowName", "wf", "ownerApp", "unknown")
                .register(registry);
        assertEquals(2, lookups.get());
        assertNotNull(find(registry, "workflow_start_error", "tenantB"));
    }

    @Test
    public void appliesThroughCompositeRegistry() {
        define("wf", "tenantA");
        io.micrometer.core.instrument.composite.CompositeMeterRegistry composite =
                new io.micrometer.core.instrument.composite.CompositeMeterRegistry();
        MeterRegistry child = new SimpleMeterRegistry();
        composite.add(child);
        composite.config().meterFilter(new WorkflowOwnerMeterFilter(lookup, now::get, TTL_MILLIS));

        counter(composite, "workflow_execution", "wf", "unknown").increment();

        assertNotNull(find(child, "workflow_execution", "tenantA"));
        assertNull(find(child, "workflow_execution", "unknown"));
    }
}
