/*
 * Copyright 2025 Conductor Authors.
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
package com.netflix.conductor.metrics;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopCounter;
import io.micrometer.core.instrument.noop.NoopDistributionSummary;
import io.micrometer.core.instrument.noop.NoopTimer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.junit.Assert.*;

public class MonitorsTest {

    @After
    public void restoreEventMetrics() {
        Monitors.setEventMetricsEnabled(true);
    }

    @Test
    public void getRegistry_returnsNonNull() {
        assertNotNull(Monitors.getRegistry());
    }

    @Test
    public void addMeterRegistry_metersAreVisibleInAddedRegistry() {
        SimpleMeterRegistry extra = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(extra);

        // Record something after adding the registry
        Monitors.getCounter("test_add_registry_counter", "tag", "value").increment(3);

        Counter counter = extra.find("test_add_registry_counter").counter();
        assertNotNull("Counter should be visible in newly added registry", counter);
        assertEquals(3.0, counter.count(), 0.001);
    }

    @Test
    public void getCounter_sameKeyReturnsSameInstance() {
        Counter c1 = Monitors.getCounter("test_counter_identity", "k", "v");
        Counter c2 = Monitors.getCounter("test_counter_identity", "k", "v");
        assertSame(c1, c2);
    }

    @Test
    public void getTimer_sameKeyReturnsSameInstance() {
        Timer t1 = Monitors.getTimer("test_timer_identity", "k", "v");
        Timer t2 = Monitors.getTimer("test_timer_identity", "k", "v");
        assertSame(t1, t2);
    }

    @Test
    public void getGauge_aliasMatchesGauge() {
        // getGauge and gauge should return the same AtomicDouble for the same key
        assertSame(
                Monitors.gauge("test_gauge_alias", "k", "v"),
                Monitors.getGauge("test_gauge_alias", "k", "v"));
    }

    @Test
    public void getDistributionSummary_aliasMatchesDistributionSummary() {
        assertSame(
                Monitors.distributionSummary("test_dist_alias", "k", "v"),
                Monitors.getDistributionSummary("test_dist_alias", "k", "v"));
    }

    @Test
    public void recordGauge_valueIsReflectedInRegistry() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);

        Monitors.recordGauge("test_gauge_value", 42L);

        // gauge() in Monitors uses an AtomicDouble — the value set should be reflected
        assertNotNull(probe.find("test_gauge_value").gauge());
        assertEquals(42.0, probe.find("test_gauge_value").gauge().value(), 0.001);
    }

    @Test
    public void timerRecordsTime() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);

        Monitors.getTimer("test_timer_record", "op", "read").record(100, TimeUnit.MILLISECONDS);

        Timer timer = probe.find("test_timer_record").timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());
    }

    @Test
    public void metersWithDifferentTagsAreDistinct() {
        MeterRegistry registry = Monitors.getRegistry();

        Monitors.getCounter("test_tag_distinct", "env", "prod").increment(1);
        Monitors.getCounter("test_tag_distinct", "env", "staging").increment(2);

        // Two separate meters, not the same object
        assertNotSame(
                Monitors.getCounter("test_tag_distinct", "env", "prod"),
                Monitors.getCounter("test_tag_distinct", "env", "staging"));
    }

    @Test
    public void registerWebhookQueueDepthGauge_readsLiveQueueSize() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);

        BlockingQueue<String> queue = new LinkedBlockingDeque<>(2);
        Monitors.registerWebhookQueueDepthGauge(queue, "WORKFLOW");

        assertEquals(0.0, probe.find("webhook_queue_depth").gauge().value(), 0.001);

        queue.offer("a");
        queue.offer("b");
        assertEquals(2.0, probe.find("webhook_queue_depth").gauge().value(), 0.001);

        queue.poll();
        assertEquals(1.0, probe.find("webhook_queue_depth").gauge().value(), 0.001);
    }

    @Test
    public void recordWebhookPublishSuccess_incrementsTaggedCounter() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);

        Monitors.recordWebhookPublishSuccess("WORKFLOW", "kitchen_sink", "COMPLETED");

        Counter counter =
                probe.find("webhook_publish_success")
                        .tag("notificationType", "WORKFLOW")
                        .tag("name", "kitchen_sink")
                        .tag("status", "COMPLETED")
                        .counter();
        assertNotNull(counter);
        assertEquals(1.0, counter.count(), 0.001);
    }

    @Test
    public void recordWebhookPublishFailure_incrementsTaggedCounter() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);

        Monitors.recordWebhookPublishFailure("TASK", "my_task", "IOException");

        Counter counter =
                probe.find("webhook_publish_failure")
                        .tag("notificationType", "TASK")
                        .tag("name", "my_task")
                        .tag("errorType", "IOException")
                        .counter();
        assertNotNull(counter);
        assertEquals(1.0, counter.count(), 0.001);
    }

    @Test
    public void recordWebhookEnqueueFailure_incrementsTaggedCounter() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);

        Monitors.recordWebhookEnqueueFailure("WORKFLOW", "kitchen_sink");

        Counter counter =
                probe.find("webhook_enqueue_failure")
                        .tag("notificationType", "WORKFLOW")
                        .tag("name", "kitchen_sink")
                        .counter();
        assertNotNull(counter);
        assertEquals(1.0, counter.count(), 0.001);
    }

    @Test
    public void recordWebhookPublishSuccess_defaultsBlankTagsToUnknown() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);

        Monitors.recordWebhookPublishSuccess("TASK", "", null);

        Counter counter =
                probe.find("webhook_publish_success")
                        .tag("notificationType", "TASK")
                        .tag("name", "unknown")
                        .tag("status", "unknown")
                        .counter();
        assertNotNull(counter);
        assertEquals(1.0, counter.count(), 0.001);
    }

    @Test
    public void eventMetricsDisabled_countersTimersSummariesAreNoopAndNotExported() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);
        Monitors.setEventMetricsEnabled(false);

        Counter counter = Monitors.getCounter("test_event_off_counter", "k", "v");
        Timer timer = Monitors.getTimer("test_event_off_timer", "k", "v");
        DistributionSummary summary =
                Monitors.distributionSummary("test_event_off_summary", "k", "v");
        counter.increment();
        timer.record(5, TimeUnit.MILLISECONDS);
        summary.record(3);

        assertTrue(counter instanceof NoopCounter);
        assertTrue(timer instanceof NoopTimer);
        assertTrue(summary instanceof NoopDistributionSummary);
        assertNull(probe.find("test_event_off_counter").counter());
        assertNull(probe.find("test_event_off_timer").timer());
        assertNull(probe.find("test_event_off_summary").summary());
    }

    @Test
    public void eventMetricsDisabled_timerStillRunsTheWrappedCode() throws Exception {
        Monitors.setEventMetricsEnabled(false);

        // JedisProxy wraps Redis calls in getTimer(...).record(...): the call must still happen.
        assertEquals("result", Monitors.getTimer("test_event_off_wrap").record(() -> "result"));
        assertEquals(
                Integer.valueOf(7),
                Monitors.getTimer("test_event_off_wrap").recordCallable(() -> 7));
        int[] ran = {0};
        Monitors.getTimer("test_event_off_wrap").record(() -> ran[0]++);
        assertEquals(1, ran[0]);
    }

    @Test
    public void eventMetricsDisabled_removesEventMetersAlreadyRegistered() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);
        Monitors.getCounter("test_event_registered_before", "k", "v").increment();
        assertNotNull(probe.find("test_event_registered_before").counter());

        Monitors.setEventMetricsEnabled(false);

        assertNull(probe.find("test_event_registered_before").counter());
    }

    @Test
    public void eventMetricsDisabled_gaugesStillWork() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);
        Monitors.setEventMetricsEnabled(false);

        Monitors.getGauge("test_event_off_gauge", "k", "v").set(42);

        assertNotNull(probe.find("test_event_off_gauge").gauge());
        assertEquals(42.0, probe.find("test_event_off_gauge").gauge().value(), 0.001);
    }

    @Test
    public void eventMetricsReEnabled_realMetersAgain() {
        SimpleMeterRegistry probe = new SimpleMeterRegistry();
        Monitors.addMeterRegistry(probe);
        Monitors.setEventMetricsEnabled(false);
        Monitors.setEventMetricsEnabled(true);

        Monitors.getCounter("test_event_reenabled", "k", "v").increment(2);

        assertFalse(Monitors.getCounter("test_event_reenabled", "k", "v") instanceof NoopCounter);
        assertEquals(2.0, probe.find("test_event_reenabled").counter().count(), 0.001);
    }
}
