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
package com.netflix.conductor.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Applies conductor.metrics.event-metrics.enabled (default true) to {@link Monitors}.
 *
 * <p>Event metrics are the counters, timers and distribution summaries recorded as work happens:
 * task polls, executions, queue waits, timeouts, errors, Redis call timings. They are recorded on
 * the pod that does the work, so they belong on the pods that serve traffic. A pod that only
 * publishes WorkflowMonitor gauges can turn them off. Gauges are not affected.
 */
@Component
public class EventMetricsConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventMetricsConfiguration.class);

    public EventMetricsConfiguration(
            @Value("${conductor.metrics.event-metrics.enabled:true}") boolean enabled) {
        Monitors.setEventMetricsEnabled(enabled);
        LOGGER.info(
                "Event metrics (counters, timers, distribution summaries) enabled: {}", enabled);
    }
}
