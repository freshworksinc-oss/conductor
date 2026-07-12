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
package com.netflix.conductor.migrator.sync;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Exposes delta-sync status on {@code /actuator/health} for k8s probes and operator visibility:
 * last pass timestamp, the latest one-line summary, and whether the last pass was at DELTA=0.
 * Always UP while the process is alive (liveness = process up); the details convey sync progress.
 */
@Component
public class SyncHealthIndicator implements HealthIndicator {

    private final SyncRunner syncRunner;

    public SyncHealthIndicator(SyncRunner syncRunner) {
        this.syncRunner = syncRunner;
    }

    @Override
    public Health health() {
        return Health.up()
                .withDetail("lastPassAtMillis", syncRunner.getLastPassAtMillis())
                .withDetail("lastSummary", syncRunner.getLastSummary())
                .withDetail("zeroDelta", syncRunner.isLastZeroDelta())
                .build();
    }
}
