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
package com.netflix.conductor.migrator.run;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.migrator.config.MigratorProperties;
import com.netflix.conductor.migrator.rest.DestClient;

/**
 * Redis-repair mode ({@code migrator.mode=repair}). Re-arms the destination's non-terminal
 * workflows on a fresh/empty Redis by calling {@code verifyAndRepair} per workflow, so the engine's
 * sweeper picks them up again. Used after a Redis switch or a new-Redis cutover.
 *
 * <p><b>Repair only</b> — it does not sync, copy definitions, or import executions. It:
 *
 * <ol>
 *   <li>enumerates the non-terminal set directly from the dest Postgres {@code workflow_pending}
 *       table (read-only),
 *   <li>calls {@code verifyAndRepair} on each against {@code migrator.dest.url} (the tenant
 *       conductor now on the new Redis), and
 *   <li>loops until a pass repairs nothing — i.e. every non-terminal workflow is already queued
 *       (converged) — or {@code migrator.repair.max-passes} is reached.
 * </ol>
 *
 * <p>Convergence is "no repairs in a pass", NOT "no pending workflows" — long-running/waiting
 * workflows (WAIT, human tasks, event waits) legitimately stay non-terminal.
 *
 * <p><b>Caveat:</b> {@code verifyAndRepair} re-pushes decider/task-queue entries but does NOT
 * restore {@code WAIT(duration)} timers — time-based WAIT tasks may still need manual handling.
 */
@Component
public class RepairRunner {

    private static final Logger log = LoggerFactory.getLogger(RepairRunner.class);

    private final DataSource destDataSource;
    private final DestClient destClient;
    private final MigratorProperties props;

    public RepairRunner(
            DataSource destDataSource, DestClient destClient, MigratorProperties props) {
        this.destDataSource = destDataSource;
        this.destClient = destClient;
        this.props = props;
    }

    public void run() {
        int maxPasses = props.getRepair().getMaxPasses();
        long throttleMs = props.getRepair().getThrottleMs();
        log.info(
                "Redis repair starting (dest={}, maxPasses={}, throttleMs={}). Repair only — no"
                        + " sync/metadata/history.",
                props.getDest().getUrl(),
                maxPasses,
                throttleMs);

        for (int pass = 1; pass <= maxPasses; pass++) {
            List<String> ids = pendingWorkflowIds();
            int repaired = 0;
            int errored = 0;
            for (String id : ids) {
                try {
                    String result = destClient.verifyAndRepair(id);
                    // verifyAndRepair returns "true" when it re-queued (repaired); "false" when the
                    // workflow was already consistent.
                    if (result != null && "true".equalsIgnoreCase(result.trim())) {
                        repaired++;
                    }
                } catch (RuntimeException e) {
                    errored++;
                    log.warn("verifyAndRepair failed for {}: {}", id, e.getMessage());
                }
                if (throttleMs > 0) {
                    try {
                        Thread.sleep(throttleMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.warn("Repair interrupted; stopping.");
                        return;
                    }
                }
            }
            log.info(
                    "Repair pass {}/{}: pending={} repaired={} errored={}",
                    pass,
                    maxPasses,
                    ids.size(),
                    repaired,
                    errored);
            if (repaired == 0) {
                log.info(
                        "Redis repair converged after {} pass(es) — all non-terminal workflows are"
                            + " re-armed. NOTE: WAIT(duration) timers are not restored by"
                            + " verifyAndRepair; handle time-based WAIT tasks separately.",
                        pass);
                return;
            }
        }
        log.warn(
                "Redis repair hit max passes ({}) without converging — re-run if pending workflows"
                        + " still aren't advancing.",
                maxPasses);
    }

    /** Non-terminal workflow ids on the destination (Conductor's pending index). Read-only. */
    private List<String> pendingWorkflowIds() {
        List<String> ids = new ArrayList<>();
        String sql = "SELECT workflow_id FROM workflow_pending";
        try (Connection tx = destDataSource.getConnection();
                Statement st = tx.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read workflow_pending for repair", e);
        }
        return ids;
    }
}
