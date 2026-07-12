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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.migrator.config.MigratorProperties;
import com.netflix.conductor.migrator.rest.SourceClient;
import com.netflix.conductor.migrator.tree.TreeResolver;

/**
 * The continuous delta-sync loop (--sync mode). Every {@code migrator.sync.interval-seconds}: it
 * re-enumerates the source's non-terminal workflows, collapses them to their owning tree roots,
 * runs a {@link DeltaSyncService} pass, and logs a brief report. When every tracked tree is in sync
 * it logs a prominent DELTA=0 cutover-ready signal. Blocks until the process is stopped.
 */
@Component
public class SyncRunner {

    private static final Logger log = LoggerFactory.getLogger(SyncRunner.class);

    private final SourceClient source;
    private final TreeResolver treeResolver;
    private final DeltaSyncService deltaSyncService;
    private final MigratorProperties props;

    /** In-memory, keyed by root workflow id. Re-derived on restart (import is idempotent). */
    private final Map<String, SyncState> state = new LinkedHashMap<>();

    // Last-pass snapshot for the /health indicator (volatile — read from the actuator thread).
    private volatile long lastPassAtMillis = 0;
    private volatile String lastSummary = "no pass yet";
    private volatile boolean lastZeroDelta = false;

    public SyncRunner(
            SourceClient source,
            TreeResolver treeResolver,
            DeltaSyncService deltaSyncService,
            MigratorProperties props) {
        this.source = source;
        this.treeResolver = treeResolver;
        this.deltaSyncService = deltaSyncService;
        this.props = props;
    }

    public void loop() {
        int intervalSeconds = props.getSync().getIntervalSeconds();
        log.info(
                "Starting delta-sync loop (interval={}s). Copies are kept DORMANT — no bootstrap;"
                        + " the source stays authoritative until the flip.",
                intervalSeconds);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                onePass();
            } catch (RuntimeException e) {
                log.error(
                        "Delta-sync pass failed (will retry next interval): {}", e.getMessage(), e);
            }
            try {
                Thread.sleep(intervalSeconds * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Delta-sync loop interrupted; exiting.");
                return;
            }
        }
    }

    public long getLastPassAtMillis() {
        return lastPassAtMillis;
    }

    public String getLastSummary() {
        return lastSummary;
    }

    public boolean isLastZeroDelta() {
        return lastZeroDelta;
    }

    private void onePass() {
        Set<String> roots = enumerateRoots();
        DeltaReport report = deltaSyncService.runPass(roots, state);

        lastPassAtMillis = System.currentTimeMillis();
        lastSummary = report.summary();
        lastZeroDelta = report.isZeroDelta();

        log.info("Delta-sync pass: {}", report.summary());
        report.getOutOfSync()
                .forEach((rootId, reason) -> log.info("  out-of-sync {} — {}", rootId, reason));
        report.getErrored().forEach((rootId, err) -> log.warn("  error {} — {}", rootId, err));
        report.getDrained()
                .forEach(rootId -> log.info("  drained {} (terminal on source)", rootId));

        if (report.isZeroDelta()) {
            log.info(
                    "DELTA=0 — all {} tracked tree(s) in sync with source; safe to begin"
                            + " cutover/flip.",
                    roots.size());
        }
    }

    /** Enumerate non-terminal source workflows (paginated) and collapse to unique tree roots. */
    private Set<String> enumerateRoots() {
        int size = props.getBatchSize();
        List<String> ids = new ArrayList<>();
        int start = 0;
        while (true) {
            List<String> page = source.searchNonTerminalIds(start, size);
            ids.addAll(page);
            if (page.size() < size) {
                break;
            }
            start += size;
        }

        Set<String> roots = new LinkedHashSet<>();
        for (String id : ids) {
            roots.add(treeResolver.findRootId(id));
        }
        return roots;
    }
}
