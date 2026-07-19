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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.migrator.config.MigratorProperties;
import com.netflix.conductor.migrator.dest.DestWriter;
import com.netflix.conductor.migrator.rest.SourceClient;
import com.netflix.conductor.migrator.tree.TreeResolver;
import com.netflix.conductor.migrator.tree.WorkflowTree;

/**
 * One-shot back-fill of TERMINAL workflow executions (COMPLETED/FAILED/TERMINATED/TIMED_OUT, or
 * whatever {@code migrator.history.statuses} lists) into the destination.
 *
 * <p>Unlike {@code --sync} — which tracks only the non-terminal, moving set and keeps copies
 * dormant for a later flip — history is a <b>single pass</b>: terminal workflows never change, so
 * there is nothing to keep in sync. It imports each tree through the same {@link DestWriter} path
 * (execution rows + Postgres index rows) and does <b>not</b> bootstrap (no {@code verifyAndRepair})
 * — a completed workflow has nothing to re-queue.
 *
 * <p>Two caveats to be explicit about:
 *
 * <ul>
 *   <li><b>Archived terminals are skipped.</b> If the source purged a terminal workflow's rows
 *       after archival, {@code GET /workflow/{id}} 404s — its data is gone (only an OpenSearch
 *       summary may remain). Those ids are logged and skipped; copy them with an
 *       OpenSearch&rarr;OpenSearch reindex instead.
 *   <li><b>Postgres index only, not OpenSearch.</b> {@link DestWriter} writes the Postgres {@code
 *       *_index} tables, not OpenSearch — so on an OpenSearch-backed destination these terminals
 *       still need a reindex for UI/search visibility.
 * </ul>
 *
 * <p>For very large histories, prefer running this as its own one-shot job ({@code mode=history}
 * alone, no {@code sync}) so it can take as long as it needs without blocking a sync pod's
 * readiness.
 */
@Component
public class HistoryMigrator {

    private static final Logger log = LoggerFactory.getLogger(HistoryMigrator.class);

    private final SourceClient source;
    private final TreeResolver treeResolver;
    private final DestWriter destWriter;
    private final MigratorProperties props;

    public HistoryMigrator(
            SourceClient source,
            TreeResolver treeResolver,
            DestWriter destWriter,
            MigratorProperties props) {
        this.source = source;
        this.treeResolver = treeResolver;
        this.destWriter = destWriter;
        this.props = props;
    }

    public void migrate() {
        List<String> statuses = props.getHistory().getStatuses();
        if (statuses == null || statuses.isEmpty()) {
            log.warn("History mode: no migrator.history.statuses configured; nothing to do.");
            return;
        }
        String query = "status IN (" + String.join(",", statuses) + ")";
        int pageSize = props.getBatchSize();
        log.info("History back-fill starting: query [{}], pageSize {}", query, pageSize);

        // 1. Enumerate all matching ids (paginated — the source can return large histories).
        List<String> ids = new ArrayList<>();
        int start = 0;
        while (true) {
            List<String> page = source.searchIds(query, start, pageSize);
            ids.addAll(page);
            if (page.size() < pageSize) {
                break;
            }
            start += pageSize;
            if (ids.size() % (pageSize * 20) == 0) {
                log.info("History enumeration: {} ids so far...", ids.size());
            }
        }
        log.info("History enumeration complete: {} matching workflow(s)", ids.size());

        // 2. Collapse to unique tree roots; skip ids that no longer resolve (archived → 404).
        Set<String> roots = new LinkedHashSet<>();
        int skipped = 0;
        for (String id : ids) {
            try {
                roots.add(treeResolver.findRootId(id));
            } catch (RuntimeException e) {
                skipped++;
                log.warn("History: skipping unresolvable id {}: {}", id, e.getMessage());
            }
        }

        // 3. Import each tree (dormant, NO bootstrap — terminal workflows have nothing to re-queue).
        int imported = 0;
        int failed = 0;
        for (String rootId : roots) {
            try {
                WorkflowTree tree = treeResolver.resolve(rootId);
                destWriter.importTree(tree);
                imported++;
            } catch (RuntimeException e) {
                failed++;
                log.warn("History: failed to import tree {}: {}", rootId, e.getMessage());
            }
        }

        log.info(
                "History back-fill complete: {} tree(s) imported, {} failed, {} id(s) skipped"
                        + " (unresolvable) — from {} enumerated.",
                imported,
                failed,
                skipped,
                ids.size());
    }
}
