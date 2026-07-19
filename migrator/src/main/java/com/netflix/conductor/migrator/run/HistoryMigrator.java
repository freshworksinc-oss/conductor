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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
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

/**
 * One-shot back-fill of TERMINAL workflow executions into the destination. Imports each tree
 * dormant via {@link DestWriter} (execution + Postgres index rows) with NO bootstrap — terminal
 * workflows have nothing to re-queue.
 *
 * <p>Two enumeration modes:
 *
 * <ul>
 *   <li><b>single window</b> (default) — one scoped query ({@code migrator.search}); good when the
 *       result set is under OpenSearch's 10k {@code max_result_window}.
 *   <li><b>auto-chunk</b> ({@code migrator.history.auto-chunk=true}) — walk the {@code
 *       search.start-time-from-ms}..{@code -to-ms} range (default {@code 0}..now) in {@code
 *       window-days} slices, adaptively halving any slice whose enumeration would exceed 10k, so an
 *       arbitrarily large history migrates in a single run.
 * </ul>
 *
 * <p>Caveats: archived terminals whose rows were purged 404 and are skipped (their data is gone —
 * only an OpenSearch summary may remain). Writes Postgres index tables only; an OpenSearch-backed
 * dest still needs an OpenSearch&rarr;OpenSearch reindex for search visibility. For very large
 * histories run this as its own job ({@code mode=history} alone) so it doesn't delay a sync pod's
 * readiness.
 */
@Component
public class HistoryMigrator {

    private static final Logger log = LoggerFactory.getLogger(HistoryMigrator.class);

    /** OpenSearch default max_result_window — deep pagination can't reach past this. */
    private static final int MAX_RESULT_WINDOW = 10_000;

    /** Smallest slice auto-chunk will subdivide to before giving up (1 hour). */
    private static final long MIN_WINDOW_MS = 3_600_000L;

    private static final long DAY_MS = 86_400_000L;

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
        MigratorProperties.History h = props.getHistory();
        List<String> statuses = h.getStatuses();
        boolean statusFilter = props.getSearch().isStatusFilter();
        if (statusFilter && (statuses == null || statuses.isEmpty())) {
            log.warn("History mode: status-filter on but no migrator.history.statuses; nothing.");
            return;
        }
        String base = statusFilter ? "status IN (" + String.join(",", statuses) + ")" : "";

        if (h.isAutoChunk()) {
            autoChunk(base);
        } else {
            singleWindow(base);
        }
    }

    // ---- single-window (uses migrator.search scoping as-is) ----

    private void singleWindow(String base) {
        int pageSize = props.getBatchSize();
        String query = source.scopedQuery(base);
        log.info("History back-fill starting: query [{}], pageSize {}", query, pageSize);
        Enumerated e = enumerateAll(query, pageSize);
        if (e.capped) {
            log.warn(
                    "History enumeration hit OpenSearch max_result_window ({}) — enable"
                        + " migrator.history.auto-chunk to cover the full range.",
                    MAX_RESULT_WINDOW);
        }
        log.info("History enumeration complete: {} matching workflow(s)", e.ids.size());
        Counters c = importIds(e.ids, new HashSet<>());
        log.info(
                "History back-fill complete: {} imported, {} failed, {} skipped — from {}"
                        + " enumerated.",
                c.imported,
                c.failed,
                c.skipped,
                e.ids.size());
    }

    // ---- auto-chunk (walk a time range in adaptive slices) ----

    private void autoChunk(String base) {
        int pageSize = props.getBatchSize();
        long floor = Math.max(0, props.getSearch().getStartTimeFromMs());
        long ceiling =
                props.getSearch().getStartTimeToMs() > 0
                        ? props.getSearch().getStartTimeToMs()
                        : System.currentTimeMillis();
        long window = (long) props.getHistory().getWindowDays() * DAY_MS;
        log.info(
                "History auto-chunk: range ({}, {}] in {}-day slices (subdividing any slice >{}).",
                floor,
                ceiling,
                props.getHistory().getWindowDays(),
                MAX_RESULT_WINDOW);

        // Seed slices newest→oldest; each covers (lo, hi].
        Deque<long[]> stack = new ArrayDeque<>();
        long hi = ceiling;
        while (hi > floor) {
            long lo = Math.max(floor, hi - window);
            stack.push(new long[] {lo, hi});
            hi = lo;
        }

        Set<String> seenRoots = new HashSet<>();
        Counters total = new Counters();
        int slices = 0;
        int cappedSlices = 0;
        while (!stack.isEmpty()) {
            long[] w = stack.pop();
            long lo = w[0];
            long hiEnd = w[1];
            // startTime > (lo-1) makes lo inclusive; < hiEnd leaves hiEnd for the newer slice.
            String q = (base.isEmpty() ? "" : base + " AND ")
                    + "startTime > " + (lo - 1) + " AND startTime < " + hiEnd;
            Enumerated e = enumerateAll(q, pageSize);
            if (e.capped) {
                long span = hiEnd - lo;
                if (span > MIN_WINDOW_MS) {
                    long mid = lo + span / 2;
                    stack.push(new long[] {lo, mid});
                    stack.push(new long[] {mid, hiEnd});
                    continue;
                }
                log.warn(
                        "Slice ({}, {}] still exceeds {} at min size — importing first {} only.",
                        lo,
                        hiEnd,
                        MAX_RESULT_WINDOW,
                        e.ids.size());
                cappedSlices++;
            }
            Counters c = importIds(e.ids, seenRoots);
            total.add(c);
            slices++;
            log.info(
                    "Slice ({}, {}]: {} ids → totals imported={} failed={} skipped={}",
                    lo,
                    hiEnd,
                    e.ids.size(),
                    total.imported,
                    total.failed,
                    total.skipped);
        }
        log.info(
                "History auto-chunk complete: {} slice(s), {} imported, {} failed, {} skipped,"
                        + " {} capped slice(s).",
                slices,
                total.imported,
                total.failed,
                total.skipped,
                cappedSlices);
    }

    // ---- helpers ----

    /** Fully paginate a query; flags whether it stopped at the max_result_window. */
    private Enumerated enumerateAll(String query, int pageSize) {
        List<String> ids = new ArrayList<>();
        int start = 0;
        boolean capped = false;
        while (true) {
            List<String> page = source.searchIds(query, start, pageSize);
            ids.addAll(page);
            if (page.size() < pageSize) {
                break;
            }
            start += pageSize;
            if (start + pageSize > MAX_RESULT_WINDOW) {
                capped = true;
                break;
            }
        }
        return new Enumerated(ids, capped);
    }

    /** Resolve ids to unique roots (skip archived 404s) and import each once (dormant). */
    private Counters importIds(List<String> ids, Set<String> seenRoots) {
        Counters c = new Counters();
        Set<String> roots = new LinkedHashSet<>();
        for (String id : ids) {
            try {
                roots.add(treeResolver.findRootId(id));
            } catch (RuntimeException e) {
                c.skipped++;
                log.warn("History: skipping unresolvable id {}: {}", id, e.getMessage());
            }
        }
        for (String root : roots) {
            if (!seenRoots.add(root)) {
                continue; // already imported (root seen in an earlier slice)
            }
            try {
                destWriter.importTree(treeResolver.resolve(root));
                c.imported++;
            } catch (RuntimeException e) {
                c.failed++;
                log.warn("History: failed to import tree {}: {}", root, e.getMessage());
            }
        }
        return c;
    }

    private static final class Enumerated {
        final List<String> ids;
        final boolean capped;

        Enumerated(List<String> ids, boolean capped) {
            this.ids = ids;
            this.capped = capped;
        }
    }

    private static final class Counters {
        int imported;
        int failed;
        int skipped;

        void add(Counters o) {
            imported += o.imported;
            failed += o.failed;
            skipped += o.skipped;
        }
    }
}
