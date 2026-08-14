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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.netflix.conductor.migrator.config.MigratorProperties;
import com.netflix.conductor.migrator.config.SchemaInitializer;
import com.netflix.conductor.migrator.rest.SourceClient;
import com.netflix.conductor.migrator.sync.SyncRunner;

/**
 * CLI entry point for an operator-triggered run.
 *
 * <pre>
 *   --metadata          copy definitions (task defs, workflow defs, event handlers) source → dest
 *   --ids=wf1,wf2,wf3   migrate these specific root workflow ids (import + bootstrap, then exit)
 *   --search            enumerate non-terminal (RUNNING/PAUSED) ids from the source (up to batchSize)
 *   --history           one-shot back-fill of TERMINAL executions (migrator.history.statuses),
 *                       paginated, dormant, no bootstrap; then exit (unless combined with --sync)
 *   --repair            one-shot Redis repair: verifyAndRepair every non-terminal dest workflow
 *                       (re-arm on a fresh Redis), loop to convergence, then exit. Repair ONLY —
 *                       no schema/metadata/history/sync.
 *   --sync              continuous delta-sync loop: keep dormant dest copies current (no bootstrap),
 *                       re-enumerating the source each pass; runs until stopped
 * </pre>
 *
 * {@code --metadata} may be combined with the others; definitions are migrated first (a
 * precondition for importing executions). {@code --sync} is a long-running mode (does not exit); it
 * re-enumerates the source and ignores {@code --ids}.
 *
 * <p>When NO operation flag is passed, operations come from config {@code migrator.mode} (e.g.
 * {@code metadata,sync}) — this is how the container/k8s pod runs (plain {@code java -jar}, no
 * flags). CLI flags, when present, take precedence over {@code migrator.mode}.
 */
@Component
public class MigrationCommand implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MigrationCommand.class);

    /** True when a long-running mode (sync) was started — signals main to keep the JVM alive. */
    private volatile boolean longRunning = false;

    private final MigrationRunner runner;
    private final MetadataMigrator metadataMigrator;
    private final HistoryMigrator historyMigrator;
    private final RepairRunner repairRunner;
    private final SyncRunner syncRunner;
    private final SchemaInitializer schemaInitializer;
    private final SourceClient source;
    private final MigratorProperties props;
    // Present only when migrator.poll-data.enabled=true (bean is conditional).
    private final ObjectProvider<PollDataMigrator> pollDataMigrator;

    public MigrationCommand(
            MigrationRunner runner,
            MetadataMigrator metadataMigrator,
            HistoryMigrator historyMigrator,
            RepairRunner repairRunner,
            SyncRunner syncRunner,
            SchemaInitializer schemaInitializer,
            SourceClient source,
            MigratorProperties props,
            ObjectProvider<PollDataMigrator> pollDataMigrator) {
        this.runner = runner;
        this.metadataMigrator = metadataMigrator;
        this.historyMigrator = historyMigrator;
        this.repairRunner = repairRunner;
        this.syncRunner = syncRunner;
        this.schemaInitializer = schemaInitializer;
        this.source = source;
        this.props = props;
        this.pollDataMigrator = pollDataMigrator;
    }

    /**
     * True if a long-running (sync) mode is active; main keeps the JVM alive instead of exiting.
     */
    public boolean isLongRunning() {
        return longRunning;
    }

    @Override
    public void run(ApplicationArguments args) {
        boolean flagMetadata = args.containsOption("metadata");
        boolean flagHistory = args.containsOption("history");
        boolean flagRepair = args.containsOption("repair");
        boolean flagSync = args.containsOption("sync");
        boolean doExecutions = args.containsOption("ids") || args.containsOption("search");

        // If no operation flag is given, fall back to config-driven mode (migrator.mode) — this is
        // how the k8s pod runs: plain `java -jar`, operations chosen entirely by config.
        boolean anyFlag = flagMetadata || flagHistory || flagRepair || flagSync || doExecutions;
        List<String> mode = props.getMode();
        boolean doMetadata = flagMetadata || (!anyFlag && mode.contains("metadata"));
        boolean doHistory = flagHistory || (!anyFlag && mode.contains("history"));
        boolean doRepair = flagRepair || (!anyFlag && mode.contains("repair"));
        boolean doSync = flagSync || (!anyFlag && mode.contains("sync"));

        if (!doMetadata && !doHistory && !doRepair && !doSync && !doExecutions) {
            log.error(
                    "Nothing to do. Set migrator.mode (e.g. metadata,sync) or pass --metadata,"
                            + " --history, --repair, --sync, --ids=wf1,wf2 / --search. See"
                            + " MigrationCommand docs.");
            return;
        }

        // Redis-repair is a standalone, one-shot operation: re-arm non-terminal workflows on the
        // dest's Redis via verifyAndRepair, then exit. It deliberately does NOT run schema-init,
        // metadata, history, or sync — so handle it first and return.
        if (doRepair) {
            log.info("Running Redis repair only (no schema/metadata/history/sync).");
            repairRunner.run();
            return;
        }

        // Ensure the destination schema exists (no-op unless migrator.dest-datasource.init-schema).
        schemaInitializer.initIfEnabled();

        // Definitions first — they are a precondition for importing executions (§5.4).
        if (doMetadata) {
            log.info("Migrating definitions (source → dest)");
            metadataMigrator.migrate();
        }

        // Poll data (one-shot) — only when migrator.poll-data.enabled=true (bean present).
        pollDataMigrator.ifAvailable(
                pdm -> {
                    log.info("Migrating poll data (source → dest)");
                    pdm.migrate();
                });

        // Terminal-history back-fill (one-shot): import terminal executions before starting the
        // continuous non-terminal sync. Terminal + non-terminal are disjoint sets. NOTE: for very
        // large histories run this as its own job (mode=history alone) so it doesn't delay a sync
        // pod's readiness.
        if (doHistory) {
            log.info("Back-filling terminal history (source → dest)");
            historyMigrator.migrate();
        }

        // Continuous delta-sync: run the loop on its own (non-daemon) thread so this
        // ApplicationRunner returns — letting ApplicationReadyEvent fire (readiness → UP) and the
        // /health probe serve. The non-daemon thread + web server keep the JVM alive; main will NOT
        // System.exit while longRunning is set.
        if (doSync) {
            Thread syncThread = new Thread(syncRunner::loop, "delta-sync");
            syncThread.start();
            longRunning = true;
            return;
        }

        if (!doExecutions) {
            return;
        }

        List<String> rootIds;
        if (args.containsOption("ids")) {
            rootIds =
                    args.getOptionValues("ids").stream()
                            .flatMap(v -> Arrays.stream(v.split(",")))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());
        } else {
            rootIds = source.searchNonTerminalIds(0, props.getBatchSize());
            log.info("Enumerated {} non-terminal workflow(s) from source", rootIds.size());
        }

        if (rootIds.isEmpty()) {
            log.warn("No workflow ids to migrate.");
            return;
        }

        log.info("Starting migration of {} root tree(s)", rootIds.size());
        MigrationReport report = runner.migrate(rootIds);
        if (report.hasFailures()) {
            log.error("Migration finished WITH FAILURES: {}", report.summary());
        } else {
            log.info("Migration finished cleanly: {}", report.summary());
        }
    }
}
