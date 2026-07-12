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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.migrator.dest.DestWriter;
import com.netflix.conductor.migrator.payload.PayloadHandler;
import com.netflix.conductor.migrator.rest.DestClient;
import com.netflix.conductor.migrator.tree.TreeResolver;
import com.netflix.conductor.migrator.tree.WorkflowTree;

/**
 * Orchestrates the core-engine migration for a set of root workflow ids. Per tree: resolve
 * (children-first) → handle payloads → import dormant → bootstrap (verifyAndRepair, children-first)
 * → verify.
 *
 * <p>This phase does NOT pause or terminate the source (that is the deferred flip). Bootstrapping
 * here is a rehearsal of activation against copies that the source keeps running independently.
 */
@Component
public class MigrationRunner {

    private static final Logger log = LoggerFactory.getLogger(MigrationRunner.class);

    private final TreeResolver treeResolver;
    private final PayloadHandler payloadHandler;
    private final DestWriter destWriter;
    private final DestClient destClient;

    public MigrationRunner(
            TreeResolver treeResolver,
            PayloadHandler payloadHandler,
            DestWriter destWriter,
            DestClient destClient) {
        this.treeResolver = treeResolver;
        this.payloadHandler = payloadHandler;
        this.destWriter = destWriter;
        this.destClient = destClient;
    }

    public MigrationReport migrate(List<String> rootWorkflowIds) {
        MigrationReport report = new MigrationReport();
        for (String rootId : rootWorkflowIds) {
            try {
                migrateTree(rootId);
                report.recordSuccess(rootId);
                log.info("Tree {} migrated successfully", rootId);
            } catch (RuntimeException e) {
                report.recordFailure(rootId, e.getMessage());
                log.error("Tree {} migration FAILED: {}", rootId, e.getMessage(), e);
            }
        }
        log.info("Migration run complete: {}", report.summary());
        return report;
    }

    private void migrateTree(String rootId) {
        WorkflowTree tree = treeResolver.resolve(rootId);
        log.info("Resolved tree {} with {} workflow(s)", rootId, tree.size());

        // A4: resolve external payload pointers (no-op unless enabled).
        tree.getChildrenFirst().forEach(payloadHandler::handle);

        // A5: import dormant, children-first.
        destWriter.importTree(tree);

        // C5: bootstrap — the wake-up call, children-first.
        for (Workflow workflow : tree.getChildrenFirst()) {
            String result = destClient.verifyAndRepair(workflow.getWorkflowId());
            log.info("verifyAndRepair({}) -> {}", workflow.getWorkflowId(), result);
        }

        // C7: verify the destination matches the export.
        for (Workflow expected : tree.getChildrenFirst()) {
            verify(expected);
        }
    }

    private void verify(Workflow expected) {
        Workflow actual = destClient.getWorkflow(expected.getWorkflowId());
        if (actual == null) {
            throw new IllegalStateException(
                    "Verification failed: workflow "
                            + expected.getWorkflowId()
                            + " absent on dest");
        }
        if (actual.getStatus() != expected.getStatus()) {
            log.warn(
                    "Verification note: workflow {} status is {} on dest, was {} on source"
                            + " (may have advanced after bootstrap)",
                    expected.getWorkflowId(),
                    actual.getStatus(),
                    expected.getStatus());
        }
        if (actual.getTasks().size() < expected.getTasks().size()) {
            throw new IllegalStateException(
                    "Verification failed: workflow "
                            + expected.getWorkflowId()
                            + " has fewer tasks on dest ("
                            + actual.getTasks().size()
                            + ") than exported ("
                            + expected.getTasks().size()
                            + ")");
        }
    }
}
