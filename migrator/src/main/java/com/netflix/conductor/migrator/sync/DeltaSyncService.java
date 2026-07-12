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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.migrator.dest.DestWriter;
import com.netflix.conductor.migrator.tree.TreeResolver;
import com.netflix.conductor.migrator.tree.WorkflowTree;

/**
 * Runs one delta-sync pass over the tracked root set. Per root: resolve the source tree, compute
 * its signature, and if it is new or changed, re-import it into the destination via {@link
 * DestWriter} — which keeps the copy DORMANT (no {@code verifyAndRepair}; the source stays
 * authoritative). Roots that disappeared from the tracked set (terminated on source) are reported
 * as drained.
 */
@Component
public class DeltaSyncService {

    private static final Logger log = LoggerFactory.getLogger(DeltaSyncService.class);

    private final TreeResolver treeResolver;
    private final DestWriter destWriter;

    public DeltaSyncService(TreeResolver treeResolver, DestWriter destWriter) {
        this.treeResolver = treeResolver;
        this.destWriter = destWriter;
    }

    public DeltaReport runPass(Set<String> currentRoots, Map<String, SyncState> state) {
        DeltaReport report = new DeltaReport();

        // Drain: roots tracked previously but no longer enumerated (terminated on source).
        List<String> gone = new ArrayList<>();
        for (String tracked : state.keySet()) {
            if (!currentRoots.contains(tracked)) {
                gone.add(tracked);
            }
        }
        for (String rootId : gone) {
            state.remove(rootId);
            report.recordDrained(rootId);
        }

        for (String rootId : currentRoots) {
            SyncState treeState = state.computeIfAbsent(rootId, SyncState::new);
            try {
                WorkflowTree tree = treeResolver.resolve(rootId);
                String signature = TreeSignature.of(tree);

                if (treeState.getLastSignature() == null) {
                    destWriter.importTree(tree); // initial dormant copy
                    mark(treeState, signature, "initial copy");
                    report.recordOutOfSync(rootId, "initial copy");
                } else if (!signature.equals(treeState.getLastSignature())) {
                    destWriter.importTree(tree); // converge changed state
                    mark(treeState, signature, "changed");
                    report.recordOutOfSync(rootId, "changed");
                } else {
                    report.recordInSync();
                }
            } catch (RuntimeException e) {
                treeState.setState(SyncState.State.ERROR);
                treeState.setLastReason(e.getMessage());
                report.recordError(rootId, e.getMessage());
                log.warn("Delta-sync failed for tree {}: {}", rootId, e.getMessage());
            }
        }
        return report;
    }

    private void mark(SyncState treeState, String signature, String reason) {
        treeState.setLastSignature(signature);
        treeState.setLastSyncedAtMillis(System.currentTimeMillis());
        treeState.setState(SyncState.State.SYNCING);
        treeState.setLastReason(reason);
    }
}
