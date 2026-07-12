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
import java.util.List;
import java.util.Map;

/** Outcome of one delta-sync pass — the brief "what's out of sync" report. */
public class DeltaReport {

    private int total;
    private int inSync;
    private final Map<String, String> outOfSync = new LinkedHashMap<>(); // rootId -> reason
    private final List<String> drained = new ArrayList<>();
    private final Map<String, String> errored = new LinkedHashMap<>(); // rootId -> error

    public void recordInSync() {
        inSync++;
        total++;
    }

    public void recordOutOfSync(String rootId, String reason) {
        outOfSync.put(rootId, reason);
        total++;
    }

    public void recordError(String rootId, String error) {
        errored.put(rootId, error);
        total++;
    }

    public void recordDrained(String rootId) {
        drained.add(rootId);
    }

    /** True only when every tracked tree is in sync and nothing errored this pass. */
    public boolean isZeroDelta() {
        return outOfSync.isEmpty() && errored.isEmpty() && total > 0;
    }

    public Map<String, String> getOutOfSync() {
        return outOfSync;
    }

    public Map<String, String> getErrored() {
        return errored;
    }

    public List<String> getDrained() {
        return drained;
    }

    public String summary() {
        return String.format(
                "tracked=%d in-sync=%d out-of-sync=%d drained=%d errored=%d",
                total, inSync, outOfSync.size(), drained.size(), errored.size());
    }
}
