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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Tally of a migration run: which root trees succeeded and which failed (with the reason). */
public class MigrationReport {

    private final List<String> succeeded = new ArrayList<>();
    private final Map<String, String> failed = new LinkedHashMap<>();

    public void recordSuccess(String rootWorkflowId) {
        succeeded.add(rootWorkflowId);
    }

    public void recordFailure(String rootWorkflowId, String reason) {
        failed.put(rootWorkflowId, reason);
    }

    public List<String> getSucceeded() {
        return succeeded;
    }

    public Map<String, String> getFailed() {
        return failed;
    }

    public boolean hasFailures() {
        return !failed.isEmpty();
    }

    public String summary() {
        return succeeded.size()
                + " succeeded, "
                + failed.size()
                + " failed"
                + (failed.isEmpty() ? "" : " " + failed);
    }
}
