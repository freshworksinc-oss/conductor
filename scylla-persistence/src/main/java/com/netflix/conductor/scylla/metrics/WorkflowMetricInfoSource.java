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
package com.netflix.conductor.scylla.metrics;

import java.util.List;

/**
 * Implemented by the Scylla metadata DAOs: name and ownerApp of the latest version of each
 * workflow, without building or keeping the definitions.
 */
public interface WorkflowMetricInfoSource {

    /**
     * @return one entry per workflow name
     */
    List<WorkflowMetricInfo> getWorkflowMetricInfo();
}
