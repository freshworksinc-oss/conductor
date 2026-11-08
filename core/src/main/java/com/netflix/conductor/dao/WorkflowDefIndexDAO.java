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
package com.netflix.conductor.dao;

import java.util.Map;

import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;

/**
 * Optional interface for {@link MetadataDAO} implementations that keep an index of workflow
 * definition names and versions, so that listing them reads no definition body. Without it the
 * service falls back to reading every definition, which does not scale to a large number of
 * versions.
 */
public interface WorkflowDefIndexDAO {

    /**
     * Every workflow name with the summaries (name and version, the times only when they are known
     * without reading the definition) of its versions, from the index only.
     */
    Map<String, ? extends Iterable<WorkflowDefSummary>> getWorkflowNamesAndVersions();
}
