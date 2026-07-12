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
package com.netflix.conductor.migrator.payload;

import com.netflix.conductor.common.run.Workflow;

/**
 * Handles external payload pointers (F10) on a workflow export before it is imported. A workflow or
 * task may carry {@code externalInput/OutputPayloadStoragePath} instead of inline data; copying the
 * JSON copies the pointer, not the blob.
 *
 * <p>This is a P0 verification item; only the no-op implementation ships in the core engine.
 */
public interface PayloadHandler {

    /**
     * Resolve/copy/rewrite any external payload pointers on the workflow and its tasks in place.
     */
    void handle(Workflow workflow);
}
