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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.migrator.tree.WorkflowTree;

/**
 * Computes a deterministic content signature for a resolved tree, used to detect whether the source
 * changed since the last sync (§4 B1 — "compare updateTime/hash per workflow"). The signature folds
 * in each workflow's and each task's {@code (id, status, updateTime)}, sorted by id so the result
 * is order-independent, then SHA-256 → hex.
 */
public final class TreeSignature {

    private TreeSignature() {}

    public static String of(WorkflowTree tree) {
        List<String> parts = new ArrayList<>();
        for (Workflow workflow : tree.getChildrenFirst()) {
            parts.add(
                    "W:"
                            + workflow.getWorkflowId()
                            + ":"
                            + workflow.getStatus()
                            + ":"
                            + workflow.getUpdateTime());
            for (Task task : workflow.getTasks()) {
                parts.add(
                        "T:"
                                + task.getTaskId()
                                + ":"
                                + task.getStatus()
                                + ":"
                                + task.getUpdateTime());
            }
        }
        Collections.sort(parts);
        return sha256Hex(String.join("|", parts));
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
