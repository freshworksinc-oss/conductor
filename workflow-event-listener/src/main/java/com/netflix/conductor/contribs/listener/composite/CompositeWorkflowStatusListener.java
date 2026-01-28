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
package com.netflix.conductor.contribs.listener.composite;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.model.WorkflowModel;

/**
 * Composite workflow status listener that delegates to multiple listeners. Failures in one listener
 * do not affect others.
 */
public class CompositeWorkflowStatusListener implements WorkflowStatusListener {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CompositeWorkflowStatusListener.class);
    private final List<WorkflowStatusListener> listeners;

    public CompositeWorkflowStatusListener(List<WorkflowStatusListener> listeners) {
        this.listeners = listeners;
        LOGGER.info(
                "Initialized composite workflow listener with {} listeners: {}",
                listeners.size(),
                listeners.stream().map(d -> d.getClass().getSimpleName()).toList());
    }

    @Override
    public void onWorkflowCompleted(WorkflowModel workflow) {
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowCompleted(workflow),
                                "onWorkflowCompleted",
                                workflow.getWorkflowId()));
    }

    @Override
    public void onWorkflowTerminated(WorkflowModel workflow) {
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowTerminated(workflow),
                                "onWorkflowTerminated",
                                workflow.getWorkflowId()));
    }

    @Override
    public void onWorkflowFinalized(WorkflowModel workflow) {
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowFinalized(workflow),
                                "onWorkflowFinalized",
                                workflow.getWorkflowId()));
    }

    @Override
    public void onWorkflowStarted(WorkflowModel workflow) {
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowStarted(workflow),
                                "onWorkflowStarted",
                                workflow.getWorkflowId()));
    }

    @Override
    public void onWorkflowRestarted(WorkflowModel workflow) {
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowRestarted(workflow),
                                "onWorkflowRestarted",
                                workflow.getWorkflowId()));
    }

    @Override
    public void onWorkflowRerun(WorkflowModel workflow) {
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowRerun(workflow),
                                "onWorkflowRerun",
                                workflow.getWorkflowId()));
    }

    @Override
    public void onWorkflowPaused(WorkflowModel workflow) {
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowPaused(workflow),
                                "onWorkflowPaused",
                                workflow.getWorkflowId()));
    }

    @Override
    public void onWorkflowResumed(WorkflowModel workflow) {
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowResumed(workflow),
                                "onWorkflowResumed",
                                workflow.getWorkflowId()));
    }

    @Override
    public void onWorkflowRetried(WorkflowModel workflow) {
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowRetried(workflow),
                                "onWorkflowRetried",
                                workflow.getWorkflowId()));
    }

    private void safeInvoke(Runnable action, String methodName, String workflowId) {
        try {
            action.run();
        } catch (Exception e) {
            LOGGER.error(
                    "Error in {} for workflow {}: {}", methodName, workflowId, e.getMessage(), e);
            // Don't propagate - one listener failure shouldn't affect others
        }
    }
}