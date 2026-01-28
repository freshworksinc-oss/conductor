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
        LOGGER.info(
                "Broadcasting onWorkflowCompleted event for workflow: {} (name: {}, status: {}) to {} listeners",
                workflow.getWorkflowId(),
                workflow.getWorkflowName(),
                workflow.getStatus(),
                listeners.size());
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowCompleted(workflow),
                                "onWorkflowCompleted",
                                workflow.getWorkflowId(),
                                listener));
    }

    @Override
    public void onWorkflowTerminated(WorkflowModel workflow) {
        LOGGER.info(
                "Broadcasting onWorkflowTerminated event for workflow: {} (name: {}, status: {}) to {} listeners",
                workflow.getWorkflowId(),
                workflow.getWorkflowName(),
                workflow.getStatus(),
                listeners.size());
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowTerminated(workflow),
                                "onWorkflowTerminated",
                                workflow.getWorkflowId(),
                                listener));
    }

    @Override
    public void onWorkflowFinalized(WorkflowModel workflow) {
        LOGGER.debug(
                "Broadcasting onWorkflowFinalized event for workflow: {} to {} listeners",
                workflow.getWorkflowId(),
                listeners.size());
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowFinalized(workflow),
                                "onWorkflowFinalized",
                                workflow.getWorkflowId(),
                                listener));
    }

    @Override
    public void onWorkflowStarted(WorkflowModel workflow) {
        LOGGER.info(
                "Broadcasting onWorkflowStarted event for workflow: {} (name: {}) to {} listeners",
                workflow.getWorkflowId(),
                workflow.getWorkflowName(),
                listeners.size());
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowStarted(workflow),
                                "onWorkflowStarted",
                                workflow.getWorkflowId(),
                                listener));
    }

    @Override
    public void onWorkflowRestarted(WorkflowModel workflow) {
        LOGGER.debug(
                "Broadcasting onWorkflowRestarted event for workflow: {} to {} listeners",
                workflow.getWorkflowId(),
                listeners.size());
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowRestarted(workflow),
                                "onWorkflowRestarted",
                                workflow.getWorkflowId(),
                                listener));
    }

    @Override
    public void onWorkflowRerun(WorkflowModel workflow) {
        LOGGER.debug(
                "Broadcasting onWorkflowRerun event for workflow: {} to {} listeners",
                workflow.getWorkflowId(),
                listeners.size());
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowRerun(workflow),
                                "onWorkflowRerun",
                                workflow.getWorkflowId(),
                                listener));
    }

    @Override
    public void onWorkflowPaused(WorkflowModel workflow) {
        LOGGER.debug(
                "Broadcasting onWorkflowPaused event for workflow: {} to {} listeners",
                workflow.getWorkflowId(),
                listeners.size());
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowPaused(workflow),
                                "onWorkflowPaused",
                                workflow.getWorkflowId(),
                                listener));
    }

    @Override
    public void onWorkflowResumed(WorkflowModel workflow) {
        LOGGER.debug(
                "Broadcasting onWorkflowResumed event for workflow: {} to {} listeners",
                workflow.getWorkflowId(),
                listeners.size());
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowResumed(workflow),
                                "onWorkflowResumed",
                                workflow.getWorkflowId(),
                                listener));
    }

    @Override
    public void onWorkflowRetried(WorkflowModel workflow) {
        LOGGER.debug(
                "Broadcasting onWorkflowRetried event for workflow: {} to {} listeners",
                workflow.getWorkflowId(),
                listeners.size());
        listeners.forEach(
                listener ->
                        safeInvoke(
                                () -> listener.onWorkflowRetried(workflow),
                                "onWorkflowRetried",
                                workflow.getWorkflowId(),
                                listener));
    }

    private void safeInvoke(
            Runnable action, String methodName, String workflowId, WorkflowStatusListener listener) {
        String listenerName = listener.getClass().getSimpleName();
        try {
            LOGGER.debug(
                    "Invoking {} on listener: {} for workflow: {}",
                    methodName,
                    listenerName,
                    workflowId);
            long startTime = System.currentTimeMillis();
            action.run();
            long duration = System.currentTimeMillis() - startTime;
            LOGGER.info(
                    "Successfully invoked {} on listener: {} for workflow: {} (took {}ms)",
                    methodName,
                    listenerName,
                    workflowId,
                    duration);
        } catch (Exception e) {
            LOGGER.error(
                    "Error in {} on listener: {} for workflow: {}: {}",
                    methodName,
                    listenerName,
                    workflowId,
                    e.getMessage(),
                    e);
            // Don't propagate - one listener failure shouldn't affect others
        }
    }
}