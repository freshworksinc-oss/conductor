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
package com.netflix.conductor.tracing;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Pluggable facade for optional manual OpenTelemetry business tracing. When disabled, all
 * implementations must be no-ops with zero overhead.
 */
public interface TracingFacade {

    String TRACE_CONTEXT_VARIABLE_KEY = "_trace_context";

    boolean isEnabled();

    void run(String spanName, Runnable runnable);

    <T> T run(String spanName, Supplier<T> supplier);

    void tag(String key, String value);

    void injectCurrentContext(Map<String, Object> target, String contextKey);

    Map<String, String> extractContext(Map<String, Object> source, String contextKey);

    AutoCloseable withContext(Map<String, String> contextMap);

    default <T> T runWithStoredContext(
            Map<String, Object> source,
            String contextKey,
            String spanName,
            Supplier<T> action) {
        if (!isEnabled()) {
            return action.get();
        }
        Map<String, String> context = extractContext(source, contextKey);
        try (AutoCloseable scope = withContext(context)) {
            return run(spanName, action);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    default void runWithStoredContext(
            Map<String, Object> source,
            String contextKey,
            String spanName,
            Runnable action) {
        runWithStoredContext(
                source,
                contextKey,
                spanName,
                () -> {
                    action.run();
                    return null;
                });
    }
}
