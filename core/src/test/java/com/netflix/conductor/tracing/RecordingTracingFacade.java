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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Test double that records span names and context inject/extract for workflow tracing tests. */
class RecordingTracingFacade implements TracingFacade {

    private static final AutoCloseable NOOP_SCOPE = () -> {};

    private final boolean enabled;
    private final List<String> spanNames = new ArrayList<>();
    private final Map<String, String> tags = new HashMap<>();
    private final Map<String, String> carrier =
            Map.of("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");

    RecordingTracingFacade(boolean enabled) {
        this.enabled = enabled;
    }

    List<String> getSpanNames() {
        return List.copyOf(spanNames);
    }

    String getTag(String key) {
        return tags.get(key);
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void run(String spanName, Runnable runnable) {
        run(spanName, () -> {
            runnable.run();
            return null;
        });
    }

    @Override
    public <T> T run(String spanName, Supplier<T> supplier) {
        if (enabled) {
            spanNames.add(spanName);
        }
        return supplier.get();
    }

    @Override
    public void tag(String key, String value) {
        if (enabled && key != null && value != null) {
            tags.put(key, value);
        }
    }

    @Override
    public void injectCurrentContext(Map<String, Object> target, String contextKey) {
        if (!enabled || target == null) {
            return;
        }
        target.put(contextKey, new HashMap<>(carrier));
    }

    @Override
    public Map<String, String> extractContext(Map<String, Object> source, String contextKey) {
        if (source == null) {
            return Collections.emptyMap();
        }
        Object value = source.get(contextKey);
        if (!(value instanceof Map<?, ?> rawMap)) {
            return Collections.emptyMap();
        }
        Map<String, String> extracted = new HashMap<>();
        rawMap.forEach(
                (key, entryValue) -> {
                    if (key != null && entryValue != null) {
                        extracted.put(key.toString(), entryValue.toString());
                    }
                });
        return extracted;
    }

    @Override
    public AutoCloseable withContext(Map<String, String> contextMap) {
        if (!enabled || contextMap == null || contextMap.isEmpty()) {
            return NOOP_SCOPE;
        }
        spanNames.add("withContext");
        return NOOP_SCOPE;
    }
}
