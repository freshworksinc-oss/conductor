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

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/** Default no-op implementation used when manual tracing is disabled. */
public class NoopTracingFacade implements TracingFacade {

    private static final AutoCloseable NOOP_SCOPE = () -> {};

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public void run(String spanName, Runnable runnable) {
        runnable.run();
    }

    @Override
    public <T> T run(String spanName, Supplier<T> supplier) {
        return supplier.get();
    }

    @Override
    public void tag(String key, String value) {}

    @Override
    public void injectCurrentContext(Map<String, Object> target, String contextKey) {}

    @Override
    public Map<String, String> extractContext(Map<String, Object> source, String contextKey) {
        return Collections.emptyMap();
    }

    @Override
    public AutoCloseable withContext(Map<String, String> contextMap) {
        return NOOP_SCOPE;
    }
}
