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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * OpenTelemetry-backed tracing facade. Requires the OpenTelemetry Java agent with {@code
 * -Dotel.javaagent.experimental.sdk.enabled=true} for export; uses only the {@code
 * opentelemetry-api} on the application classpath.
 */
public class OpenTelemetryTracingFacade implements TracingFacade {

    private static final String TRACER_NAME = "conductor-core";

    private static final TextMapSetter<Map<String, String>> TEXT_MAP_SETTER =
            (carrier, key, value) -> {
                if (carrier != null) {
                    carrier.put(key, value);
                }
            };

    private static final TextMapGetter<Map<String, String>> TEXT_MAP_GETTER =
            new TextMapGetter<>() {
                @Override
                public Iterable<String> keys(Map<String, String> carrier) {
                    return carrier != null ? carrier.keySet() : Collections.emptySet();
                }

                @Override
                public String get(Map<String, String> carrier, String key) {
                    return carrier != null ? carrier.get(key) : null;
                }
            };

    private static final AutoCloseable NOOP_SCOPE = () -> {};

    private final Tracer tracer;
    private final TextMapPropagator propagator;

    public OpenTelemetryTracingFacade() {
        this.tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
        this.propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
    }

    @Override
    public boolean isEnabled() {
        return true;
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
        Span span = tracer.spanBuilder(spanName).startSpan();
        try (Scope scope = span.makeCurrent()) {
            return supplier.get();
        } catch (RuntimeException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public void tag(String key, String value) {
        if (value != null) {
            Span.current().setAttribute(key, value);
        }
    }

    @Override
    public void injectCurrentContext(Map<String, Object> target, String contextKey) {
        if (target == null) {
            return;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(Context.current(), carrier, TEXT_MAP_SETTER);
        if (!carrier.isEmpty()) {
            target.put(contextKey, carrier);
        }
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
        Map<String, String> carrier = new HashMap<>();
        rawMap.forEach(
                (key, entryValue) -> {
                    if (key != null && entryValue != null) {
                        carrier.put(key.toString(), entryValue.toString());
                    }
                });
        return carrier;
    }

    @Override
    public AutoCloseable withContext(Map<String, String> contextMap) {
        if (contextMap == null || contextMap.isEmpty()) {
            return NOOP_SCOPE;
        }
        Context extracted = propagator.extract(Context.root(), contextMap, TEXT_MAP_GETTER);
        Scope scope = extracted.makeCurrent();
        return scope::close;
    }
}
