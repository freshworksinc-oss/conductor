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
package com.netflix.conductor.tenant;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.contribs.metrics.MetricsCollector;
import com.netflix.conductor.dao.MetadataDAO;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import jakarta.annotation.PostConstruct;

/**
 * Workflow-level metrics (workflow_start_success, workflow_execution, workflow_failure, ...) are
 * tagged with the owner app of the running workflow, which is never set on the REST path, so they
 * all report ownerApp="unknown". The tenant lives on the workflow definition (ownerApp is stamped
 * with the tenant by the auth proxy), so this filter rewrites the tag at meter registration time
 * using the latest definition of the workflow.
 *
 * <p>Kept as a self-contained component so no upstream class has to change. Any failure leaves the
 * tag untouched.
 */
@Component
public class WorkflowOwnerMeterFilter implements MeterFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowOwnerMeterFilter.class);

    static final String OWNER_TAG = "ownerApp";
    static final String WORKFLOW_TAG = "workflowName";
    static final String UNKNOWN = "unknown";

    private final Function<String, Optional<WorkflowDef>> definitionLookup;
    private final LongSupplier clock;
    private final long ttlMillis;
    private final Map<String, CachedOwner> cache = new ConcurrentHashMap<>();

    // Guards against re-entry: looking up a definition may itself record meters
    private final ThreadLocal<Boolean> resolving = ThreadLocal.withInitial(() -> false);

    @Autowired
    public WorkflowOwnerMeterFilter(
            ObjectProvider<MetadataDAO> metadataDAO,
            @Value("${conductor.metrics.workflow-owner.cache-ttl-seconds:300}") long ttlSeconds) {
        this(
                name -> {
                    MetadataDAO dao = metadataDAO.getIfAvailable();
                    return dao == null ? Optional.empty() : dao.getLatestWorkflowDef(name);
                },
                System::currentTimeMillis,
                ttlSeconds * 1000);
    }

    WorkflowOwnerMeterFilter(
            Function<String, Optional<WorkflowDef>> definitionLookup,
            LongSupplier clock,
            long ttlMillis) {
        this.definitionLookup = definitionLookup;
        this.clock = clock;
        this.ttlMillis = ttlMillis;
    }

    @PostConstruct
    public void register() {
        MeterRegistry registry = MetricsCollector.getMeterRegistry();
        registry.config().meterFilter(this);
        LOGGER.info("Registered {} on the metrics registry", getClass().getSimpleName());
    }

    @Override
    public Meter.Id map(Meter.Id id) {
        try {
            String workflowName = id.getTag(WORKFLOW_TAG);
            if (workflowName == null || !UNKNOWN.equals(id.getTag(OWNER_TAG))) {
                return id;
            }
            String owner = resolveOwner(workflowName);
            if (owner == null) {
                return id;
            }
            List<Tag> tags =
                    id.getTags().stream()
                            .map(t -> OWNER_TAG.equals(t.getKey()) ? Tag.of(OWNER_TAG, owner) : t)
                            .collect(Collectors.toList());
            return id.replaceTags(Tags.of(tags));
        } catch (Exception e) {
            LOGGER.debug("Could not resolve owner for meter {}", id.getName(), e);
            return id;
        }
    }

    private String resolveOwner(String workflowName) {
        long now = clock.getAsLong();
        CachedOwner cached = cache.get(workflowName);
        if (cached != null && cached.expiresAt > now) {
            return cached.owner;
        }
        if (resolving.get()) {
            return cached == null ? null : cached.owner;
        }
        resolving.set(true);
        try {
            String owner =
                    definitionLookup
                            .apply(workflowName)
                            .map(WorkflowDef::getOwnerApp)
                            .filter(StringUtils::isNotBlank)
                            .orElse(null);
            cache.put(workflowName, new CachedOwner(owner, now + ttlMillis));
            return owner;
        } catch (Exception e) {
            LOGGER.debug("Definition lookup failed for workflow {}", workflowName, e);
            return cached == null ? null : cached.owner;
        } finally {
            resolving.set(false);
        }
    }

    private static final class CachedOwner {
        private final String owner;
        private final long expiresAt;

        private CachedOwner(String owner, long expiresAt) {
            this.owner = owner;
            this.expiresAt = expiresAt;
        }
    }
}
