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

/**
 * Which tenant metadata implementation runs, with no property of its own: it follows the
 * persistence, {@code conductor.db.type}.
 *
 * <ul>
 *   <li>{@code postgres}: the Postgres {@code meta_*} tables, {@link PostgresTenantMetadataDAO}
 *   <li>anything else (scylla, redis, ...): the persistence's own {@code MetadataDAO}, {@link
 *       DaoTenantMetadataDAO}; no Postgres needed
 * </ul>
 *
 * {@code conductor.tenant-metadata.enabled=false} is the only property: it turns {@code
 * /api/tenant/metadata} off.
 */
public final class TenantMetadataConditions {

    private static final String ENABLED = "${conductor.tenant-metadata.enabled:true}";
    private static final String DB_TYPE = "'${conductor.db.type:}'";

    /** SpEL for {@code @ConditionalOnExpression}: served, on Postgres persistence. */
    public static final String POSTGRES = ENABLED + " and " + DB_TYPE + " == 'postgres'";

    /** SpEL for {@code @ConditionalOnExpression}: served, on any other persistence. */
    public static final String METADATA_DAO = ENABLED + " and " + DB_TYPE + " != 'postgres'";

    /** SpEL for {@code @ConditionalOnExpression}: tenant metadata is served at all. */
    public static final String SERVED = ENABLED;

    private TenantMetadataConditions() {}
}
