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
package com.netflix.conductor.scylla.util;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.netflix.conductor.tenant.TenantContext;

/**
 * Decides a new workflow's owner and shard_id. Runs once, in createWorkflow; every later read and
 * write uses the stored shard (workflow_lookup / task_lookup), never the correlationId.
 *
 * <ol>
 *   <li>Account given: numeric and positive → that number, otherwise a positive murmur3 hash.
 *   <li>No account, correlationId a positive integer (Journeys) → that integer; it is the account.
 *   <li>Tenant only → {@code -(1 + floorMod(murmur3(tenant), 1_000_000))}, so -1 .. -1,000,000.
 * </ol>
 *
 * Account shards are > 0 and tenant shards < 0, so they never collide; 0 stays for legacy rows.
 * murmur3 (not String.hashCode) keeps the value identical across pods, restarts and languages.
 */
public final class ShardResolver {

    static final int TENANT_SHARD_RANGE = 1_000_000;

    private ShardResolver() {}

    public static WorkflowOwner resolve(String tenantId, String accountId, String correlationId) {
        String account = TenantContext.normaliseAccount(tenantId, accountId);
        if (account != null) {
            Integer numeric = positiveInt(account);
            int shard =
                    numeric != null
                            ? numeric
                            : 1 + Math.floorMod(murmur3(account), Integer.MAX_VALUE);
            return new WorkflowOwner(shard, tenantId, account);
        }
        Integer journeysAccount = positiveInt(correlationId);
        if (journeysAccount != null) {
            return new WorkflowOwner(journeysAccount, tenantId, correlationId.trim());
        }
        return new WorkflowOwner(tenantShard(tenantId), tenantId, TenantContext.NO_ACCOUNT);
    }

    public static int tenantShard(String tenantId) {
        return -(1 + Math.floorMod(murmur3(tenantId), TENANT_SHARD_RANGE));
    }

    /**
     * Shard of a workflow written before workflow_lookup carried the shard for every workflow: the
     * old rule, {@code parseInt(correlationId)} or 0.
     */
    public static int legacyShard(String correlationId) {
        if (StringUtils.isBlank(correlationId)) {
            return 0;
        }
        try {
            return Integer.parseInt(correlationId.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static Integer positiveInt(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * MurmurHash3 x86_32, seed 0, over the UTF-8 bytes. Written out rather than taken from Guava:
     * this module resolves an old Guava (via the Scylla driver), and the value must stay identical
     * across library upgrades and other languages' murmur3 implementations.
     */
    static int murmur3(String value) {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        int h1 = 0;
        int roundedEnd = data.length & ~3;
        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 =
                    (data[i] & 0xff)
                            | ((data[i + 1] & 0xff) << 8)
                            | ((data[i + 2] & 0xff) << 16)
                            | (data[i + 3] << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }
        int k1 = 0;
        switch (data.length & 3) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // fall through
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // fall through
            case 1:
                k1 |= data[roundedEnd] & 0xff;
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
                break;
            default:
                break;
        }
        h1 ^= data.length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }

    /** Stored owner and shard of a workflow. {@code tenantId} is null for legacy rows. */
    public static final class WorkflowOwner {
        private final int shardId;
        private final String tenantId;
        private final String accountId;

        public WorkflowOwner(int shardId, String tenantId, String accountId) {
            this.shardId = shardId;
            this.tenantId = tenantId;
            this.accountId = accountId;
        }

        public int getShardId() {
            return shardId;
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getAccountId() {
            return accountId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof WorkflowOwner)) {
                return false;
            }
            WorkflowOwner that = (WorkflowOwner) o;
            return shardId == that.shardId
                    && Objects.equals(tenantId, that.tenantId)
                    && Objects.equals(accountId, that.accountId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, tenantId, accountId);
        }

        @Override
        public String toString() {
            return "WorkflowOwner{shard="
                    + shardId
                    + ", tenant="
                    + tenantId
                    + ", account="
                    + accountId
                    + "}";
        }
    }
}
