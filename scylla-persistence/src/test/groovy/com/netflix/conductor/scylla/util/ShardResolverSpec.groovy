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
package com.netflix.conductor.scylla.util

import com.netflix.conductor.scylla.util.ShardResolver.WorkflowOwner

import spock.lang.Specification
import spock.lang.Unroll

class ShardResolverSpec extends Specification {

    @Unroll
    def "account given: '#account' -> shard #shard"() {
        expect:
        ShardResolver.resolve("fs", account, "fs: 678: ORDER-1") == new WorkflowOwner(shard, "fs", account)

        where:
        account || shard
        "678"   || 678
        "1"     || 1
    }

    def "non-numeric, negative or oversized account hashes to a positive shard"() {
        expect:
        ["acme", "-5", "99999999999"].every {
            def owner = ShardResolver.resolve("fs", it, null)
            owner.shardId > 0 && owner.accountId == it
        }
    }

    def "account equal to the tenant, blank or -1 means no account"() {
        expect:
        ShardResolver.resolve("fs", account, "fs: ORDER-1") ==
                new WorkflowOwner(ShardResolver.tenantShard("fs"), "fs", "-1")

        where:
        account << ["fs", "", null, "-1"]
    }

    def "no account and an integer correlationId (Journeys): the integer is account and shard"() {
        expect:
        ShardResolver.resolve("journeys", null, "12345") == new WorkflowOwner(12345, "journeys", "12345")
    }

    @Unroll
    def "tenant only, correlationId '#correlationId' -> tenant shard"() {
        when:
        def owner = ShardResolver.resolve("fs", null, correlationId)

        then:
        owner == new WorkflowOwner(ShardResolver.tenantShard("fs"), "fs", "-1")

        where:
        correlationId << ["fs: ORDER-1", null, "ORDER-1", "0", "-7"]
    }

    def "header account wins over an integer correlationId"() {
        expect:
        ShardResolver.resolve("fs", "678", "12345").shardId == 678
    }

    def "tenant shards are negative, deterministic and within -1..-1,000,000"() {
        expect:
        ["fs", "journeys", "a", "tenant-123", "ünïcödé"].every {
            int shard = ShardResolver.tenantShard(it)
            shard <= -1 && shard >= -1_000_000 && shard == ShardResolver.tenantShard(it)
        }
        ShardResolver.tenantShard("fs") != ShardResolver.tenantShard("journeys")
    }

    def "murmur3 x86_32 seed 0 reference values"() {
        expect:
        ShardResolver.murmur3("") == 0
        ShardResolver.murmur3("hello") == 0x248bfa47
        ShardResolver.murmur3("The quick brown fox jumps over the lazy dog") == 0x2e4ff723
    }

    @Unroll
    def "legacy shard of '#correlationId' is #shard"() {
        expect:
        ShardResolver.legacyShard(correlationId) == shard

        where:
        correlationId   || shard
        "12345"         || 12345
        null            || 0
        ""              || 0
        "fs: ORDER-1"   || 0
    }
}
