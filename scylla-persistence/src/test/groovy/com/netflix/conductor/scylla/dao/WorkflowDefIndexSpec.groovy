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
package com.netflix.conductor.scylla.dao

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import spock.lang.Specification

/** No Scylla needed: the index logic only. */
class WorkflowDefIndexSpec extends Specification {

    AtomicLong now = new AtomicLong(1000)
    AtomicInteger loads = new AtomicInteger()
    List<String> entries = ["b/1", "a/2", "a/1", "a/10", "x/y/z/3"]

    WorkflowDefIndex index(long ttl) {
        new WorkflowDefIndex({ loads.incrementAndGet(); entries } as WorkflowDefIndex.Loader, ttl, { now.get() })
    }

    def "names are sorted and versions are numeric"() {
        when:
        def s = index(1000).get()

        then:
        s.names() == ["a", "b", "x/y/z"]
        s.versions("a") as List == [1, 2, 10]
        s.latestVersion("a") == 10
        s.latestVersion("x/y/z") == 3
        s.latestVersion("nope") == -1
        s.versions("nope").isEmpty()
        s.entries() == 5
    }

    def "entries that are not name/version are skipped"() {
        given:
        entries = ["a/1", "noslash", "/5", "a/notanumber", "a/2"]

        when:
        def s = index(1000).get()

        then:
        s.names() == ["a"]
        s.versions("a") as List == [1, 2]
    }

    def "is read once within the time to live and again after it"() {
        given:
        def idx = index(1000)

        when:
        def first = idx.get()
        now.addAndGet(999)

        then:
        idx.get().is(first)
        loads.get() == 1

        when:
        now.addAndGet(2)
        entries = ["a/1", "a/2", "c/1"]
        def second = idx.get()

        then:
        loads.get() == 2
        second.names() == ["a", "c"]
    }

    def "invalidate makes the next read see the change"() {
        given:
        def idx = index(60000)
        idx.get()
        entries = ["a/1", "a/2", "a/3"]

        expect:
        idx.get().latestVersion("a") == 10

        when:
        idx.invalidate()

        then:
        idx.get().latestVersion("a") == 3
        loads.get() == 2
    }
}
