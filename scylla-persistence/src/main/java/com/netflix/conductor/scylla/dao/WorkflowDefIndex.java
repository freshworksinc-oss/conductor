/*
 * Copyright 2022 Conductor Authors.
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
package com.netflix.conductor.scylla.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.LongSupplier;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

/**
 * The names and version numbers of all workflow definitions, read from the {@code
 * workflow_defs_index} table (one short string per definition: {@code name/version}), and kept in
 * memory for a short time.
 *
 * <p>The index is what lets the DAO answer "which names", "which versions of this name" and "what
 * is the latest version of each name" without reading a single definition body. A deployment can
 * have a hundred thousand definitions, so reading them one by one (or all at once) for a UI list is
 * not an option. {@link #invalidate()} is called on every create, update and remove, so a change is
 * visible at once on this node; other nodes see it after the time to live.
 */
final class WorkflowDefIndex {

    /** Reads the raw {@code name/version} strings (a full scan of the index partition). */
    interface Loader {
        List<String> load();
    }

    /** The index at one moment: immutable, apart from the lazily cached latest definitions. */
    static final class Snapshot {
        private final NavigableMap<String, NavigableSet<Integer>> versionsByName;
        private final int entries;
        private volatile List<WorkflowDef> latestDefs;

        Snapshot(NavigableMap<String, NavigableSet<Integer>> versionsByName, int entries) {
            this.versionsByName = versionsByName;
            this.entries = entries;
        }

        /** Number of (name, version) entries. */
        int entries() {
            return entries;
        }

        /** Distinct names, sorted. */
        List<String> names() {
            return new ArrayList<>(versionsByName.keySet());
        }

        /** The versions of a name, ascending; empty when the name is unknown. */
        NavigableSet<Integer> versions(String name) {
            NavigableSet<Integer> v = versionsByName.get(name);
            return v == null ? Collections.emptyNavigableSet() : v;
        }

        /** The highest version of a name, or -1 when the name is unknown. */
        int latestVersion(String name) {
            NavigableSet<Integer> v = versionsByName.get(name);
            return v == null || v.isEmpty() ? -1 : v.last();
        }

        NavigableMap<String, NavigableSet<Integer>> versionsByName() {
            return versionsByName;
        }

        /** The latest definition of every name, if already read for this snapshot (else null). */
        List<WorkflowDef> latestDefs() {
            return latestDefs;
        }

        void latestDefs(List<WorkflowDef> defs) {
            this.latestDefs = defs;
        }
    }

    private final Loader loader;
    private final long ttlMillis;
    private final LongSupplier clock;
    private volatile Snapshot snapshot;
    private volatile long loadedAt;

    WorkflowDefIndex(Loader loader, long ttlMillis, LongSupplier clock) {
        this.loader = loader;
        this.ttlMillis = ttlMillis;
        this.clock = clock;
    }

    /** The current index; reloaded when it is older than the time to live (one reader loads). */
    Snapshot get() {
        Snapshot s = snapshot;
        if (s != null && clock.getAsLong() - loadedAt < ttlMillis) {
            return s;
        }
        synchronized (this) {
            s = snapshot;
            if (s != null && clock.getAsLong() - loadedAt < ttlMillis) {
                return s; // another thread loaded it while this one waited
            }
            Snapshot fresh = parse(loader.load());
            snapshot = fresh;
            loadedAt = clock.getAsLong();
            return fresh;
        }
    }

    /** Forget what was read: the next {@link #get()} reads the index again. */
    void invalidate() {
        snapshot = null;
    }

    /** {@code name/version}: the version is after the LAST slash (names may contain slashes). */
    static Snapshot parse(List<String> entries) {
        NavigableMap<String, NavigableSet<Integer>> byName = new TreeMap<>();
        int count = 0;
        for (String e : entries) {
            int cut = e == null ? -1 : e.lastIndexOf('/');
            if (cut <= 0) {
                continue; // not a name/version entry
            }
            try {
                int version = Integer.parseInt(e.substring(cut + 1));
                byName.computeIfAbsent(e.substring(0, cut), k -> new TreeSet<>()).add(version);
                count++;
            } catch (NumberFormatException ignored) {
                // skip an entry whose version is not a number
            }
        }
        return new Snapshot(byName, count);
    }
}
