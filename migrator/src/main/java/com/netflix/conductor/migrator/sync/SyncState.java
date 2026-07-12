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
package com.netflix.conductor.migrator.sync;

/**
 * In-memory per-tree sync state, keyed by root workflow id. Not persisted (re-derived on restart).
 */
public class SyncState {

    public enum State {
        SYNCING,
        ERROR
    }

    private final String rootId;
    private String lastSignature;
    private long lastSyncedAtMillis;
    private State state = State.SYNCING;
    private String lastReason;

    public SyncState(String rootId) {
        this.rootId = rootId;
    }

    public String getRootId() {
        return rootId;
    }

    public String getLastSignature() {
        return lastSignature;
    }

    public void setLastSignature(String lastSignature) {
        this.lastSignature = lastSignature;
    }

    public long getLastSyncedAtMillis() {
        return lastSyncedAtMillis;
    }

    public void setLastSyncedAtMillis(long lastSyncedAtMillis) {
        this.lastSyncedAtMillis = lastSyncedAtMillis;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String getLastReason() {
        return lastReason;
    }

    public void setLastReason(String lastReason) {
        this.lastReason = lastReason;
    }
}
