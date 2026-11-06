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

import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;

/**
 * Tenant and account of the caller, carried from the REST filter to the persistence layer on the
 * request thread.
 *
 * <p>A plain (non-inheritable) ThreadLocal: pool threads are reused, so an inherited value would be
 * a stale tenant from whichever request created the thread. Work handed to an executor must be
 * wrapped with {@link #wrap(Runnable)} / {@link #wrap(Callable)} to carry the context across.
 *
 * <p>{@code null} from {@link #get()} means no caller context (internal work such as the decider,
 * sweeper or system-task workers, or a request without tenant headers); persistence then trusts the
 * call and skips ownership checks.
 */
public final class TenantContext {

    /** Stored account for workflows created without an account. */
    public static final String NO_ACCOUNT = "-1";

    private static final ThreadLocal<TenantContext> CURRENT = new ThreadLocal<>();

    private final String tenantId;
    private final String accountId;

    private TenantContext(String tenantId, String accountId) {
        this.tenantId = tenantId;
        this.accountId = accountId;
    }

    /**
     * Builds a context, normalising the account: blank, {@code "-1"} or equal to the tenant means
     * no account (the auth proxy sets accountID to the tenant when x-account-id is absent).
     *
     * @return {@code null} when {@code tenantId} is blank
     */
    public static TenantContext of(String tenantId, String accountId) {
        if (StringUtils.isBlank(tenantId)) {
            return null;
        }
        String tenant = tenantId.trim();
        return new TenantContext(tenant, normaliseAccount(tenant, accountId));
    }

    /**
     * @return the account, or {@code null} when it means "no account" for this tenant
     */
    public static String normaliseAccount(String tenantId, String accountId) {
        if (StringUtils.isBlank(accountId)) {
            return null;
        }
        String account = accountId.trim();
        if (NO_ACCOUNT.equals(account) || account.equals(tenantId)) {
            return null;
        }
        return account;
    }

    public static TenantContext get() {
        return CURRENT.get();
    }

    public static void set(TenantContext context) {
        if (context == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(context);
        }
    }

    public static void clear() {
        CURRENT.remove();
    }

    /** Runs {@code action} with {@code context} set, restoring the previous context afterwards. */
    public static void runAs(TenantContext context, Runnable action) {
        TenantContext previous = CURRENT.get();
        set(context);
        try {
            action.run();
        } finally {
            set(previous);
        }
    }

    /** Captures the current context so {@code action} runs with it on another thread. */
    public static Runnable wrap(Runnable action) {
        TenantContext captured = CURRENT.get();
        return () -> runAs(captured, action);
    }

    /** Captures the current context so {@code action} runs with it on another thread. */
    public static <V> Callable<V> wrap(Callable<V> action) {
        TenantContext captured = CURRENT.get();
        return () -> {
            TenantContext previous = CURRENT.get();
            set(captured);
            try {
                return action.call();
            } finally {
                set(previous);
            }
        };
    }

    public String getTenantId() {
        return tenantId;
    }

    /**
     * @return the account, or {@code null} when the caller sent none
     */
    public String getAccountId() {
        return accountId;
    }

    /**
     * Whether this caller may access a workflow owned by {@code ownerTenant} / {@code
     * ownerAccount}. A caller without an account may access every workflow of its tenant. Rows
     * without a stored tenant (written before ownership was recorded) are allowed.
     */
    public boolean owns(String ownerTenant, String ownerAccount) {
        if (StringUtils.isBlank(ownerTenant)) {
            return true;
        }
        if (!tenantId.equals(ownerTenant)) {
            return false;
        }
        return accountId == null || Objects.equals(accountId, ownerAccount);
    }

    @Override
    public String toString() {
        return "TenantContext{tenantId=" + tenantId + ", accountId=" + accountId + "}";
    }
}
