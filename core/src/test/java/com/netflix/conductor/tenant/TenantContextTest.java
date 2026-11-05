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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TenantContextTest {

    @After
    public void clear() {
        TenantContext.clear();
    }

    @Test
    public void blankTenantMeansNoContext() {
        assertNull(TenantContext.of(null, "678"));
        assertNull(TenantContext.of("  ", null));
    }

    @Test
    public void accountEqualToTenantBlankOrMinusOneMeansNoAccount() {
        assertNull(TenantContext.of("fs", "fs").getAccountId());
        assertNull(TenantContext.of("fs", "").getAccountId());
        assertNull(TenantContext.of("fs", "-1").getAccountId());
        assertEquals("678", TenantContext.of("fs", " 678 ").getAccountId());
    }

    @Test
    public void tenantWideCallerOwnsEveryAccountOfItsTenant() {
        TenantContext fs = TenantContext.of("fs", null);
        assertTrue(fs.owns("fs", "678"));
        assertTrue(fs.owns("fs", "-1"));
        assertFalse(fs.owns("journeys", "12345"));
    }

    @Test
    public void accountCallerOwnsOnlyItsAccount() {
        TenantContext fs678 = TenantContext.of("fs", "678");
        assertTrue(fs678.owns("fs", "678"));
        assertFalse(fs678.owns("fs", "999"));
        assertFalse(fs678.owns("fs", "-1"));
        assertFalse(fs678.owns("journeys", "678"));
    }

    @Test
    public void rowsWithoutStoredOwnerAreAllowed() {
        assertTrue(TenantContext.of("fs", "678").owns(null, null));
    }

    @Test
    public void runAsRestoresThePreviousContext() {
        TenantContext outer = TenantContext.of("fs", null);
        TenantContext inner = TenantContext.of("journeys", "12345");
        TenantContext.set(outer);
        AtomicReference<TenantContext> seen = new AtomicReference<>();

        TenantContext.runAs(inner, () -> seen.set(TenantContext.get()));

        assertSame(inner, seen.get());
        assertSame(outer, TenantContext.get());
    }

    @Test
    public void wrapCarriesTheContextToAnotherThread() throws Exception {
        TenantContext fs = TenantContext.of("fs", "678");
        TenantContext.set(fs);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            AtomicReference<TenantContext> seen = new AtomicReference<>();
            executor.submit(TenantContext.wrap(() -> seen.set(TenantContext.get()))).get();
            assertSame(fs, seen.get());
            // the pool thread is left clean for the next task
            assertNull(executor.submit(TenantContext::get).get());
        } finally {
            executor.shutdownNow();
        }
    }
}
