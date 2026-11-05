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
package com.netflix.conductor.pgmirror;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RedisPgMirrorPublisherTest {

    /** A Redis list we control: it can fail, and report any length. */
    static class FakeList implements RedisListClient {
        final List<List<String>> pushes = new ArrayList<>();
        boolean failing = false;
        long length = 0;
        int rpushCalls = 0;

        @Override
        public long rpush(String key, List<String> values) {
            rpushCalls++;
            if (failing) {
                throw new RuntimeException("redis down");
            }
            pushes.add(new ArrayList<>(values));
            return pushes.size();
        }

        @Override
        public long llen(String key) {
            return length;
        }

        @Override
        public void close() {}
    }

    private static PgMirrorMessage message(String op) {
        return new PgMirrorMessage(
                1,
                op + "-id",
                op,
                "wf",
                1L,
                true,
                List.of(new PgStatement("DELETE FROM x WHERE a = $1", List.of("v"))));
    }

    private final AtomicLong now = new AtomicLong(1_000_000);

    private RedisPgMirrorPublisher publisher(FakeList redis) {
        PgMirrorProperties p = new PgMirrorProperties();
        p.setBreakerFailures(3);
        p.setBreakerOpenMs(10_000);
        p.setMaxQueueLength(100);
        return new RedisPgMirrorPublisher(redis, "pg-mirror:test:sql", p, now::get);
    }

    @Test
    public void allMessagesOfOneCallGoInOneRpushInOrder() throws Exception {
        FakeList redis = new FakeList();
        RedisPgMirrorPublisher pub = publisher(redis);
        pub.publish(List.of(message("a"), message("b"), message("c")));

        assertEquals(1, redis.rpushCalls);
        List<String> pushed = redis.pushes.get(0);
        assertEquals(3, pushed.size());
        PgMirrorMessage back = new ObjectMapper().readValue(pushed.get(1), PgMirrorMessage.class);
        assertEquals("b", back.op());
        assertEquals("DELETE FROM x WHERE a = $1", back.statements().get(0).sql());
        assertEquals(3, pub.published());
    }

    @Test
    public void nothingToPublishMakesNoCall() {
        FakeList redis = new FakeList();
        publisher(redis).publish(List.of());
        assertEquals(0, redis.rpushCalls);
    }

    @Test
    public void aFailedPushIsCountedNeverThrown() {
        FakeList redis = new FakeList();
        redis.failing = true;
        RedisPgMirrorPublisher pub = publisher(redis);
        pub.publish(List.of(message("a")));
        assertEquals(1, pub.failed());
        assertEquals(0, pub.published());
    }

    @Test
    public void theBreakerOpensAfterNFailuresSkipsThenClosesAgain() {
        FakeList redis = new FakeList();
        redis.failing = true;
        RedisPgMirrorPublisher pub = publisher(redis);
        for (int i = 0; i < 3; i++) { // 3 failures in a row open the breaker
            pub.publish(List.of(message("a")));
        }
        assertEquals(3, redis.rpushCalls);

        pub.publish(List.of(message("b"), message("c")));
        assertEquals("no call while the breaker is open", 3, redis.rpushCalls);
        assertEquals(2, pub.skippedBreaker());

        now.addAndGet(10_001); // the breaker time is over, Redis is back
        redis.failing = false;
        pub.publish(List.of(message("d")));
        assertEquals(4, redis.rpushCalls);
        assertEquals(1, pub.published());
    }

    @Test
    public void aSuccessResetsTheFailureCount() {
        FakeList redis = new FakeList();
        RedisPgMirrorPublisher pub = publisher(redis);
        redis.failing = true;
        pub.publish(List.of(message("a")));
        pub.publish(List.of(message("a")));
        redis.failing = false;
        pub.publish(List.of(message("a"))); // success: the two failures are forgotten
        redis.failing = true;
        pub.publish(List.of(message("a")));
        pub.publish(List.of(message("a")));
        int callsBefore = redis.rpushCalls;
        pub.publish(List.of(message("a"))); // third failure of a new series: the breaker opens now
        pub.publish(List.of(message("z")));
        assertEquals(callsBefore + 1, redis.rpushCalls);
        assertEquals(1, pub.skippedBreaker());
    }

    @Test
    public void aTooLongListPausesPushesUntilItIsShorterAgain() {
        FakeList redis = new FakeList();
        RedisPgMirrorPublisher pub = publisher(redis);
        redis.length = 101; // over the cap of 100
        pub.checkLengthNow();
        pub.publish(List.of(message("a")));
        assertEquals(0, redis.rpushCalls);
        assertEquals(1, pub.skippedFull());

        redis.length = 5;
        pub.checkLengthNow();
        pub.publish(List.of(message("a")));
        assertEquals(1, redis.rpushCalls);
    }

    @Test
    public void closeWithoutStartIsFine() {
        publisher(new FakeList()).close();
        assertTrue(true);
    }
}
