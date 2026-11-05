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

import java.util.List;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

/** The two Redis list calls the mirror needs, so the publisher can be tested without Redis. */
public interface RedisListClient extends AutoCloseable {

    /** Appends the values to the end of the list, in order, in one call. */
    long rpush(String key, List<String> values);

    long llen(String key);

    @Override
    void close();

    /** The real thing, on Jedis. */
    static RedisListClient jedis(PgMirrorProperties.Redis redis, int timeoutMs) {
        DefaultJedisClientConfig.Builder cfg =
                DefaultJedisClientConfig.builder()
                        .connectionTimeoutMillis(timeoutMs)
                        .socketTimeoutMillis(timeoutMs)
                        .ssl(redis.isSsl());
        if (redis.getUsername() != null && !redis.getUsername().isBlank()) {
            cfg.user(redis.getUsername());
        }
        if (redis.getPassword() != null && !redis.getPassword().isBlank()) {
            cfg.password(redis.getPassword());
        }
        JedisPooled jedis =
                new JedisPooled(new HostAndPort(redis.getHost(), redis.getPort()), cfg.build());
        return new RedisListClient() {
            @Override
            public long rpush(String key, List<String> values) {
                return jedis.rpush(key, values.toArray(new String[0]));
            }

            @Override
            public long llen(String key) {
                return jedis.llen(key);
            }

            @Override
            public void close() {
                jedis.close();
            }
        };
    }
}
