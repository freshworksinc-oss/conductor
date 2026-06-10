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
package com.netflix.conductor;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import io.pyroscope.http.Format;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.javaagent.PyroscopeAgent;
import io.pyroscope.javaagent.config.Config;
import jakarta.annotation.PostConstruct;

/**
 * Starts the Pyroscope continuous profiling agent (CPU/itimer profiles) when {@code
 * conductor.pyroscope.enabled=true}. Profiles are pushed to a Pyroscope-compatible server and are
 * complementary to the Prometheus metrics already exported by the server.
 */
@Configuration
@ConditionalOnProperty(name = "conductor.pyroscope.enabled", havingValue = "true")
public class PyroscopeConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(PyroscopeConfiguration.class);

    @Value("${conductor.pyroscope.application-name:conductor-server}")
    private String applicationName;

    @Value("${conductor.pyroscope.server-address}")
    private String serverAddress;

    @Value("${conductor.pyroscope.auth-token:}")
    private String authToken;

    @PostConstruct
    public void init() {
        Config.Builder builder =
                new Config.Builder()
                        .setApplicationName(applicationName)
                        .setProfilingEvent(EventType.ITIMER)
                        .setFormat(Format.JFR)
                        .setServerAddress(serverAddress);

        if (StringUtils.hasText(authToken)) {
            builder.setAuthToken(authToken);
        }

        Map<String, String> labels = new HashMap<>();
        String podName = System.getenv("POD_NAME");
        if (StringUtils.hasText(podName)) {
            labels.put("host", podName);
        }
        if (!labels.isEmpty()) {
            builder.setLabels(labels);
        }

        PyroscopeAgent.start(builder.build());
        LOGGER.info(
                "Pyroscope profiling started for application '{}' -> {}",
                applicationName,
                serverAddress);
    }
}
