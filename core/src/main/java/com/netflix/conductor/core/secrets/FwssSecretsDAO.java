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
package com.netflix.conductor.core.secrets;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.netflix.conductor.dao.SecretsDAO;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Reads secrets from Freshworks Secrets Service by shelling out to the fw-secrets-util CLI,
 * always as our own single fixed FWSS tenant. Other tenants share secrets into ours via FWSS's
 * own sharing mechanism, so a secret name here is the fully-qualified "&lt;owning-tenant&gt;/&lt;name&gt;"
 * (e.g. "ceapps/kafka-sandbox"), never just a bare name.
 *
 * <p>Read-only, and every call is bounded by {@code timeoutSeconds}: the CLI has been observed to
 * hang rather than fail fast under some AWS auth/network conditions, so a stuck subprocess is
 * force-killed rather than left to block the caller indefinitely.
 */
@Component
@ConditionalOnProperty(name = "conductor.secrets.type", havingValue = "fwss")
public class FwssSecretsDAO implements SecretsDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(FwssSecretsDAO.class);
    private static final Pattern EXPORT_LINE = Pattern.compile("export\\s+\\S+=\"([^\"]*)\"");

    private final String cliPath;
    private final String tenant;
    private final String environment;
    private final String region;
    private final int timeoutSeconds;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public FwssSecretsDAO(
            @Value("${conductor.secrets.fwss.cliPath:fw-secrets-util}") String cliPath,
            @Value("${conductor.secrets.fwss.tenant}") String tenant,
            @Value("${conductor.secrets.fwss.environment}") String environment,
            @Value("${conductor.secrets.fwss.region:}") String region,
            @Value("${conductor.secrets.fwss.timeoutSeconds:3}") int timeoutSeconds) {
        this.cliPath = cliPath;
        this.tenant = tenant;
        this.environment = environment;
        this.region = region;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public String getSecret(String name) {
        // "name" comes from a workflow definition, i.e. it's author-controlled input, not
        // trusted config. The "--" stops the CLI's flag parser before it, so a name crafted to
        // look like a flag (e.g. "--assume-role=...") is taken literally as a secret name
        // instead of being parsed as an argument to fw-secrets-util itself.
        List<String> command = new ArrayList<>();
        command.add(cliPath);
        command.add("get");
        command.add("secret");
        command.add("--output=env-variable");
        addCommonFlags(command);
        command.add("--");
        command.add(name);

        String output = runCommand(command);
        if (output == null) {
            return null;
        }
        Matcher matcher = EXPORT_LINE.matcher(output);
        if (!matcher.find()) {
            LOGGER.warn("Unexpected fw-secrets-util output for secret '{}'", name);
            return null;
        }
        try {
            return new String(Base64.getDecoder().decode(matcher.group(1)), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            // Keep this DAO's contract uniform: every other failure mode here returns null
            // rather than throwing, so a malformed response shouldn't be different.
            LOGGER.warn("fw-secrets-util returned non-base64 output for secret '{}'", name);
            return null;
        }
    }

    @Override
    public boolean secretExists(String name) {
        return getSecret(name) != null;
    }

    @Override
    public List<String> listSecretNames() {
        List<String> command = new ArrayList<>();
        command.add(cliPath);
        command.add("get");
        command.add("secret");
        command.add("--all");
        command.add("--output=json");
        addCommonFlags(command);

        String output = runCommand(command);
        if (output == null) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        try {
            JsonNode root = objectMapper.readTree(output);
            Iterator<String> tenants = root.fieldNames();
            while (tenants.hasNext()) {
                String tenantName = tenants.next();
                Iterator<String> secretNames = root.get(tenantName).fieldNames();
                while (secretNames.hasNext()) {
                    names.add(tenantName + "/" + secretNames.next());
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to parse fw-secrets-util --all output", e);
            return List.of();
        }
        return names;
    }

    @Override
    public void putSecret(String name, String value) {
        throw new UnsupportedOperationException(
                "fwss-backed secrets are read-only; manage secrets via fw-secrets-service directly");
    }

    @Override
    public void deleteSecret(String name) {
        throw new UnsupportedOperationException(
                "fwss-backed secrets are read-only; manage secrets via fw-secrets-service directly");
    }

    private void addCommonFlags(List<String> command) {
        command.add("--tenant=" + tenant);
        command.add("--environment=" + environment);
        if (!region.isEmpty()) {
            command.add("--region=" + region);
        }
    }

    /**
     * Runs the CLI with a hard timeout. Waits for the process to exit before reading its output,
     * since reading first would block on a hung process and the timeout would never get a chance
     * to fire. Returns null on any failure (timeout, non-zero exit, or I/O error) - callers treat
     * that the same as "secret not found", matching the rest of the DAO's null-on-miss contract.
     */
    private String runCommand(List<String> command) {
        Process process = null;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
            if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
                LOGGER.warn("fw-secrets-util timed out after {}s", timeoutSeconds);
                return null;
            }
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (process.exitValue() != 0) {
                // Never log the process output here - on a non-zero exit it's almost always just
                // an error message, but it could in principle carry a partially-emitted secret
                // value, and this warning is expected to end up in exported/shipped logs.
                LOGGER.warn("fw-secrets-util exited with {}", process.exitValue());
                return null;
            }
            return output;
        } catch (IOException | InterruptedException e) {
            LOGGER.warn("Failed to run fw-secrets-util", e);
            return null;
        } finally {
            // destroyForcibly() on an already-terminated process is a harmless no-op, so this
            // covers both the timeout case and normal cleanup in one place. Explicitly closing
            // the stream matters here specifically because there's no caching - every secret
            // lookup spawns a fresh process, so a leaked file descriptor per call adds up fast.
            if (process != null) {
                process.destroyForcibly();
                try {
                    process.getInputStream().close();
                } catch (IOException ignored) {
                    // best-effort cleanup only
                }
            }
        }
    }
}
