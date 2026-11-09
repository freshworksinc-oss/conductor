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
package com.netflix.conductor.rest.controllers;

import java.util.Map;

import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Local-dev stand-in for the production auth-proxy's /api/userinfo endpoint. In real deployments
 * this is served by the envoy/auth-proxy layer in front of Conductor, not by Conductor itself;
 * running the server standalone (e.g. `bootRun --args='--spring.profiles.active=local'`) has
 * nothing answering that route, so every RoleGate-protected UI action stays hidden. Only active
 * under the "local" profile so it can never shadow the real proxy elsewhere.
 */
@RestController
@Profile("local")
@RequestMapping(value = "/api/userinfo", produces = MediaType.APPLICATION_JSON_VALUE)
public class UserInfoResource {

    @GetMapping
    public Map<String, Object> getUserInfo() {
        return Map.of(
                "email", "local-dev@test.com", "tenants", Map.of("local-tenant", "SuperAdmin"));
    }
}
