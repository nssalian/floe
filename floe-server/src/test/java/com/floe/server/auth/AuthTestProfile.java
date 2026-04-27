/*
 * Copyright 2026 The Floe Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.floe.server.auth;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

/** Test profile for authentication integration tests. Enables auth and uses in-memory stores. */
public class AuthTestProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                // Enable authentication for tests
                "floe.auth.enabled",
                "true",
                // Use in-memory stores
                "floe.store.type",
                "MEMORY",
                // Disable database audit logging (no DB in tests)
                "floe.security.audit.database-enabled",
                "false",
                // OIDC config - provide dummy values to satisfy config mapping
                "quarkus.oidc.auth-server-url",
                "http://localhost:8180/realms/test",
                "quarkus.oidc.client-id",
                "test-client",
                "quarkus.oidc.credentials.secret",
                "test-secret",
                "quarkus.oidc.application-type",
                "service",
                "quarkus.oidc.tenant-enabled",
                "false");
    }

    @Override
    public String getConfigProfile() {
        return "test";
    }
}
