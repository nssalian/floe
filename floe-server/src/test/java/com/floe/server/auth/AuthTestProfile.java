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
