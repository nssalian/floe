package com.floe.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.auth.ApiKey;
import com.floe.core.auth.Permission;
import com.floe.core.auth.Role;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ApiKeyResponseTest {

    private final Instant createdAt = Instant.parse("2024-01-15T10:00:00Z");
    private final Instant expiresAt = Instant.parse("2025-01-15T10:00:00Z");
    private final Instant lastUsedAt = Instant.parse("2024-06-15T10:00:00Z");

    @Test
    void shouldCreateWithAllFields() {
        Set<String> permissions = Set.of("READ_POLICIES", "WRITE_POLICIES");
        ApiKeyResponse response =
                new ApiKeyResponse(
                        "key-123",
                        "My API Key",
                        Role.OPERATOR,
                        permissions,
                        true,
                        createdAt,
                        expiresAt,
                        lastUsedAt,
                        "admin@example.com");

        assertEquals("key-123", response.id());
        assertEquals("My API Key", response.name());
        assertEquals(Role.OPERATOR, response.role());
        assertEquals(permissions, response.permissions());
        assertTrue(response.enabled());
        assertEquals(createdAt, response.createdAt());
        assertEquals(expiresAt, response.expiresAt());
        assertEquals(lastUsedAt, response.lastUsedAt());
        assertEquals("admin@example.com", response.createdBy());
    }

    @Test
    void fromShouldConvertApiKeyToResponse() {
        ApiKey apiKey =
                ApiKey.builder()
                        .id("key-123")
                        .name("Admin Key")
                        .keyHash("hashed-key")
                        .role(Role.ADMIN)
                        .enabled(true)
                        .createdAt(createdAt)
                        .expiresAt(expiresAt)
                        .lastUsedAt(lastUsedAt)
                        .createdBy("system")
                        .build();

        ApiKeyResponse response = ApiKeyResponse.from(apiKey);

        assertEquals("key-123", response.id());
        assertEquals("Admin Key", response.name());
        assertEquals(Role.ADMIN, response.role());
        assertTrue(response.enabled());
        assertEquals(createdAt, response.createdAt());
        assertEquals(expiresAt, response.expiresAt());
        assertEquals(lastUsedAt, response.lastUsedAt());
        assertEquals("system", response.createdBy());

        // ADMIN should have all permissions
        assertTrue(response.permissions().contains(Permission.READ_POLICIES.name()));
        assertTrue(response.permissions().contains(Permission.WRITE_POLICIES.name()));
        assertTrue(response.permissions().contains(Permission.TRIGGER_MAINTENANCE.name()));
        assertTrue(response.permissions().contains(Permission.MANAGE_API_KEYS.name()));
    }

    @Test
    void fromShouldMapViewerPermissions() {
        ApiKey apiKey =
                ApiKey.builder()
                        .id("key-456")
                        .name("Viewer Key")
                        .keyHash("hashed-key")
                        .role(Role.VIEWER)
                        .enabled(true)
                        .createdAt(createdAt)
                        .createdBy("admin")
                        .build();

        ApiKeyResponse response = ApiKeyResponse.from(apiKey);

        assertEquals(Role.VIEWER, response.role());
        assertTrue(response.permissions().contains(Permission.READ_POLICIES.name()));
        assertFalse(response.permissions().contains(Permission.WRITE_POLICIES.name()));
        assertFalse(response.permissions().contains(Permission.MANAGE_API_KEYS.name()));
    }

    @Test
    void fromShouldMapOperatorPermissions() {
        ApiKey apiKey =
                ApiKey.builder()
                        .id("key-789")
                        .name("Operator Key")
                        .keyHash("hashed-key")
                        .role(Role.OPERATOR)
                        .enabled(true)
                        .createdAt(createdAt)
                        .createdBy("admin")
                        .build();

        ApiKeyResponse response = ApiKeyResponse.from(apiKey);

        assertEquals(Role.OPERATOR, response.role());
        assertTrue(response.permissions().contains(Permission.READ_POLICIES.name()));
        assertTrue(response.permissions().contains(Permission.TRIGGER_MAINTENANCE.name()));
        assertFalse(response.permissions().contains(Permission.MANAGE_API_KEYS.name()));
    }

    @Test
    void fromShouldHandleNullOptionalFields() {
        ApiKey apiKey =
                ApiKey.builder()
                        .id("key-123")
                        .name("Basic Key")
                        .keyHash("hashed-key")
                        .role(Role.VIEWER)
                        .enabled(true)
                        .createdAt(createdAt)
                        .createdBy("admin")
                        .build();

        ApiKeyResponse response = ApiKeyResponse.from(apiKey);

        assertNull(response.expiresAt());
        assertNull(response.lastUsedAt());
    }

    @Test
    void recordShouldSupportEquality() {
        Set<String> permissions = Set.of("READ_POLICIES");
        ApiKeyResponse response1 =
                new ApiKeyResponse(
                        "key-123",
                        "Key",
                        Role.VIEWER,
                        permissions,
                        true,
                        createdAt,
                        null,
                        null,
                        "admin");
        ApiKeyResponse response2 =
                new ApiKeyResponse(
                        "key-123",
                        "Key",
                        Role.VIEWER,
                        permissions,
                        true,
                        createdAt,
                        null,
                        null,
                        "admin");

        assertEquals(response1, response2);
        assertEquals(response1.hashCode(), response2.hashCode());
    }

    @Test
    void differentResponsesShouldNotBeEqual() {
        Set<String> permissions = Set.of("READ_POLICIES");
        ApiKeyResponse response1 =
                new ApiKeyResponse(
                        "key-123",
                        "Key",
                        Role.VIEWER,
                        permissions,
                        true,
                        createdAt,
                        null,
                        null,
                        "admin");
        ApiKeyResponse response2 =
                new ApiKeyResponse(
                        "key-456",
                        "Key",
                        Role.VIEWER,
                        permissions,
                        true,
                        createdAt,
                        null,
                        null,
                        "admin");

        assertNotEquals(response1, response2);
    }
}
