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

package com.floe.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.auth.ApiKey;
import com.floe.core.auth.Role;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CreateApiKeyResponseTest {

    private final Instant createdAt = Instant.parse("2024-01-15T10:00:00Z");

    @Test
    void shouldCreateWithKeyAndApiKeyResponse() {
        ApiKeyResponse apiKeyResponse =
                new ApiKeyResponse(
                        "key-123",
                        "My API Key",
                        Role.ADMIN,
                        Set.of("READ_POLICIES", "WRITE_POLICIES"),
                        true,
                        createdAt,
                        null,
                        null,
                        "admin");

        CreateApiKeyResponse response = new CreateApiKeyResponse("floe_abc123xyz", apiKeyResponse);

        assertEquals("floe_abc123xyz", response.key());
        assertEquals(apiKeyResponse, response.apiKey());
        assertEquals("key-123", response.apiKey().id());
        assertEquals("My API Key", response.apiKey().name());
    }

    @Test
    void fromShouldCreateFromPlaintextKeyAndApiKey() {
        ApiKey apiKey =
                ApiKey.builder()
                        .id("key-123")
                        .name("New API Key")
                        .keyHash("hashed-key")
                        .role(Role.OPERATOR)
                        .enabled(true)
                        .createdAt(createdAt)
                        .createdBy("system")
                        .build();

        CreateApiKeyResponse response = CreateApiKeyResponse.from("floe_secret123", apiKey);

        assertEquals("floe_secret123", response.key());
        assertNotNull(response.apiKey());
        assertEquals("key-123", response.apiKey().id());
        assertEquals("New API Key", response.apiKey().name());
        assertEquals(Role.OPERATOR, response.apiKey().role());
        assertTrue(response.apiKey().enabled());
    }

    @Test
    void fromShouldIncludeAllApiKeyDetails() {
        Instant expiresAt = Instant.parse("2025-01-15T10:00:00Z");
        ApiKey apiKey =
                ApiKey.builder()
                        .id("key-456")
                        .name("Expiring Key")
                        .keyHash("hashed")
                        .role(Role.VIEWER)
                        .enabled(true)
                        .createdAt(createdAt)
                        .expiresAt(expiresAt)
                        .createdBy("admin@example.com")
                        .build();

        CreateApiKeyResponse response = CreateApiKeyResponse.from("floe_expiring", apiKey);

        assertEquals(expiresAt, response.apiKey().expiresAt());
        assertEquals("admin@example.com", response.apiKey().createdBy());
    }

    @Test
    void recordShouldSupportEquality() {
        ApiKeyResponse apiKeyResponse =
                new ApiKeyResponse(
                        "key-123",
                        "Key",
                        Role.VIEWER,
                        Set.of(),
                        true,
                        createdAt,
                        null,
                        null,
                        "admin");

        CreateApiKeyResponse response1 = new CreateApiKeyResponse("floe_key1", apiKeyResponse);
        CreateApiKeyResponse response2 = new CreateApiKeyResponse("floe_key1", apiKeyResponse);

        assertEquals(response1, response2);
        assertEquals(response1.hashCode(), response2.hashCode());
    }

    @Test
    void differentResponsesShouldNotBeEqual() {
        ApiKeyResponse apiKeyResponse =
                new ApiKeyResponse(
                        "key-123",
                        "Key",
                        Role.VIEWER,
                        Set.of(),
                        true,
                        createdAt,
                        null,
                        null,
                        "admin");

        CreateApiKeyResponse response1 = new CreateApiKeyResponse("floe_key1", apiKeyResponse);
        CreateApiKeyResponse response2 = new CreateApiKeyResponse("floe_key2", apiKeyResponse);

        assertNotEquals(response1, response2);
    }

    @Test
    void recordShouldHaveToString() {
        ApiKeyResponse apiKeyResponse =
                new ApiKeyResponse(
                        "key-123",
                        "My Key",
                        Role.ADMIN,
                        Set.of(),
                        true,
                        createdAt,
                        null,
                        null,
                        "admin");

        CreateApiKeyResponse response = new CreateApiKeyResponse("floe_secret", apiKeyResponse);
        String toString = response.toString();

        assertTrue(toString.contains("CreateApiKeyResponse"));
        assertTrue(toString.contains("floe_secret"));
    }
}
