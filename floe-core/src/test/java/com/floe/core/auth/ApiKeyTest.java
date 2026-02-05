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

package com.floe.core.auth;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ApiKeyTest {

    // Record Tests

    @Test
    @DisplayName("should create ApiKey with all fields")
    void shouldCreateApiKeyWithAllFields() {
        Instant now = Instant.now();
        Instant expiresAt = now.plus(30, ChronoUnit.DAYS);

        ApiKey key =
                new ApiKey(
                        "key-123",
                        "hash-abc",
                        "test-key",
                        Role.OPERATOR,
                        true,
                        now,
                        expiresAt,
                        now,
                        "creator-456");

        assertEquals("key-123", key.id());
        assertEquals("hash-abc", key.keyHash());
        assertEquals("test-key", key.name());
        assertEquals(Role.OPERATOR, key.role());
        assertTrue(key.enabled());
        assertEquals(now, key.createdAt());
        assertEquals(expiresAt, key.expiresAt());
        assertEquals(now, key.lastUsedAt());
        assertEquals("creator-456", key.createdBy());
    }

    @Test
    @DisplayName("should have correct key prefix")
    void shouldHaveCorrectKeyPrefix() {
        assertEquals("floe_", ApiKey.KEY_PREFIX);
    }

    @Test
    @DisplayName("should have correct key random length")
    void shouldHaveCorrectKeyRandomLength() {
        assertEquals(32, ApiKey.KEY_RANDOM_LENGTH);
    }

    @Test
    @DisplayName("should not be expired when expiresAt is null")
    void shouldNotBeExpiredWhenExpiresAtIsNull() {
        ApiKey key =
                ApiKey.builder()
                        .keyHash("hash")
                        .name("test")
                        .role(Role.VIEWER)
                        .expiresAt(null)
                        .build();

        assertFalse(key.isExpired());
    }

    @Test
    @DisplayName("should not be expired when expiresAt is in the future")
    void shouldNotBeExpiredWhenExpiresAtIsInFuture() {
        ApiKey key =
                ApiKey.builder()
                        .keyHash("hash")
                        .name("test")
                        .role(Role.VIEWER)
                        .expiresAt(Instant.now().plus(1, ChronoUnit.DAYS))
                        .build();

        assertFalse(key.isExpired());
    }

    @Test
    @DisplayName("should be expired when expiresAt is in the past")
    void shouldBeExpiredWhenExpiresAtIsInPast() {
        ApiKey key =
                ApiKey.builder()
                        .keyHash("hash")
                        .name("test")
                        .role(Role.VIEWER)
                        .expiresAt(Instant.now().minus(1, ChronoUnit.DAYS))
                        .build();

        assertTrue(key.isExpired());
    }

    @Test
    @DisplayName("should be valid when enabled and not expired")
    void shouldBeValidWhenEnabledAndNotExpired() {
        ApiKey key =
                ApiKey.builder()
                        .keyHash("hash")
                        .name("test")
                        .role(Role.VIEWER)
                        .enabled(true)
                        .expiresAt(null)
                        .build();

        assertTrue(key.isValid());
    }

    @Test
    @DisplayName("should not be valid when disabled")
    void shouldNotBeValidWhenDisabled() {
        ApiKey key =
                ApiKey.builder()
                        .keyHash("hash")
                        .name("test")
                        .role(Role.VIEWER)
                        .enabled(false)
                        .expiresAt(null)
                        .build();

        assertFalse(key.isValid());
    }

    @Test
    @DisplayName("should not be valid when expired")
    void shouldNotBeValidWhenExpired() {
        ApiKey key =
                ApiKey.builder()
                        .keyHash("hash")
                        .name("test")
                        .role(Role.VIEWER)
                        .enabled(true)
                        .expiresAt(Instant.now().minus(1, ChronoUnit.HOURS))
                        .build();

        assertFalse(key.isValid());
    }

    @Test
    @DisplayName("should not be valid when disabled and expired")
    void shouldNotBeValidWhenDisabledAndExpired() {
        ApiKey key =
                ApiKey.builder()
                        .keyHash("hash")
                        .name("test")
                        .role(Role.VIEWER)
                        .enabled(false)
                        .expiresAt(Instant.now().minus(1, ChronoUnit.HOURS))
                        .build();

        assertFalse(key.isValid());
    }

    @Test
    @DisplayName("should have permission when role grants it")
    void shouldHavePermissionWhenRoleGrantsIt() {
        ApiKey key = ApiKey.builder().keyHash("hash").name("test").role(Role.ADMIN).build();

        assertTrue(key.hasPermission(Permission.WRITE_POLICIES));
        assertTrue(key.hasPermission(Permission.READ_POLICIES));
        assertTrue(key.hasPermission(Permission.MANAGE_API_KEYS));
    }

    @Test
    @DisplayName("should not have permission when role lacks it")
    void shouldNotHavePermissionWhenRoleLacksIt() {
        ApiKey key = ApiKey.builder().keyHash("hash").name("test").role(Role.VIEWER).build();

        assertFalse(key.hasPermission(Permission.WRITE_POLICIES));
        assertFalse(key.hasPermission(Permission.TRIGGER_MAINTENANCE));
        assertTrue(key.hasPermission(Permission.READ_POLICIES));
    }

    @Test
    @DisplayName("should create new key with updated lastUsedAt")
    void shouldCreateNewKeyWithUpdatedLastUsedAt() {
        ApiKey original = ApiKey.builder().keyHash("hash").name("test").role(Role.VIEWER).build();

        Instant newLastUsed = Instant.now();
        ApiKey updated = original.withLastUsedAt(newLastUsed);

        assertNotSame(original, updated);
        assertEquals(newLastUsed, updated.lastUsedAt());
        assertEquals(original.id(), updated.id());
        assertEquals(original.keyHash(), updated.keyHash());
        assertEquals(original.name(), updated.name());
    }

    @Test
    @DisplayName("should create new key with updated enabled status")
    void shouldCreateNewKeyWithUpdatedEnabled() {
        ApiKey original =
                ApiKey.builder()
                        .keyHash("hash")
                        .name("test")
                        .role(Role.VIEWER)
                        .enabled(true)
                        .build();

        ApiKey disabled = original.withEnabled(false);

        assertNotSame(original, disabled);
        assertFalse(disabled.enabled());
        assertTrue(original.enabled());
    }

    @Test
    @DisplayName("should create new key with updated role")
    void shouldCreateNewKeyWithUpdatedRole() {
        ApiKey original = ApiKey.builder().keyHash("hash").name("test").role(Role.VIEWER).build();

        ApiKey upgraded = original.withRole(Role.ADMIN);

        assertNotSame(original, upgraded);
        assertEquals(Role.ADMIN, upgraded.role());
        assertEquals(Role.VIEWER, original.role());
    }

    @Test
    @DisplayName("should create new key with updated name")
    void shouldCreateNewKeyWithUpdatedName() {
        ApiKey original =
                ApiKey.builder().keyHash("hash").name("original-name").role(Role.VIEWER).build();

        ApiKey renamed = original.withName("new-name");

        assertNotSame(original, renamed);
        assertEquals("new-name", renamed.name());
        assertEquals("original-name", original.name());
    }

    @Test
    @DisplayName("should build ApiKey with default values")
    void shouldBuildApiKeyWithDefaults() {
        ApiKey key = ApiKey.builder().keyHash("hash-123").name("my-key").build();

        assertNotNull(key.id());
        assertEquals("hash-123", key.keyHash());
        assertEquals("my-key", key.name());
        assertEquals(Role.VIEWER, key.role()); // default
        assertTrue(key.enabled()); // default
        assertNotNull(key.createdAt());
        assertNull(key.expiresAt());
        assertNull(key.lastUsedAt());
        assertNull(key.createdBy());
    }

    @Test
    @DisplayName("should build ApiKey with all fields set")
    void shouldBuildApiKeyWithAllFields() {
        Instant now = Instant.now();

        ApiKey key =
                ApiKey.builder()
                        .id("custom-id")
                        .keyHash("hash-456")
                        .name("full-key")
                        .role(Role.OPERATOR)
                        .enabled(false)
                        .createdAt(now)
                        .expiresAt(now.plus(7, ChronoUnit.DAYS))
                        .lastUsedAt(now)
                        .createdBy("admin")
                        .build();

        assertEquals("custom-id", key.id());
        assertEquals("hash-456", key.keyHash());
        assertEquals("full-key", key.name());
        assertEquals(Role.OPERATOR, key.role());
        assertFalse(key.enabled());
        assertEquals(now, key.createdAt());
        assertNotNull(key.expiresAt());
        assertEquals(now, key.lastUsedAt());
        assertEquals("admin", key.createdBy());
    }

    @Test
    @DisplayName("should throw when keyHash is null")
    void shouldThrowWhenKeyHashIsNull() {
        ApiKey.Builder builder = ApiKey.builder().name("test");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
        assertEquals("keyHash is required", ex.getMessage());
    }

    @Test
    @DisplayName("should throw when keyHash is blank")
    void shouldThrowWhenKeyHashIsBlank() {
        ApiKey.Builder builder = ApiKey.builder().keyHash("   ").name("test");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
        assertEquals("keyHash is required", ex.getMessage());
    }

    @Test
    @DisplayName("should throw when name is null")
    void shouldThrowWhenNameIsNull() {
        ApiKey.Builder builder = ApiKey.builder().keyHash("hash");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
        assertEquals("name is required", ex.getMessage());
    }

    @Test
    @DisplayName("should throw when name is blank")
    void shouldThrowWhenNameIsBlank() {
        ApiKey.Builder builder = ApiKey.builder().keyHash("hash").name("  ");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
        assertEquals("name is required", ex.getMessage());
    }

    @Test
    @DisplayName("should throw when role is null")
    void shouldThrowWhenRoleIsNull() {
        ApiKey.Builder builder = ApiKey.builder().keyHash("hash").name("test").role(null);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
        assertEquals("role is required", ex.getMessage());
    }

    @Test
    @DisplayName("should generate unique IDs for multiple builds")
    void shouldGenerateUniqueIdsForMultipleBuilds() {
        ApiKey key1 = ApiKey.builder().keyHash("hash1").name("key1").build();
        ApiKey key2 = ApiKey.builder().keyHash("hash2").name("key2").build();

        assertNotEquals(key1.id(), key2.id());
    }
}
