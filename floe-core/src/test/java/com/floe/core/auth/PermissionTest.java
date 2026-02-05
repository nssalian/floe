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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Permission")
class PermissionTest {

    @Nested
    @DisplayName("Descriptions")
    class Descriptions {

        @Test
        @DisplayName("READ_POLICIES should have correct description")
        void readPoliciesShouldHaveDescription() {
            assertEquals("Read maintenance policies", Permission.READ_POLICIES.description());
        }

        @Test
        @DisplayName("WRITE_POLICIES should have correct description")
        void writePoliciesShouldHaveDescription() {
            assertEquals(
                    "Create and update maintenance policies",
                    Permission.WRITE_POLICIES.description());
        }

        @Test
        @DisplayName("DELETE_POLICIES should have correct description")
        void deletePoliciesShouldHaveDescription() {
            assertEquals("Delete maintenance policies", Permission.DELETE_POLICIES.description());
        }

        @Test
        @DisplayName("READ_TABLES should have correct description")
        void readTablesShouldHaveDescription() {
            assertEquals("View tables and their metadata", Permission.READ_TABLES.description());
        }

        @Test
        @DisplayName("READ_OPERATIONS should have correct description")
        void readOperationsShouldHaveDescription() {
            assertEquals(
                    "View maintenance operation history", Permission.READ_OPERATIONS.description());
        }

        @Test
        @DisplayName("TRIGGER_MAINTENANCE should have correct description")
        void triggerMaintenanceShouldHaveDescription() {
            assertEquals(
                    "Trigger maintenance operations", Permission.TRIGGER_MAINTENANCE.description());
        }

        @Test
        @DisplayName("MANAGE_API_KEYS should have correct description")
        void manageApiKeysShouldHaveDescription() {
            assertEquals(
                    "Create, update, and revoke API keys",
                    Permission.MANAGE_API_KEYS.description());
        }
    }

    @Nested
    @DisplayName("Enum Properties")
    class EnumProperties {

        @Test
        @DisplayName("should have seven permissions")
        void shouldHaveSevenPermissions() {
            assertEquals(7, Permission.values().length);
        }

        @Test
        @DisplayName("should parse from string")
        void shouldParseFromString() {
            assertEquals(Permission.READ_POLICIES, Permission.valueOf("READ_POLICIES"));
            assertEquals(Permission.WRITE_POLICIES, Permission.valueOf("WRITE_POLICIES"));
            assertEquals(Permission.DELETE_POLICIES, Permission.valueOf("DELETE_POLICIES"));
            assertEquals(Permission.READ_TABLES, Permission.valueOf("READ_TABLES"));
            assertEquals(Permission.READ_OPERATIONS, Permission.valueOf("READ_OPERATIONS"));
            assertEquals(Permission.TRIGGER_MAINTENANCE, Permission.valueOf("TRIGGER_MAINTENANCE"));
            assertEquals(Permission.MANAGE_API_KEYS, Permission.valueOf("MANAGE_API_KEYS"));
        }

        @Test
        @DisplayName("all permissions should have non-empty descriptions")
        void allPermissionsShouldHaveNonEmptyDescriptions() {
            for (Permission permission : Permission.values()) {
                assertNotNull(permission.description());
                assertFalse(
                        permission.description().isEmpty(),
                        permission.name() + " should have non-empty description");
            }
        }

        @Test
        @DisplayName("should throw for invalid permission")
        void shouldThrowForInvalidPermission() {
            assertThrows(IllegalArgumentException.class, () -> Permission.valueOf("INVALID"));
        }
    }
}
