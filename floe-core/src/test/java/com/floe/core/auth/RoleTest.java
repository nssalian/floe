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

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Role")
class RoleTest {

    @Nested
    @DisplayName("ADMIN Role")
    class AdminRole {

        @Test
        @DisplayName("should have all permissions")
        void adminShouldHaveAllPermissions() {
            Role admin = Role.ADMIN;

            for (Permission permission : Permission.values()) {
                assertTrue(
                        admin.hasPermission(permission),
                        "ADMIN should have permission: " + permission);
            }
        }

        @Test
        @DisplayName("should have correct description")
        void adminShouldHaveDescription() {
            assertEquals("Administrator with full system access", Role.ADMIN.description());
        }

        @Test
        @DisplayName("permissions should contain all permissions")
        void adminPermissionsShouldContainAllPermissions() {
            Set<Permission> permissions = Role.ADMIN.permissions();

            assertEquals(Permission.values().length, permissions.size());
            for (Permission permission : Permission.values()) {
                assertTrue(permissions.contains(permission));
            }
        }
    }

    @Nested
    @DisplayName("OPERATOR Role")
    class OperatorRole {

        @Test
        @DisplayName("should have READ_POLICIES permission")
        void operatorShouldHaveReadPoliciesPermission() {
            assertTrue(Role.OPERATOR.hasPermission(Permission.READ_POLICIES));
        }

        @Test
        @DisplayName("should have READ_TABLES permission")
        void operatorShouldHaveReadTablesPermission() {
            assertTrue(Role.OPERATOR.hasPermission(Permission.READ_TABLES));
        }

        @Test
        @DisplayName("should have READ_OPERATIONS permission")
        void operatorShouldHaveReadOperationsPermission() {
            assertTrue(Role.OPERATOR.hasPermission(Permission.READ_OPERATIONS));
        }

        @Test
        @DisplayName("should have TRIGGER_MAINTENANCE permission")
        void operatorShouldHaveTriggerMaintenancePermission() {
            assertTrue(Role.OPERATOR.hasPermission(Permission.TRIGGER_MAINTENANCE));
        }

        @Test
        @DisplayName("should not have WRITE_POLICIES permission")
        void operatorShouldNotHaveWritePoliciesPermission() {
            assertFalse(Role.OPERATOR.hasPermission(Permission.WRITE_POLICIES));
        }

        @Test
        @DisplayName("should not have MANAGE_API_KEYS permission")
        void operatorShouldNotHaveManageApiKeysPermission() {
            assertFalse(Role.OPERATOR.hasPermission(Permission.MANAGE_API_KEYS));
        }

        @Test
        @DisplayName("should have correct description")
        void operatorShouldHaveDescription() {
            assertEquals(
                    "Operator with ability to trigger maintenance and view all data",
                    Role.OPERATOR.description());
        }

        @Test
        @DisplayName("should have four permissions")
        void operatorPermissionsShouldBeFour() {
            assertEquals(4, Role.OPERATOR.permissions().size());
        }
    }

    @Nested
    @DisplayName("VIEWER Role")
    class ViewerRole {

        @Test
        @DisplayName("should have READ_POLICIES permission")
        void viewerShouldHaveReadPoliciesPermission() {
            assertTrue(Role.VIEWER.hasPermission(Permission.READ_POLICIES));
        }

        @Test
        @DisplayName("should have READ_TABLES permission")
        void viewerShouldHaveReadTablesPermission() {
            assertTrue(Role.VIEWER.hasPermission(Permission.READ_TABLES));
        }

        @Test
        @DisplayName("should have READ_OPERATIONS permission")
        void viewerShouldHaveReadOperationsPermission() {
            assertTrue(Role.VIEWER.hasPermission(Permission.READ_OPERATIONS));
        }

        @Test
        @DisplayName("should not have TRIGGER_MAINTENANCE permission")
        void viewerShouldNotHaveTriggerMaintenancePermission() {
            assertFalse(Role.VIEWER.hasPermission(Permission.TRIGGER_MAINTENANCE));
        }

        @Test
        @DisplayName("should not have WRITE_POLICIES permission")
        void viewerShouldNotHaveWritePoliciesPermission() {
            assertFalse(Role.VIEWER.hasPermission(Permission.WRITE_POLICIES));
        }

        @Test
        @DisplayName("should not have MANAGE_API_KEYS permission")
        void viewerShouldNotHaveManageApiKeysPermission() {
            assertFalse(Role.VIEWER.hasPermission(Permission.MANAGE_API_KEYS));
        }

        @Test
        @DisplayName("should have correct description")
        void viewerShouldHaveDescription() {
            assertEquals(
                    "Viewer with read-only access to policies, tables, and operations",
                    Role.VIEWER.description());
        }

        @Test
        @DisplayName("should have three permissions")
        void viewerPermissionsShouldBeThree() {
            assertEquals(3, Role.VIEWER.permissions().size());
        }
    }

    @Nested
    @DisplayName("Immutability")
    class Immutability {

        @Test
        @DisplayName("permissions should be immutable")
        void permissionsShouldBeImmutable() {
            Set<Permission> permissions = Role.VIEWER.permissions();

            assertThrows(
                    UnsupportedOperationException.class,
                    () -> {
                        permissions.add(Permission.WRITE_POLICIES);
                    });
        }
    }

    @Nested
    @DisplayName("Enum Properties")
    class EnumProperties {

        @Test
        @DisplayName("should have three roles")
        void shouldHaveThreeRoles() {
            assertEquals(3, Role.values().length);
        }

        @Test
        @DisplayName("should parse role from string")
        void shouldParseRoleFromString() {
            assertEquals(Role.ADMIN, Role.valueOf("ADMIN"));
            assertEquals(Role.OPERATOR, Role.valueOf("OPERATOR"));
            assertEquals(Role.VIEWER, Role.valueOf("VIEWER"));
        }
    }
}
