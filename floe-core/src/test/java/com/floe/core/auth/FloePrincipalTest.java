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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("FloePrincipal")
class FloePrincipalTest {

    @Nested
    @DisplayName("Builder")
    class BuilderTests {

        @Test
        @DisplayName("should build principal with required fields")
        void shouldBuildWithRequiredFields() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("john.doe")
                            .role(Role.VIEWER)
                            .authenticationMethod("API_KEY")
                            .build();

            assertThat(principal.userId()).isEqualTo("user-123");
            assertThat(principal.username()).isEqualTo("john.doe");
            assertThat(principal.getName()).isEqualTo("john.doe");
            assertThat(principal.roles()).containsExactly(Role.VIEWER);
            assertThat(principal.authenticationMethod()).isEqualTo("API_KEY");
        }

        @Test
        @DisplayName("should throw exception when userId is null")
        void shouldThrowWhenUserIdNull() {
            assertThatThrownBy(
                            () ->
                                    FloePrincipal.builder()
                                            .username("john.doe")
                                            .role(Role.VIEWER)
                                            .authenticationMethod("API_KEY")
                                            .build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("userId");
        }

        @Test
        @DisplayName("should throw exception when username is null")
        void shouldThrowWhenUsernameNull() {
            assertThatThrownBy(
                            () ->
                                    FloePrincipal.builder()
                                            .userId("user-123")
                                            .role(Role.VIEWER)
                                            .authenticationMethod("API_KEY")
                                            .build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("username");
        }

        @Test
        @DisplayName("should build with multiple roles")
        void shouldBuildWithMultipleRoles() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("admin")
                            .roles(Set.of(Role.ADMIN, Role.OPERATOR))
                            .authenticationMethod("OIDC")
                            .build();

            assertThat(principal.roles()).containsExactlyInAnyOrder(Role.ADMIN, Role.OPERATOR);
        }

        @Test
        @DisplayName("should build with metadata")
        void shouldBuildWithMetadata() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("john.doe")
                            .role(Role.VIEWER)
                            .authenticationMethod("OIDC")
                            .metadata("email", "john.doe@example.com")
                            .metadata("issuer", "https://auth.example.com")
                            .build();

            assertThat(principal.metadata()).containsEntry("email", "john.doe@example.com");
            assertThat(principal.metadata()).containsEntry("issuer", "https://auth.example.com");
        }
    }

    @Nested
    @DisplayName("Permission Computation")
    class PermissionTests {

        @Test
        @DisplayName("should compute permissions from single ADMIN role")
        void shouldComputeAdminPermissions() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("admin-1")
                            .username("admin")
                            .role(Role.ADMIN)
                            .authenticationMethod("API_KEY")
                            .build();

            assertThat(principal.permissions()).containsAll(Role.ADMIN.permissions());
            assertThat(principal.permissions()).contains(Permission.MANAGE_API_KEYS);
            assertThat(principal.permissions()).contains(Permission.WRITE_POLICIES);
            assertThat(principal.permissions()).contains(Permission.TRIGGER_MAINTENANCE);
        }

        @Test
        @DisplayName("should compute permissions from OPERATOR role")
        void shouldComputeOperatorPermissions() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("op-1")
                            .username("operator")
                            .role(Role.OPERATOR)
                            .authenticationMethod("API_KEY")
                            .build();

            assertThat(principal.permissions())
                    .containsExactlyInAnyOrder(
                            Permission.READ_POLICIES,
                            Permission.READ_TABLES,
                            Permission.READ_OPERATIONS,
                            Permission.TRIGGER_MAINTENANCE);

            assertThat(principal.permissions()).doesNotContain(Permission.MANAGE_API_KEYS);
            assertThat(principal.permissions()).doesNotContain(Permission.WRITE_POLICIES);
        }

        @Test
        @DisplayName("should compute permissions from VIEWER role")
        void shouldComputeViewerPermissions() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("viewer-1")
                            .username("viewer")
                            .role(Role.VIEWER)
                            .authenticationMethod("OIDC")
                            .build();

            assertThat(principal.permissions())
                    .containsExactlyInAnyOrder(
                            Permission.READ_POLICIES,
                            Permission.READ_TABLES,
                            Permission.READ_OPERATIONS);

            assertThat(principal.permissions()).doesNotContain(Permission.TRIGGER_MAINTENANCE);
        }

        @Test
        @DisplayName("should union permissions from multiple roles")
        void shouldUnionPermissionsFromMultipleRoles() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("multi-role-user")
                            .roles(Set.of(Role.VIEWER, Role.OPERATOR))
                            .authenticationMethod("OIDC")
                            .build();

            // Should have all OPERATOR permissions (superset of VIEWER)
            assertThat(principal.permissions())
                    .containsExactlyInAnyOrder(
                            Permission.READ_POLICIES,
                            Permission.READ_TABLES,
                            Permission.READ_OPERATIONS,
                            Permission.TRIGGER_MAINTENANCE);
        }
    }

    @Nested
    @DisplayName("Role Checks")
    class RoleCheckTests {

        @Test
        @DisplayName("hasRole should return true for assigned role")
        void hasRoleShouldReturnTrueForAssignedRole() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("operator")
                            .role(Role.OPERATOR)
                            .authenticationMethod("API_KEY")
                            .build();

            assertThat(principal.hasRole(Role.OPERATOR)).isTrue();
            assertThat(principal.hasRole(Role.ADMIN)).isFalse();
            assertThat(principal.hasRole(Role.VIEWER)).isFalse();
        }

        @Test
        @DisplayName("hasAnyRole should return true if any role matches")
        void hasAnyRoleShouldReturnTrueIfAnyMatches() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("admin")
                            .role(Role.ADMIN)
                            .authenticationMethod("OIDC")
                            .build();

            assertThat(principal.hasAnyRole(Role.ADMIN, Role.OPERATOR)).isTrue();
            assertThat(principal.hasAnyRole(Role.OPERATOR, Role.VIEWER)).isFalse();
        }
    }

    @Nested
    @DisplayName("Permission Checks")
    class PermissionCheckTests {

        @Test
        @DisplayName("hasPermission should return true for granted permission")
        void hasPermissionShouldReturnTrueForGranted() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("operator")
                            .role(Role.OPERATOR)
                            .authenticationMethod("API_KEY")
                            .build();

            assertThat(principal.hasPermission(Permission.TRIGGER_MAINTENANCE)).isTrue();
            assertThat(principal.hasPermission(Permission.READ_POLICIES)).isTrue();
            assertThat(principal.hasPermission(Permission.MANAGE_API_KEYS)).isFalse();
        }

        @Test
        @DisplayName("hasAllPermissions should return true only if all granted")
        void hasAllPermissionsShouldRequireAll() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("viewer")
                            .role(Role.VIEWER)
                            .authenticationMethod("OIDC")
                            .build();

            assertThat(
                            principal.hasAllPermissions(
                                    Permission.READ_POLICIES, Permission.READ_TABLES))
                    .isTrue();

            assertThat(
                            principal.hasAllPermissions(
                                    Permission.READ_POLICIES, Permission.TRIGGER_MAINTENANCE))
                    .isFalse();
        }

        @Test
        @DisplayName("hasAnyPermission should return true if any granted")
        void hasAnyPermissionShouldReturnTrueIfAnyGranted() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("viewer")
                            .role(Role.VIEWER)
                            .authenticationMethod("OIDC")
                            .build();

            assertThat(
                            principal.hasAnyPermission(
                                    Permission.READ_POLICIES, Permission.MANAGE_API_KEYS))
                    .isTrue();

            assertThat(
                            principal.hasAnyPermission(
                                    Permission.WRITE_POLICIES, Permission.MANAGE_API_KEYS))
                    .isFalse();
        }
    }

    @Nested
    @DisplayName("Immutability")
    class ImmutabilityTests {

        @Test
        @DisplayName("roles should be immutable")
        void rolesShouldBeImmutable() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("john.doe")
                            .role(Role.VIEWER)
                            .authenticationMethod("API_KEY")
                            .build();

            assertThatThrownBy(() -> principal.roles().add(Role.ADMIN))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("permissions should be immutable")
        void permissionsShouldBeImmutable() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("john.doe")
                            .role(Role.VIEWER)
                            .authenticationMethod("API_KEY")
                            .build();

            assertThatThrownBy(() -> principal.permissions().add(Permission.MANAGE_API_KEYS))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("metadata should be immutable")
        void metadataShouldBeImmutable() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-123")
                            .username("john.doe")
                            .role(Role.VIEWER)
                            .authenticationMethod("API_KEY")
                            .metadata("email", "test@example.com")
                            .build();

            assertThatThrownBy(() -> principal.metadata().put("extra", "value"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
