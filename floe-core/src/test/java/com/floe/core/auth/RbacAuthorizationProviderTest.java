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

import com.floe.core.auth.AuthorizationProvider.AuthorizationRequest;
import com.floe.core.auth.AuthorizationProvider.AuthorizationResult;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("RbacAuthorizationProvider")
class RbacAuthorizationProviderTest {

    @Nested
    @DisplayName("with ADMIN role")
    class WithAdminRole {

        private final RbacAuthorizationProvider provider =
                new RbacAuthorizationProvider(Role.ADMIN);

        @Test
        @DisplayName("should allow read:policies")
        void shouldAllowReadPolicies() {
            var request = AuthorizationRequest.of("admin-user", "read:policies", "policy", "p-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }

        @Test
        @DisplayName("should allow write:policies")
        void shouldAllowWritePolicies() {
            var request = AuthorizationRequest.of("admin-user", "write:policies", "policy", "p-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }

        @Test
        @DisplayName("should allow delete:policies")
        void shouldAllowDeletePolicies() {
            var request = AuthorizationRequest.of("admin-user", "delete:policies", "policy", "p-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }

        @Test
        @DisplayName("should allow trigger:maintenance")
        void shouldAllowTriggerMaintenance() {
            var request =
                    AuthorizationRequest.of("admin-user", "trigger:maintenance", "table", "t-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }

        @Test
        @DisplayName("should allow manage:api-keys")
        void shouldAllowManageApiKeys() {
            var request =
                    AuthorizationRequest.of("admin-user", "manage:api-keys", "api-key", "k-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }
    }

    @Nested
    @DisplayName("with OPERATOR role")
    class WithOperatorRole {

        private final RbacAuthorizationProvider provider =
                new RbacAuthorizationProvider(Role.OPERATOR);

        @Test
        @DisplayName("should allow read:policies")
        void shouldAllowReadPolicies() {
            var request = AuthorizationRequest.of("operator", "read:policies", "policy", "p-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }

        @Test
        @DisplayName("should deny write:policies")
        void shouldDenyWritePolicies() {
            var request = AuthorizationRequest.of("operator", "write:policies", "policy", "p-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isFalse();
        }

        @Test
        @DisplayName("should allow trigger:maintenance")
        void shouldAllowTriggerMaintenance() {
            var request =
                    AuthorizationRequest.of("operator", "trigger:maintenance", "table", "t-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }

        @Test
        @DisplayName("should deny manage:api-keys")
        void shouldDenyManageApiKeys() {
            var request = AuthorizationRequest.of("operator", "manage:api-keys", "api-key", "k-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isFalse();
        }
    }

    @Nested
    @DisplayName("with VIEWER role")
    class WithViewerRole {

        private final RbacAuthorizationProvider provider =
                new RbacAuthorizationProvider(Role.VIEWER);

        @Test
        @DisplayName("should allow read:policies")
        void shouldAllowReadPolicies() {
            var request = AuthorizationRequest.of("viewer", "read:policies", "policy", "p-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }

        @Test
        @DisplayName("should allow read:tables")
        void shouldAllowReadTables() {
            var request = AuthorizationRequest.of("viewer", "read:tables", "table", "t-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }

        @Test
        @DisplayName("should allow read:operations")
        void shouldAllowReadOperations() {
            var request = AuthorizationRequest.of("viewer", "read:operations", "operation", "o-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isTrue();
        }

        @Test
        @DisplayName("should deny trigger:maintenance")
        void shouldDenyTriggerMaintenance() {
            var request = AuthorizationRequest.of("viewer", "trigger:maintenance", "table", "t-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isFalse();
        }

        @Test
        @DisplayName("should deny write:policies")
        void shouldDenyWritePolicies() {
            var request = AuthorizationRequest.of("viewer", "write:policies", "policy", "p-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isFalse();
        }
    }

    @Nested
    @DisplayName("with role resolver")
    class WithRoleResolver {

        private final Map<String, Role> userRoles =
                Map.of("alice", Role.ADMIN, "bob", Role.OPERATOR, "charlie", Role.VIEWER);

        private final RbacAuthorizationProvider provider =
                new RbacAuthorizationProvider(
                        subject -> Optional.ofNullable(userRoles.get(subject)));

        @Test
        @DisplayName("should authorize based on resolved role")
        void shouldAuthorizeBasedOnResolvedRole() {
            var aliceRequest = AuthorizationRequest.of("alice", "delete:policies", "policy", "p-1");
            var bobRequest = AuthorizationRequest.of("bob", "delete:policies", "policy", "p-1");

            assertThat(provider.authorize(aliceRequest).allowed()).isTrue();
            assertThat(provider.authorize(bobRequest).allowed()).isFalse();
        }

        @Test
        @DisplayName("should deny when subject has no role")
        void shouldDenyWhenNoRole() {
            var request = AuthorizationRequest.of("unknown-user", "read:policies", "policy", "p-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isFalse();
            assertThat(result.reason()).contains("no role");
        }
    }

    @Nested
    @DisplayName("unknown actions")
    class UnknownActions {

        private final RbacAuthorizationProvider provider =
                new RbacAuthorizationProvider(Role.ADMIN);

        @Test
        @DisplayName("should deny unknown action")
        void shouldDenyUnknownAction() {
            var request = AuthorizationRequest.of("admin", "unknown:action", "resource", "r-1");
            var result = provider.authorize(request);

            assertThat(result.allowed()).isFalse();
            assertThat(result.reason()).contains("unknown action");
        }
    }

    @Nested
    @DisplayName("filterAuthorized")
    class FilterAuthorized {

        private final RbacAuthorizationProvider provider =
                new RbacAuthorizationProvider(Role.VIEWER);

        @Test
        @DisplayName("should filter resources based on authorization")
        void shouldFilterResources() {
            List<String> resources = List.of("policy-1", "policy-2", "policy-3");

            // Viewer can read policies
            List<String> authorized =
                    provider.filterAuthorized(
                            "viewer", "read:policies", resources, r -> r, "policy");

            assertThat(authorized).hasSize(3);
        }

        @Test
        @DisplayName("should return empty list when no resources authorized")
        void shouldReturnEmptyWhenNoneAuthorized() {
            List<String> resources = List.of("policy-1", "policy-2");

            // Viewer cannot delete policies
            List<String> authorized =
                    provider.filterAuthorized(
                            "viewer", "delete:policies", resources, r -> r, "policy");

            assertThat(authorized).isEmpty();
        }
    }

    @Nested
    @DisplayName("AuthorizationRequest factory methods")
    class RequestFactoryMethods {

        @Test
        @DisplayName("should create request with of()")
        void shouldCreateWithOf() {
            var request = AuthorizationRequest.of("user", "read:policies", "policy", "p-1");

            assertThat(request.subject()).isEqualTo("user");
            assertThat(request.action()).isEqualTo("read:policies");
            assertThat(request.resourceType()).isEqualTo("policy");
            assertThat(request.resourceId()).isEqualTo("p-1");
            assertThat(request.context()).isEmpty();
        }
    }

    @Nested
    @DisplayName("AuthorizationResult factory methods")
    class ResultFactoryMethods {

        @Test
        @DisplayName("should create allowed result")
        void shouldCreateAllowed() {
            var result = AuthorizationResult.allowed("has permission");

            assertThat(result.allowed()).isTrue();
            assertThat(result.reason()).isEqualTo("has permission");
        }

        @Test
        @DisplayName("should create denied result")
        void shouldCreateDenied() {
            var result = AuthorizationResult.denied("missing permission");

            assertThat(result.allowed()).isFalse();
            assertThat(result.reason()).isEqualTo("missing permission");
        }

        @Test
        @DisplayName("should have ALLOWED constant")
        void shouldHaveAllowedConstant() {
            assertThat(AuthorizationResult.ALLOWED.allowed()).isTrue();
        }

        @Test
        @DisplayName("should have DENIED constant")
        void shouldHaveDeniedConstant() {
            assertThat(AuthorizationResult.DENIED.allowed()).isFalse();
        }
    }
}
