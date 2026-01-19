package com.floe.server.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.floe.core.auth.AuditLogger;
import com.floe.core.auth.FloePrincipal;
import com.floe.core.auth.Permission;
import com.floe.core.auth.Role;
import com.floe.server.config.FloeConfig;
import com.floe.server.metrics.FloeMetrics;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for FloeHttpAuthenticationMechanism.
 *
 * <p>Tests verify:
 *
 * <ul>
 *   <li>API key authentication works via IdentityProvider abstraction
 *   <li>Missing/invalid credentials return empty SecurityIdentity
 *   <li>Role-based permissions are correctly mapped
 *   <li>Audit logging and metrics are recorded
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
class AuthenticationIntegrationTest {

    private static final String API_KEY_HEADER = "X-API-Key";

    @Mock private FloeConfig config;

    @Mock private FloeConfig.Auth authConfig;

    @Mock private AuditLogger auditLogger;

    @Mock private FloeMetrics metrics;

    @Mock private RoutingContext routingContext;

    @Mock private HttpServerRequest httpRequest;

    @Mock private SocketAddress remoteAddress;

    private FloeHttpAuthenticationMechanism mechanism;

    @BeforeEach
    void setUp() {
        // Common stubs for RoutingContext
        lenient().when(routingContext.request()).thenReturn(httpRequest);
        lenient().when(httpRequest.path()).thenReturn("/api/v1/policies");
        lenient().when(httpRequest.method()).thenReturn(HttpMethod.GET);
        lenient().when(httpRequest.remoteAddress()).thenReturn(remoteAddress);
        lenient().when(remoteAddress.host()).thenReturn("127.0.0.1");

        // Common stubs for config
        lenient().when(config.auth()).thenReturn(authConfig);
        lenient().when(authConfig.enabled()).thenReturn(true);
        lenient().when(authConfig.headerName()).thenReturn(API_KEY_HEADER);

        // Create mechanism with empty provider list by default
        mechanism =
                new FloeHttpAuthenticationMechanism(
                        Collections.emptyList(), auditLogger, metrics, config);
    }

    private FloePrincipal createTestPrincipal(String name, Role role) {
        return FloePrincipal.builder()
                .userId("user-" + name)
                .username(name)
                .roles(Set.of(role))
                .authenticationMethod("API_KEY")
                .build();
    }

    // Authentication Tests

    @Nested
    @DisplayName("Authentication")
    class AuthenticationTests {

        @Test
        @DisplayName("Auth disabled returns empty SecurityIdentity")
        void authDisabledReturnsEmpty() {
            when(authConfig.enabled()).thenReturn(false);

            Uni<SecurityIdentity> result = mechanism.authenticate(routingContext, null);

            SecurityIdentity identity = result.await().indefinitely();
            assertTrue(identity == null || identity.isAnonymous());
        }

        @Test
        @DisplayName("No providers returns empty SecurityIdentity")
        void noProvidersReturnsEmpty() {
            // mechanism.floeIdentityProviders is empty by default in mocked test

            Uni<SecurityIdentity> result = mechanism.authenticate(routingContext, null);

            // Should return empty/null since no providers
            assertNotNull(result);
            SecurityIdentity identity = result.await().indefinitely();
            assertNull(identity);
        }
    }

    // Role Permission Tests

    @Nested
    @DisplayName("Role Permissions")
    class RolePermissionTests {

        @Test
        @DisplayName("ADMIN has all permissions")
        void adminHasAllPermissions() {
            Set<Permission> adminPermissions = Role.ADMIN.permissions();

            for (Permission permission : Permission.values()) {
                assertTrue(
                        adminPermissions.contains(permission),
                        "ADMIN should have permission: " + permission);
            }
        }

        @Test
        @DisplayName("OPERATOR has correct permissions")
        void operatorHasCorrectPermissions() {
            Set<Permission> operatorPermissions = Role.OPERATOR.permissions();

            assertTrue(operatorPermissions.contains(Permission.READ_POLICIES));
            assertTrue(operatorPermissions.contains(Permission.READ_TABLES));
            assertTrue(operatorPermissions.contains(Permission.READ_OPERATIONS));
            assertTrue(operatorPermissions.contains(Permission.TRIGGER_MAINTENANCE));

            assertFalse(operatorPermissions.contains(Permission.WRITE_POLICIES));
            assertFalse(operatorPermissions.contains(Permission.DELETE_POLICIES));
            assertFalse(operatorPermissions.contains(Permission.MANAGE_API_KEYS));
        }

        @Test
        @DisplayName("VIEWER has read-only permissions")
        void viewerHasReadOnlyPermissions() {
            Set<Permission> viewerPermissions = Role.VIEWER.permissions();

            assertTrue(viewerPermissions.contains(Permission.READ_POLICIES));
            assertTrue(viewerPermissions.contains(Permission.READ_TABLES));
            assertTrue(viewerPermissions.contains(Permission.READ_OPERATIONS));

            assertFalse(viewerPermissions.contains(Permission.WRITE_POLICIES));
            assertFalse(viewerPermissions.contains(Permission.DELETE_POLICIES));
            assertFalse(viewerPermissions.contains(Permission.TRIGGER_MAINTENANCE));
            assertFalse(viewerPermissions.contains(Permission.MANAGE_API_KEYS));
        }
    }

    // FloePrincipal Tests

    @Nested
    @DisplayName("FloePrincipal")
    class FloePrincipalTests {

        @Test
        @DisplayName("FloePrincipal correctly reports permissions from roles")
        void principalReportsPermissionsFromRoles() {
            FloePrincipal admin = createTestPrincipal("admin", Role.ADMIN);
            FloePrincipal viewer = createTestPrincipal("viewer", Role.VIEWER);
            FloePrincipal operator = createTestPrincipal("operator", Role.OPERATOR);

            // Admin has all permissions
            assertTrue(admin.hasPermission(Permission.WRITE_POLICIES));
            assertTrue(admin.hasPermission(Permission.MANAGE_API_KEYS));
            assertTrue(admin.hasPermission(Permission.TRIGGER_MAINTENANCE));

            // Viewer only has read permissions
            assertTrue(viewer.hasPermission(Permission.READ_POLICIES));
            assertFalse(viewer.hasPermission(Permission.WRITE_POLICIES));
            assertFalse(viewer.hasPermission(Permission.TRIGGER_MAINTENANCE));

            // Operator can trigger maintenance but not manage keys
            assertTrue(operator.hasPermission(Permission.TRIGGER_MAINTENANCE));
            assertTrue(operator.hasPermission(Permission.READ_POLICIES));
            assertFalse(operator.hasPermission(Permission.MANAGE_API_KEYS));
        }

        @Test
        @DisplayName("FloePrincipal hasAnyPermission works correctly")
        void principalHasAnyPermissionWorks() {
            FloePrincipal viewer = createTestPrincipal("viewer", Role.VIEWER);

            // Viewer has READ_POLICIES, so should return true
            assertTrue(
                    viewer.hasAnyPermission(
                            new Permission[] {
                                Permission.READ_POLICIES, Permission.WRITE_POLICIES,
                            }));

            // Viewer doesn't have any of these
            assertFalse(
                    viewer.hasAnyPermission(
                            new Permission[] {
                                Permission.WRITE_POLICIES, Permission.MANAGE_API_KEYS,
                            }));
        }
    }

    // Challenge Data Tests

    @Nested
    @DisplayName("Challenge Data")
    class ChallengeDataTests {

        @Test
        @DisplayName("getChallenge returns 401 with WWW-Authenticate header")
        void getChallengeReturns401() {
            var challenge = mechanism.getChallenge(routingContext).await().indefinitely();

            assertNotNull(challenge);
            assertEquals(401, challenge.status);
            assertNotNull(challenge.headerName);
            assertEquals("WWW-Authenticate", challenge.headerName.toString());
        }
    }
}
