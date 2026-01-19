package com.floe.server.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.floe.core.auth.AuthenticationRequest;
import com.floe.core.auth.FloePrincipal;
import com.floe.core.auth.Role;
import com.floe.server.config.FloeConfig;
import io.quarkus.security.identity.SecurityIdentity;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@DisplayName("OidcIdentityProvider")
class OidcIdentityProviderTest {

    @Mock private SecurityIdentity securityIdentity;

    @Mock private FloeConfig config;

    @Mock private FloeConfig.Auth auth;

    @Mock private AuthenticationRequest request;

    @Mock private JsonWebToken jwt;

    private OidcIdentityProvider provider;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        when(config.auth()).thenReturn(auth);
        when(auth.roleMapping()).thenReturn(Map.of());

        provider = new OidcIdentityProvider();
        provider.securityIdentity = securityIdentity;
        provider.config = config;
    }

    @Nested
    @DisplayName("supports")
    class Supports {

        @Test
        @DisplayName("should return true when bearer token is present")
        void shouldReturnTrueWhenBearerTokenPresent() {
            when(request.getBearerToken()).thenReturn(Optional.of("jwt-token"));
            assertThat(provider.supports(request)).isTrue();
        }

        @Test
        @DisplayName("should return false when bearer token is missing")
        void shouldReturnFalseWhenBearerTokenMissing() {
            when(request.getBearerToken()).thenReturn(Optional.empty());
            assertThat(provider.supports(request)).isFalse();
        }
    }

    @Nested
    @DisplayName("authenticate")
    class Authenticate {

        @BeforeEach
        void setUp() {
            when(request.getBearerToken()).thenReturn(Optional.of("jwt-token"));
            when(securityIdentity.isAnonymous()).thenReturn(false);
            when(securityIdentity.getPrincipal()).thenReturn(jwt);
            when(jwt.getSubject()).thenReturn("user-123");
        }

        @Test
        @DisplayName("should return empty when no bearer token")
        void shouldReturnEmptyWhenNoBearerToken() {
            when(request.getBearerToken()).thenReturn(Optional.empty());

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("should return empty when security identity is anonymous")
        void shouldReturnEmptyWhenAnonymous() {
            when(securityIdentity.isAnonymous()).thenReturn(true);

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("should return empty when principal is not JWT")
        void shouldReturnEmptyWhenNotJwt() {
            when(securityIdentity.getPrincipal()).thenReturn(() -> "not-a-jwt");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("should authenticate with admin role from roles claim")
        void shouldAuthenticateWithAdminRole() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("admin"));
            when(jwt.getClaim("preferred_username")).thenReturn("john.doe");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            FloePrincipal principal = result.get();
            assertThat(principal.userId()).isEqualTo("user-123");
            assertThat(principal.username()).isEqualTo("john.doe");
            assertThat(principal.roles()).containsExactly(Role.ADMIN);
            assertThat(principal.authenticationMethod()).isEqualTo("OIDC");
        }

        @Test
        @DisplayName("should authenticate with operator role")
        void shouldAuthenticateWithOperatorRole() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("operator"));
            when(jwt.getClaim("preferred_username")).thenReturn("jane.doe");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.OPERATOR);
        }

        @Test
        @DisplayName("should authenticate with viewer role")
        void shouldAuthenticateWithViewerRole() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("viewer"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.VIEWER);
        }

        @Test
        @DisplayName("should return empty when no roles found")
        void shouldReturnEmptyWhenNoRoles() {
            when(jwt.getClaim("roles")).thenReturn(null);
            when(jwt.getClaim("realm_access")).thenReturn(null);
            when(jwt.getClaim("permissions")).thenReturn(null);
            when(jwt.getClaim("groups")).thenReturn(null);
            when(jwt.getClaim("scope")).thenReturn(null);

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("should return empty when roles don't map to Floe roles")
        void shouldReturnEmptyWhenNoMappableRoles() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("unknown-role", "another-role"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("should support multiple roles")
        void shouldSupportMultipleRoles() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("admin", "operator"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactlyInAnyOrder(Role.ADMIN, Role.OPERATOR);
        }
    }

    @Nested
    @DisplayName("role mapping")
    class RoleMapping {

        @BeforeEach
        void setUp() {
            when(request.getBearerToken()).thenReturn(Optional.of("jwt-token"));
            when(securityIdentity.isAnonymous()).thenReturn(false);
            when(securityIdentity.getPrincipal()).thenReturn(jwt);
            when(jwt.getSubject()).thenReturn("user-123");
        }

        @Test
        @DisplayName("should map floe-admin to ADMIN")
        void shouldMapFloeAdmin() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("floe-admin"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.ADMIN);
        }

        @Test
        @DisplayName("should map administrator to ADMIN")
        void shouldMapAdministrator() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("administrator"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.ADMIN);
        }

        @Test
        @DisplayName("should map superuser to ADMIN")
        void shouldMapSuperuser() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("superuser"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.ADMIN);
        }

        @Test
        @DisplayName("should map floe-operator to OPERATOR")
        void shouldMapFloeOperator() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("floe-operator"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.OPERATOR);
        }

        @Test
        @DisplayName("should map maintainer to OPERATOR")
        void shouldMapMaintainer() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("maintainer"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.OPERATOR);
        }

        @Test
        @DisplayName("should map floe-viewer to VIEWER")
        void shouldMapFloeViewer() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("floe-viewer"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.VIEWER);
        }

        @Test
        @DisplayName("should map readonly to VIEWER")
        void shouldMapReadonly() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("readonly"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.VIEWER);
        }

        @Test
        @DisplayName("should support custom role mapping from config")
        void shouldSupportCustomRoleMapping() {
            when(auth.roleMapping()).thenReturn(Map.of("data-engineer", "OPERATOR"));
            when(jwt.getClaim("roles")).thenReturn(Set.of("data-engineer"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.OPERATOR);
        }

        @Test
        @DisplayName("should be case-insensitive for built-in mappings")
        void shouldBeCaseInsensitive() {
            when(jwt.getClaim("roles")).thenReturn(Set.of("ADMIN", "Operator", "vIeWeR"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles())
                    .containsExactlyInAnyOrder(Role.ADMIN, Role.OPERATOR, Role.VIEWER);
        }
    }

    @Nested
    @DisplayName("claim extraction")
    class ClaimExtraction {

        @BeforeEach
        void setUp() {
            when(request.getBearerToken()).thenReturn(Optional.of("jwt-token"));
            when(securityIdentity.isAnonymous()).thenReturn(false);
            when(securityIdentity.getPrincipal()).thenReturn(jwt);
            when(jwt.getSubject()).thenReturn("user-123");
            when(jwt.getClaim("roles")).thenReturn(Set.of("admin"));
        }

        @Test
        @DisplayName("should extract roles from Keycloak realm_access")
        void shouldExtractFromRealmAccess() {
            when(jwt.getClaim("roles")).thenReturn(null);
            when(jwt.getClaim("realm_access")).thenReturn(Map.of("roles", List.of("admin")));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.ADMIN);
        }

        @Test
        @DisplayName("should extract roles from Auth0 permissions")
        void shouldExtractFromPermissions() {
            when(jwt.getClaim("roles")).thenReturn(null);
            when(jwt.getClaim("permissions")).thenReturn(Set.of("operator"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.OPERATOR);
        }

        @Test
        @DisplayName("should extract roles from Azure AD groups")
        void shouldExtractFromGroups() {
            when(jwt.getClaim("roles")).thenReturn(null);
            when(jwt.getClaim("groups")).thenReturn(Set.of("viewer"));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.VIEWER);
        }

        @Test
        @DisplayName("should extract roles from OAuth2 scope")
        void shouldExtractFromScope() {
            when(jwt.getClaim("roles")).thenReturn(null);
            when(jwt.getClaim("scope")).thenReturn("openid profile admin");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().roles()).containsExactly(Role.ADMIN);
        }

        @Test
        @DisplayName("should prefer preferred_username for username")
        void shouldPreferPreferredUsername() {
            when(jwt.getClaim("preferred_username")).thenReturn("john.doe");
            when(jwt.getClaim("name")).thenReturn("John Doe");
            when(jwt.getClaim("email")).thenReturn("john@example.com");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().username()).isEqualTo("john.doe");
        }

        @Test
        @DisplayName("should fallback to name for username")
        void shouldFallbackToName() {
            when(jwt.getClaim("preferred_username")).thenReturn(null);
            when(jwt.getClaim("name")).thenReturn("John Doe");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().username()).isEqualTo("John Doe");
        }

        @Test
        @DisplayName("should fallback to email for username")
        void shouldFallbackToEmail() {
            when(jwt.getClaim("preferred_username")).thenReturn(null);
            when(jwt.getClaim("name")).thenReturn(null);
            when(jwt.getClaim("email")).thenReturn("john@example.com");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().username()).isEqualTo("john@example.com");
        }

        @Test
        @DisplayName("should fallback to subject for username")
        void shouldFallbackToSubject() {
            when(jwt.getClaim("preferred_username")).thenReturn(null);
            when(jwt.getClaim("name")).thenReturn(null);
            when(jwt.getClaim("email")).thenReturn(null);

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().username()).isEqualTo("user-123");
        }
    }

    @Nested
    @DisplayName("metadata")
    class Metadata {

        @BeforeEach
        void setUp() {
            when(request.getBearerToken()).thenReturn(Optional.of("jwt-token"));
            when(securityIdentity.isAnonymous()).thenReturn(false);
            when(securityIdentity.getPrincipal()).thenReturn(jwt);
            when(jwt.getSubject()).thenReturn("user-123");
            when(jwt.getClaim("roles")).thenReturn(Set.of("admin"));
        }

        @Test
        @DisplayName("should include email in metadata")
        void shouldIncludeEmail() {
            when(jwt.getClaim("email")).thenReturn("john@example.com");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().metadata()).containsEntry("email", "john@example.com");
        }

        @Test
        @DisplayName("should include issuer in metadata")
        void shouldIncludeIssuer() {
            when(jwt.getIssuer()).thenReturn("https://auth.example.com");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().metadata()).containsEntry("issuer", "https://auth.example.com");
        }

        @Test
        @DisplayName("should include timestamps in metadata")
        void shouldIncludeTimestamps() {
            when(jwt.getIssuedAtTime()).thenReturn(1000L);
            when(jwt.getExpirationTime()).thenReturn(2000L);

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().metadata()).containsEntry("issuedAt", 1000L);
            assertThat(result.get().metadata()).containsEntry("expiresAt", 2000L);
        }

        @Test
        @DisplayName("should include name claims in metadata")
        void shouldIncludeNameClaims() {
            when(jwt.getClaim("given_name")).thenReturn("John");
            when(jwt.getClaim("family_name")).thenReturn("Doe");

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            assertThat(result.get().metadata()).containsEntry("givenName", "John");
            assertThat(result.get().metadata()).containsEntry("familyName", "Doe");
        }
    }

    @Nested
    @DisplayName("properties")
    class Properties {

        @Test
        @DisplayName("should return Bearer scheme")
        void shouldReturnBearerScheme() {
            assertThat(provider.getScheme()).isEqualTo("Bearer");
        }

        @Test
        @DisplayName("should have priority 100")
        void shouldHavePriority100() {
            assertThat(provider.priority()).isEqualTo(100);
        }
    }
}
