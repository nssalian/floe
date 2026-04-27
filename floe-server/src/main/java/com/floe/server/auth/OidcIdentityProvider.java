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

package com.floe.server.auth;

import com.floe.core.auth.AuthenticationRequest;
import com.floe.core.auth.FloePrincipal;
import com.floe.core.auth.IdentityProvider;
import com.floe.core.auth.Role;
import com.floe.server.config.FloeConfig;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OIDC/OAuth 2.0 identity provider using Quarkus OIDC. Supports any OIDC-compliant provider
 * (Keycloak, Auth0, Okta, Azure AD, AWS Cognito, etc.)
 *
 * <p>Features: - JWT token validation (signature, expiration, issuer) - Dynamic role mapping from
 * JWT claims - Flexible claim extraction (realm_access, resource_access, permissions)
 */
@ApplicationScoped
public class OidcIdentityProvider implements IdentityProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OidcIdentityProvider.class);

    @Inject SecurityIdentity securityIdentity;

    @Inject FloeConfig config;

    @Override
    public Optional<FloePrincipal> authenticate(AuthenticationRequest request) {
        // Check if request has bearer token
        if (request.getBearerToken().isEmpty()) {
            return Optional.empty();
        }

        // Quarkus OIDC will have already validated the JWT
        // We just need to extract the principal and map roles
        if (securityIdentity.isAnonymous()) {
            LOG.debug("Security identity is anonymous, JWT validation failed");
            return Optional.empty();
        }

        try {
            // Get the JWT from the security identity
            if (!(securityIdentity.getPrincipal() instanceof JsonWebToken jwt)) {
                LOG.warn(
                        "Principal is not a JsonWebToken: "
                                + securityIdentity.getPrincipal().getClass());
                return Optional.empty();
            }

            // Extract user information
            String userId = jwt.getSubject(); // "sub" claim
            String username = extractUsername(jwt);

            // Map JWT roles to Floe roles
            Set<Role> roles = mapJwtRolesToFloeRoles(jwt);

            if (roles.isEmpty()) {
                LOG.warn("No valid roles found for user " + username + " (" + userId + ")");
                // Decide: fail auth or assign default role?
                // For now, fail - users must have at least one role
                return Optional.empty();
            }

            // Build the principal
            FloePrincipal.Builder builder =
                    FloePrincipal.builder()
                            .userId(userId)
                            .username(username)
                            .roles(roles)
                            .authenticationMethod("OIDC");

            // Add useful metadata from JWT claims
            addMetadata(builder, jwt);

            FloePrincipal principal = builder.build();
            LOG.debug(
                    "Successfully authenticated OIDC user: "
                            + username
                            + " ("
                            + userId
                            + ") with roles: "
                            + roles);

            return Optional.of(principal);
        } catch (Exception e) {
            LOG.error("Error authenticating OIDC user", e);
            return Optional.empty();
        }
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        return request.getBearerToken().isPresent();
    }

    @Override
    public String getScheme() {
        return "Bearer";
    }

    @Override
    public int priority() {
        return 100; // Check OIDC before API keys
    }

    /**
     * Extract username from JWT. Tries multiple standard claims in order of preference.
     *
     * @param jwt JsonWebToken
     * @return Username
     */
    private String extractUsername(JsonWebToken jwt) {
        // Try standard claims in order of preference
        String username = jwt.getClaim("preferred_username");
        if (username != null && !username.isEmpty()) {
            return username;
        }

        username = jwt.getClaim("name");
        if (username != null && !username.isEmpty()) {
            return username;
        }

        username = jwt.getClaim("email");
        if (username != null && !username.isEmpty()) {
            return username;
        }

        // Fallback to subject
        return jwt.getSubject();
    }

    /**
     * Map JWT roles to Floe internal roles. Supports multiple claim locations: - realm_access.roles
     * (Keycloak) - roles (standard) - resource_access.<client-id>.roles (Keycloak client roles) -
     * permissions (Auth0) - groups (Azure AD)
     *
     * @param jwt JsonWebToken
     * @return Set of Floe roles
     */
    private Set<Role> mapJwtRolesToFloeRoles(JsonWebToken jwt) {
        Set<String> jwtRoles = extractRolesFromJwt(jwt);

        if (jwtRoles.isEmpty()) {
            LOG.debug("No roles found in JWT for user " + jwt.getSubject());
            return Set.of();
        }

        // Map external role names to internal Role enum
        Set<Role> floeRoles =
                jwtRoles.stream()
                        .map(this::mapRoleName)
                        .flatMap(Optional::stream)
                        .collect(Collectors.toSet());

        if (floeRoles.isEmpty()) {
            LOG.warn(
                    "JWT contains roles "
                            + jwtRoles
                            + " but none map to Floe roles for user "
                            + jwt.getSubject());
        }

        return floeRoles;
    }

    /**
     * Extract roles from JWT. Tries multiple standard locations.
     *
     * @param jwt JsonWebToken
     * @return Set of role names
     */
    private Set<String> extractRolesFromJwt(JsonWebToken jwt) {
        Set<String> roles = new HashSet<>();

        // Try standard "roles" claim first
        try {
            Set<String> roleClaim = jwt.getClaim("roles");
            if (roleClaim != null && !roleClaim.isEmpty()) {
                roles.addAll(roleClaim);
            }
        } catch (Exception e) {
            // Claim might not exist or be wrong type
        }

        // Try Keycloak realm_access.roles
        try {
            Map<String, Object> realmAccess = jwt.getClaim("realm_access");
            if (realmAccess != null) {
                Object rolesObj = realmAccess.get("roles");
                if (rolesObj instanceof Collection) {
                    ((Collection<?>) rolesObj).forEach(r -> roles.add(String.valueOf(r)));
                }
            }
        } catch (Exception e) {
            // Claim might not exist
        }

        // Try Auth0 permissions
        try {
            Set<String> permissions = jwt.getClaim("permissions");
            if (permissions != null && !permissions.isEmpty()) {
                roles.addAll(permissions);
            }
        } catch (Exception e) {
            // Claim might not exist
        }

        // Try Azure AD groups
        try {
            Set<String> groups = jwt.getClaim("groups");
            if (groups != null && !groups.isEmpty()) {
                roles.addAll(groups);
            }
        } catch (Exception e) {
            // Claim might not exist
        }

        // Try scope (OAuth2 scopes can represent roles)
        try {
            String scope = jwt.getClaim("scope");
            if (scope != null && !scope.isEmpty()) {
                // Scopes are space-separated
                Collections.addAll(roles, scope.split(" "));
            }
        } catch (Exception e) {
            // Claim might not exist
        }

        return roles;
    }

    /**
     * Map external role name to Floe internal role
     *
     * @param externalRole External role name from identity provider
     * @return Optional Floe role
     */
    private Optional<Role> mapRoleName(String externalRole) {
        if (externalRole == null || externalRole.isEmpty()) {
            return Optional.empty();
        }

        // Built-in role mappings (case-insensitive)
        Optional<Role> builtInRole = getRole(externalRole);

        if (builtInRole.isPresent()) {
            return builtInRole;
        }

        // Check custom role mappings from config
        // Format: floe.auth.role-mapping.my-custom-admin=ADMIN
        Map<String, String> customMappings = config.auth().roleMapping();
        if (customMappings != null && !customMappings.isEmpty()) {
            String mappedRole = customMappings.get(externalRole);
            if (mappedRole != null) {
                try {
                    return Optional.of(Role.valueOf(mappedRole.toUpperCase(Locale.ROOT)));
                } catch (IllegalArgumentException e) {
                    LOG.warn("Invalid role mapping: " + externalRole + " -> " + mappedRole);
                }
            }
        }

        return Optional.empty();
    }

    private static Optional<Role> getRole(String externalRole) {
        String roleLower = externalRole.toLowerCase(Locale.ROOT);
        return switch (roleLower) {
            case "floe-admin", "admin", "administrator", "superuser" -> Optional.of(Role.ADMIN);
            case "floe-operator", "operator", "maintainer", "editor" -> Optional.of(Role.OPERATOR);
            case "floe-viewer", "viewer", "readonly", "read-only", "reader" ->
                    Optional.of(Role.VIEWER);
            default -> Optional.empty();
        };
    }

    /**
     * Add useful metadata from JWT claims to principal
     *
     * @param builder FloePrincipal builder
     * @param jwt JsonWebToken
     */
    private void addMetadata(FloePrincipal.Builder builder, JsonWebToken jwt) {
        // Add email if present
        String email = jwt.getClaim("email");
        if (email != null && !email.isEmpty()) {
            builder.metadata("email", email);
        }

        // Add issuer
        String issuer = jwt.getIssuer();
        if (issuer != null) {
            builder.metadata("issuer", issuer);
        }

        // Add issued at and expiration
        Long iat = jwt.getIssuedAtTime();
        if (iat != null) {
            builder.metadata("issuedAt", iat);
        }

        Long exp = jwt.getExpirationTime();
        if (exp != null) {
            builder.metadata("expiresAt", exp);
        }

        // Add any custom claims that might be useful
        String givenName = jwt.getClaim("given_name");
        if (givenName != null) {
            builder.metadata("givenName", givenName);
        }

        String familyName = jwt.getClaim("family_name");
        if (familyName != null) {
            builder.metadata("familyName", familyName);
        }
    }
}
