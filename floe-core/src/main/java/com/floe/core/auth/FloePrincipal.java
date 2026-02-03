package com.floe.core.auth;

import java.security.Principal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Unified principal representing an authenticated user or service account. Supports both OIDC users
 * and API key authentication.
 */
public class FloePrincipal implements Principal {

    private final String userId;
    private final String username;
    private final Set<Role> roles;
    private final Set<Permission> permissions;
    private final String authenticationMethod; // "OIDC" or "API_KEY"
    private final Map<String, Object> metadata;

    private FloePrincipal(Builder builder) {
        this.userId = Objects.requireNonNull(builder.userId, "userId required");
        this.username = Objects.requireNonNull(builder.username, "username required");
        this.roles = Set.copyOf(builder.roles);
        this.permissions = computePermissions(this.roles);
        this.authenticationMethod = builder.authenticationMethod;
        this.metadata = Map.copyOf(builder.metadata);
    }

    @Override
    public String getName() {
        return username;
    }

    /** Returns the unique user identifier. */
    public String userId() {
        return userId;
    }

    /** Returns the username. */
    public String username() {
        return username;
    }

    /** Returns the set of roles assigned to this principal. */
    public Set<Role> roles() {
        return roles;
    }

    /** Returns all permissions granted through assigned roles. */
    public Set<Permission> permissions() {
        return permissions;
    }

    /** Returns the authentication method used (OIDC or API_KEY). */
    public String authenticationMethod() {
        return authenticationMethod;
    }

    /** Returns metadata associated with this principal. */
    public Map<String, Object> metadata() {
        return metadata;
    }

    /**
     * Check if this principal has the specified role.
     *
     * @param role the role to check
     * @return true if the principal has this role
     */
    public boolean hasRole(Role role) {
        return roles.contains(role);
    }

    /**
     * Check if this principal has any of the specified roles.
     *
     * @param roles the roles to check
     * @return true if the principal has at least one of the roles
     */
    public boolean hasAnyRole(Role... roles) {
        return Arrays.stream(roles).anyMatch(this.roles::contains);
    }

    /**
     * Check if this principal has the specified permission.
     *
     * @param permission the permission to check
     * @return true if the principal has this permission
     */
    public boolean hasPermission(Permission permission) {
        return permissions.contains(permission);
    }

    /**
     * Check if this principal has all of the specified permissions.
     *
     * @param permissions the permissions to check
     * @return true if the principal has all permissions
     */
    public boolean hasAllPermissions(Permission... permissions) {
        return Arrays.stream(permissions).allMatch(this.permissions::contains);
    }

    /**
     * Check if this principal has any of the specified permissions.
     *
     * @param permissions the permissions to check
     * @return true if the principal has at least one permission
     */
    public boolean hasAnyPermission(Permission... permissions) {
        return Arrays.stream(permissions).anyMatch(this.permissions::contains);
    }

    private Set<Permission> computePermissions(Set<Role> roles) {
        return roles.stream()
                .flatMap(role -> role.permissions().stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for FloePrincipal. */
    public static class Builder {

        private String userId;
        private String username;
        private Set<Role> roles = new HashSet<>();
        private String authenticationMethod;
        private Map<String, Object> metadata = new HashMap<>();

        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder roles(Set<Role> roles) {
            this.roles = roles;
            return this;
        }

        public Builder role(Role role) {
            this.roles.add(role);
            return this;
        }

        public Builder authenticationMethod(String method) {
            this.authenticationMethod = method;
            return this;
        }

        public Builder metadata(String key, Object value) {
            this.metadata.put(key, value);
            return this;
        }

        public FloePrincipal build() {
            return new FloePrincipal(this);
        }
    }
}
