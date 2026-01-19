package com.floe.core.auth;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Roles define a set of permissions for API access.
 *
 * <p>Roles are hierarchical in nature: ADMIN > OPERATOR > VIEWER
 */
@SuppressWarnings("ImmutableEnumChecker")
public enum Role {
    /** Administrator - Full system access */
    ADMIN("Administrator with full system access", EnumSet.allOf(Permission.class)),

    /** Operator - Can trigger maintenance and view data */
    OPERATOR(
            "Operator with ability to trigger maintenance and view all data",
            EnumSet.of(
                    Permission.READ_POLICIES,
                    Permission.READ_TABLES,
                    Permission.READ_OPERATIONS,
                    Permission.TRIGGER_MAINTENANCE)),

    /** Viewer - Read-only access */
    VIEWER(
            "Viewer with read-only access to policies, tables, and operations",
            EnumSet.of(
                    Permission.READ_POLICIES, Permission.READ_TABLES, Permission.READ_OPERATIONS));

    private final String description;
    private final Set<Permission> permissions;

    Role(String description, Set<Permission> permissions) {
        this.description = description;
        this.permissions = Collections.unmodifiableSet(permissions);
    }

    public String description() {
        return description;
    }

    /** Check if this role has a specific permission. */
    public boolean hasPermission(Permission permission) {
        return permissions.contains(permission);
    }

    /** Get all permissions granted to this role. */
    public Set<Permission> permissions() {
        return permissions;
    }
}
