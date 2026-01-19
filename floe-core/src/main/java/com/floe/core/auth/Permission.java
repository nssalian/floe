package com.floe.core.auth;

/**
 * Granular permissions for API access control.
 *
 * <p>Permissions are grouped by resource type and can be extended as needed.
 */
public enum Permission {
    // Policy management
    READ_POLICIES("Read maintenance policies"),
    WRITE_POLICIES("Create and update maintenance policies"),
    DELETE_POLICIES("Delete maintenance policies"),

    // Table operations
    READ_TABLES("View tables and their metadata"),

    // Maintenance operations
    READ_OPERATIONS("View maintenance operation history"),
    TRIGGER_MAINTENANCE("Trigger maintenance operations"),

    // API key management
    MANAGE_API_KEYS("Create, update, and revoke API keys");

    private final String description;

    Permission(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
