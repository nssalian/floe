package com.floe.core.auth;

/** Security audit events that should be logged for compliance and security monitoring. */
public enum AuditEvent {
    // Authentication Events
    AUTH_SUCCESS("Authentication successful"),
    AUTH_FAILED_INVALID_KEY("Authentication failed - invalid API key"),
    AUTH_FAILED_DISABLED_KEY("Authentication failed - API key disabled"),
    AUTH_FAILED_EXPIRED_KEY("Authentication failed - API key expired"),
    AUTH_FAILED_INVALID_TOKEN("Authentication failed - invalid JWT token"),
    AUTH_ERROR("Authentication error occurred"),

    // Authorization Events
    AUTHZ_SUCCESS("Authorization successful"),
    AUTHZ_FAILED("Authorization failed - insufficient permissions"),
    REQUEST_AUTHORIZED("Request authorized and processed"),

    // API Key Management Events
    API_KEY_CREATED("API key created"),
    API_KEY_UPDATED("API key updated"),
    API_KEY_REVOKED("API key revoked"),
    API_KEY_VIEWED("API key details viewed"),

    // Policy Management Events
    POLICY_CREATED("Maintenance policy created"),
    POLICY_UPDATED("Maintenance policy updated"),
    POLICY_DELETED("Maintenance policy deleted"),

    // Maintenance Operation Events
    MAINTENANCE_TRIGGERED("Maintenance operation triggered"),
    MAINTENANCE_COMPLETED("Maintenance operation completed"),
    MAINTENANCE_FAILED("Maintenance operation failed"),

    // Security Events
    SUSPICIOUS_ACTIVITY("Suspicious activity detected"),
    INVALID_REQUEST("Invalid request received");

    private final String description;

    AuditEvent(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
