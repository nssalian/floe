package com.floe.core.auth;

/** Interface for logging security audit events. Implementations should log to structured format */
public interface AuditLogger {

    /**
     * Log an audit event with context information
     *
     * @param event The type of audit event
     * @param context Additional context (userId, IP, resource, etc.)
     */
    void log(AuditEvent event, Object... context);

    /**
     * Log an audit event with principal and resource information
     *
     * @param event The type of audit event
     * @param principal The authenticated principal (user or service)
     * @param resource The resource being accessed (e.g., policy ID, table name)
     * @param result The result of the operation (success, failure, etc.)
     */
    void logAccess(AuditEvent event, FloePrincipal principal, String resource, String result);

    /**
     * Log a security event (failed auth, rate limit exceeded, etc.)
     *
     * @param event The type of security event
     * @param ipAddress Client IP address
     * @param details Additional details about the event
     */
    void logSecurityEvent(AuditEvent event, String ipAddress, String details);
}
