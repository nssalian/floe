package com.floe.server.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floe.core.auth.*;
import com.floe.server.config.FloeConfig;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of AuditLogger that logs security events using a dual-writer pattern: 1. Database
 * (PostgreSQL) - Structured, queryable, append-only audit trail for compliance 2. Application logs
 * - Real-time monitoring and SIEM integration
 *
 * <p>This approach provides defense-in-depth and meets SOC 2/GDPR requirements.
 */
@ApplicationScoped
public class AuditLoggerImpl implements AuditLogger {

    private static final Logger LOG = LoggerFactory.getLogger(AuditLoggerImpl.class);

    @Inject FloeConfig config;

    @Inject ObjectMapper objectMapper;

    @Inject AuditLogRepository auditLogRepository;

    @Override
    public void log(AuditEvent event, Object... context) {
        if (!config.security().audit().enabled()) {
            return;
        }

        try {
            // Build details map from context
            Map<String, Object> details = new HashMap<>();
            if (context != null && context.length > 0) {
                for (int i = 0; i < context.length; i++) {
                    details.put("context_" + i, String.valueOf(context[i]));
                }
            }

            // 1. Write to database (append-only, queryable)
            AuditLog auditLog =
                    AuditLog.builder()
                            .timestamp(Instant.now())
                            .eventType(event.name())
                            .eventDescription(event.description())
                            .severity(getEventSeverity(event))
                            .details(details)
                            .build();

            persistAuditLog(auditLog);

            // 2. Write to application log (real-time monitoring)
            ObjectNode logEntry = objectMapper.createObjectNode();
            logEntry.put("timestamp", auditLog.timestamp().toString());
            logEntry.put("event", event.name());
            logEntry.put("description", event.description());

            if (context != null && context.length > 0) {
                for (int i = 0; i < context.length; i++) {
                    logEntry.put("context_" + i, String.valueOf(context[i]));
                }
            }

            String json = objectMapper.writeValueAsString(logEntry);
            logToApplicationLog(event, json);
        } catch (Exception e) {
            LOG.error("Failed to write audit log", e);
        }
    }

    @Override
    public void logAccess(
            AuditEvent event, FloePrincipal principal, String resource, String result) {
        if (!config.security().audit().enabled()) {
            return;
        }

        try {
            Map<String, Object> details = new HashMap<>();
            details.put("roles", principal.roles().toString());
            details.put("result", result);
            details.putAll(principal.metadata());

            // 1. Write to database
            AuditLog auditLog =
                    AuditLog.builder()
                            .timestamp(Instant.now())
                            .eventType(event.name())
                            .eventDescription(event.description())
                            .severity(getEventSeverity(event))
                            .userId(principal.userId())
                            .username(principal.username())
                            .authMethod(principal.authenticationMethod())
                            .resource(resource)
                            .details(details)
                            .build();

            persistAuditLog(auditLog);

            // 2. Write to application log
            Map<String, Object> logEntry = new HashMap<>();
            logEntry.put("timestamp", auditLog.timestamp());
            logEntry.put("event", event.name());
            logEntry.put("description", event.description());
            logEntry.put("userId", principal.userId());
            logEntry.put("username", principal.username());
            logEntry.put("roles", principal.roles());
            logEntry.put("authMethod", principal.authenticationMethod());
            logEntry.put("resource", resource);
            logEntry.put("result", result);

            String json = objectMapper.writeValueAsString(logEntry);
            logToApplicationLog(event, json);
        } catch (Exception e) {
            LOG.error("Failed to write audit log", e);
        }
    }

    @Override
    public void logSecurityEvent(AuditEvent event, String ipAddress, String details) {
        if (!config.security().audit().enabled()) {
            return;
        }

        try {
            Map<String, Object> detailsMap = new HashMap<>();
            detailsMap.put("details", details);

            // 1. Write to database
            AuditLog auditLog =
                    AuditLog.builder()
                            .timestamp(Instant.now())
                            .eventType(event.name())
                            .eventDescription(event.description())
                            .severity(getEventSeverity(event))
                            .ipAddress(ipAddress)
                            .details(detailsMap)
                            .build();

            persistAuditLog(auditLog);

            // 2. Write to application log
            Map<String, Object> logEntry = new HashMap<>();
            logEntry.put("timestamp", auditLog.timestamp());
            logEntry.put("event", event.name());
            logEntry.put("description", event.description());
            logEntry.put("ipAddress", ipAddress);
            logEntry.put("details", details);
            logEntry.put("severity", getEventSeverity(event));

            String json = objectMapper.writeValueAsString(logEntry);
            logToApplicationLog(event, json);
        } catch (Exception e) {
            LOG.error("Failed to write audit log", e);
        }
    }

    /**
     * Persist audit log to database asynchronously (non-blocking) If database write fails, error is
     * logged but doesn't block the request
     */
    private void persistAuditLog(AuditLog auditLog) {
        // Check if database audit logging is enabled
        if (!config.security().audit().databaseEnabled()) {
            return;
        }

        try {
            // Check if Quarkus runtime is available (for unit tests)
            if (io.quarkus.arc.Arc.container() != null) {
                // Use requires-new transaction to avoid rolling back audit logs
                // if the main transaction fails
                QuarkusTransaction.requiringNew()
                        .run(
                                () -> {
                                    auditLogRepository.save(auditLog);
                                });
            } else {
                // Direct save for unit tests (no transaction)
                auditLogRepository.save(auditLog);
            }
        } catch (Exception e) {
            // Log error but don't fail the request
            // Application logs still capture the event
            LOG.error("Failed to persist audit log to database", e);
        }
    }

    /**
     * Write the audit event to the application log based on severity
     *
     * @param event The audit event type
     * @param json JSON formatted log entry
     */
    private void logToApplicationLog(AuditEvent event, String json) {
        switch (getEventSeverity(event)) {
            case "ERROR":
                LOG.error(json);
                break;
            case "WARN":
                LOG.warn(json);
                break;
            default:
                LOG.info(json);
        }
    }

    /**
     * Determine severity level based on event type
     *
     * @param event Audit event
     * @return Severity level (INFO, WARN, ERROR)
     */
    private String getEventSeverity(AuditEvent event) {
        return switch (event) {
            case AUTH_FAILED_INVALID_KEY,
                            AUTH_FAILED_DISABLED_KEY,
                            AUTH_FAILED_EXPIRED_KEY,
                            AUTH_FAILED_INVALID_TOKEN,
                            AUTHZ_FAILED,
                            SUSPICIOUS_ACTIVITY,
                            INVALID_REQUEST ->
                    "WARN";
            case AUTH_ERROR, MAINTENANCE_FAILED -> "ERROR";
            default -> "INFO";
        };
    }
}
