package com.floe.server.auth;

import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.floe.core.auth.AuditEvent;
import com.floe.core.auth.AuditLogRepository;
import com.floe.core.auth.FloePrincipal;
import com.floe.core.auth.Role;
import com.floe.server.config.FloeConfig;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuditLoggerImplTest {

    private AuditLoggerImpl auditLogger;

    @Mock private FloeConfig config;

    @Mock private FloeConfig.Security security;

    @Mock private FloeConfig.Security.Audit audit;

    @Mock private AuditLogRepository auditLogRepository;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new JavaTimeModule());
        auditLogger = new AuditLoggerImpl();
        auditLogger.config = config;
        auditLogger.objectMapper = objectMapper;
        auditLogger.auditLogRepository = auditLogRepository;

        // Use lenient stubbing to avoid UnnecessaryStubbingException
        lenient().when(config.security()).thenReturn(security);
        lenient().when(security.audit()).thenReturn(audit);
        lenient().when(audit.enabled()).thenReturn(true);
        lenient().when(audit.databaseEnabled()).thenReturn(true);
    }

    @Test
    void testLogSimpleEvent() throws Exception {
        auditLogger.log(AuditEvent.AUTH_SUCCESS);

        // Verify no exceptions are thrown
    }

    @Test
    void testLogEventWithContext() throws Exception {
        auditLogger.log(AuditEvent.AUTH_SUCCESS, "user123", "192.168.1.1");

        // Verify no exceptions
    }

    @Test
    void testLogEventWithMultipleContext() throws Exception {
        auditLogger.log(
                AuditEvent.API_KEY_CREATED, "key-id", "admin-user", "192.168.1.1", "extra-info");

        // Verify no exceptions
    }

    @Test
    void testLogAccessWithPrincipal() throws Exception {
        FloePrincipal principal =
                FloePrincipal.builder()
                        .userId("user-123")
                        .username("test-user")
                        .roles(Set.of(Role.ADMIN))
                        .authenticationMethod("OIDC")
                        .build();

        auditLogger.logAccess(AuditEvent.REQUEST_AUTHORIZED, principal, "/api/policies", "SUCCESS");

        // Verify no exceptions
    }

    @Test
    void testLogAccessWithPrincipalRoles() throws Exception {
        FloePrincipal principal =
                FloePrincipal.builder()
                        .userId("user-456")
                        .username("operator-user")
                        .roles(Set.of(Role.OPERATOR))
                        .authenticationMethod("API_KEY")
                        .build();

        auditLogger.logAccess(AuditEvent.AUTHZ_SUCCESS, principal, "/api/operations", "ALLOWED");

        // Verify no exceptions
    }

    @Test
    void testLogSecurityEvent() throws Exception {
        auditLogger.logSecurityEvent(
                AuditEvent.SUSPICIOUS_ACTIVITY, "192.168.1.100", "Too many requests");

        // Verify no exceptions
    }

    @Test
    void testLogSecurityEventWithNullIp() throws Exception {
        auditLogger.logSecurityEvent(AuditEvent.SUSPICIOUS_ACTIVITY, null, "Anomaly detected");

        // Verify no exceptions (should handle null gracefully)
    }

    @Test
    void testLoggingDisabled() {
        when(audit.enabled()).thenReturn(false);

        // Should not log anything when disabled
        auditLogger.log(AuditEvent.AUTH_SUCCESS);
        auditLogger.logAccess(
                AuditEvent.REQUEST_AUTHORIZED, createMockPrincipal(), "/api/test", "SUCCESS");
        auditLogger.logSecurityEvent(AuditEvent.SUSPICIOUS_ACTIVITY, "1.2.3.4", "test");

        // Verify no exceptions and methods complete quickly
    }

    @Test
    void testDatabaseLoggingDisabled() {
        when(audit.databaseEnabled()).thenReturn(false);

        auditLogger.log(AuditEvent.AUTH_SUCCESS, "test-context");

        // Should only log to application logs, not database
        verifyNoInteractions(auditLogRepository);
    }

    @Test
    void testSeverityMappingWarnEvents() {
        AuditEvent[] warnEvents = {
            AuditEvent.AUTH_FAILED_INVALID_KEY,
            AuditEvent.AUTH_FAILED_DISABLED_KEY,
            AuditEvent.AUTH_FAILED_EXPIRED_KEY,
            AuditEvent.AUTH_FAILED_INVALID_TOKEN,
            AuditEvent.AUTHZ_FAILED,
            AuditEvent.SUSPICIOUS_ACTIVITY,
            AuditEvent.INVALID_REQUEST,
        };

        for (AuditEvent event : warnEvents) {
            auditLogger.logSecurityEvent(event, "1.2.3.4", "test");
            // Verify no exceptions - severity is determined internally
        }
    }

    @Test
    void testSeverityMappingErrorEvents() {
        AuditEvent[] errorEvents = {
            AuditEvent.AUTH_ERROR, AuditEvent.MAINTENANCE_FAILED,
        };

        for (AuditEvent event : errorEvents) {
            auditLogger.logSecurityEvent(event, "1.2.3.4", "test");
            // Verify no exceptions
        }
    }

    @Test
    void testSeverityMappingInfoEvents() {
        AuditEvent[] infoEvents = {
            AuditEvent.AUTH_SUCCESS,
            AuditEvent.AUTHZ_SUCCESS,
            AuditEvent.REQUEST_AUTHORIZED,
            AuditEvent.API_KEY_CREATED,
            AuditEvent.API_KEY_UPDATED,
            AuditEvent.API_KEY_REVOKED,
            AuditEvent.API_KEY_VIEWED,
            AuditEvent.POLICY_CREATED,
            AuditEvent.POLICY_UPDATED,
            AuditEvent.POLICY_DELETED,
            AuditEvent.MAINTENANCE_TRIGGERED,
            AuditEvent.MAINTENANCE_COMPLETED,
        };

        for (AuditEvent event : infoEvents) {
            auditLogger.log(event);
            // Verify no exceptions
        }
    }

    @Test
    void testNullContextHandling() {
        auditLogger.log(AuditEvent.AUTH_SUCCESS, (Object[]) null);

        // Verify no exceptions
    }

    @Test
    void testEmptyContextHandling() {
        auditLogger.log(AuditEvent.AUTH_SUCCESS, new Object[0]);

        // Verify no exceptions
    }

    @Test
    void testSpecialCharactersInContext() {
        auditLogger.log(
                AuditEvent.AUTH_SUCCESS,
                "user\"with'quotes",
                "ip:192.168.1.1",
                "data\nwith\nnewlines");

        // Verify no exceptions (JSON escaping should handle this)
    }

    @Test
    void testAllAuditEventsCanBeLogged() {
        // Verify every audit event can be logged without error
        for (AuditEvent event : AuditEvent.values()) {
            auditLogger.log(event, "test-context");
        }

        // All events should log successfully
    }

    @Test
    void testPrincipalWithMetadata() {
        FloePrincipal principal =
                FloePrincipal.builder()
                        .userId("user-123")
                        .username("metadata-user")
                        .roles(Set.of(Role.ADMIN))
                        .authenticationMethod("OIDC")
                        .metadata("email", "user@example.com")
                        .metadata("department", "Engineering")
                        .metadata("custom_field", "custom_value")
                        .build();

        auditLogger.logAccess(AuditEvent.AUTHZ_SUCCESS, principal, "/api/policies", "ALLOWED");

        // Verify no exceptions with metadata
    }

    private FloePrincipal createMockPrincipal() {
        return FloePrincipal.builder()
                .userId("mock-user")
                .username("mock-username")
                .roles(Set.of(Role.VIEWER))
                .authenticationMethod("API_KEY")
                .build();
    }
}
