package com.floe.core.auth;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("AuditEvent")
class AuditEventTest {

    @Nested
    @DisplayName("Authentication Events")
    class AuthenticationEvents {

        @Test
        @DisplayName("AUTH_SUCCESS should have correct description")
        void authSuccessShouldHaveDescription() {
            assertEquals("Authentication successful", AuditEvent.AUTH_SUCCESS.description());
        }

        @Test
        @DisplayName("AUTH_FAILED_INVALID_KEY should have correct description")
        void authFailedInvalidKeyShouldHaveDescription() {
            assertEquals(
                    "Authentication failed - invalid API key",
                    AuditEvent.AUTH_FAILED_INVALID_KEY.description());
        }

        @Test
        @DisplayName("AUTH_FAILED_DISABLED_KEY should have correct description")
        void authFailedDisabledKeyShouldHaveDescription() {
            assertEquals(
                    "Authentication failed - API key disabled",
                    AuditEvent.AUTH_FAILED_DISABLED_KEY.description());
        }

        @Test
        @DisplayName("AUTH_FAILED_EXPIRED_KEY should have correct description")
        void authFailedExpiredKeyShouldHaveDescription() {
            assertEquals(
                    "Authentication failed - API key expired",
                    AuditEvent.AUTH_FAILED_EXPIRED_KEY.description());
        }

        @Test
        @DisplayName("AUTH_FAILED_INVALID_TOKEN should have correct description")
        void authFailedInvalidTokenShouldHaveDescription() {
            assertEquals(
                    "Authentication failed - invalid JWT token",
                    AuditEvent.AUTH_FAILED_INVALID_TOKEN.description());
        }

        @Test
        @DisplayName("AUTH_ERROR should have correct description")
        void authErrorShouldHaveDescription() {
            assertEquals("Authentication error occurred", AuditEvent.AUTH_ERROR.description());
        }
    }

    @Nested
    @DisplayName("Authorization Events")
    class AuthorizationEvents {

        @Test
        @DisplayName("AUTHZ_SUCCESS should have correct description")
        void authzSuccessShouldHaveDescription() {
            assertEquals("Authorization successful", AuditEvent.AUTHZ_SUCCESS.description());
        }

        @Test
        @DisplayName("AUTHZ_FAILED should have correct description")
        void authzFailedShouldHaveDescription() {
            assertEquals(
                    "Authorization failed - insufficient permissions",
                    AuditEvent.AUTHZ_FAILED.description());
        }

        @Test
        @DisplayName("REQUEST_AUTHORIZED should have correct description")
        void requestAuthorizedShouldHaveDescription() {
            assertEquals(
                    "Request authorized and processed",
                    AuditEvent.REQUEST_AUTHORIZED.description());
        }
    }

    @Nested
    @DisplayName("API Key Management Events")
    class ApiKeyManagementEvents {

        @Test
        @DisplayName("API_KEY_CREATED should have correct description")
        void apiKeyCreatedShouldHaveDescription() {
            assertEquals("API key created", AuditEvent.API_KEY_CREATED.description());
        }

        @Test
        @DisplayName("API_KEY_UPDATED should have correct description")
        void apiKeyUpdatedShouldHaveDescription() {
            assertEquals("API key updated", AuditEvent.API_KEY_UPDATED.description());
        }

        @Test
        @DisplayName("API_KEY_REVOKED should have correct description")
        void apiKeyRevokedShouldHaveDescription() {
            assertEquals("API key revoked", AuditEvent.API_KEY_REVOKED.description());
        }

        @Test
        @DisplayName("API_KEY_VIEWED should have correct description")
        void apiKeyViewedShouldHaveDescription() {
            assertEquals("API key details viewed", AuditEvent.API_KEY_VIEWED.description());
        }
    }

    @Nested
    @DisplayName("Policy Management Events")
    class PolicyManagementEvents {

        @Test
        @DisplayName("POLICY_CREATED should have correct description")
        void policyCreatedShouldHaveDescription() {
            assertEquals("Maintenance policy created", AuditEvent.POLICY_CREATED.description());
        }

        @Test
        @DisplayName("POLICY_UPDATED should have correct description")
        void policyUpdatedShouldHaveDescription() {
            assertEquals("Maintenance policy updated", AuditEvent.POLICY_UPDATED.description());
        }

        @Test
        @DisplayName("POLICY_DELETED should have correct description")
        void policyDeletedShouldHaveDescription() {
            assertEquals("Maintenance policy deleted", AuditEvent.POLICY_DELETED.description());
        }
    }

    @Nested
    @DisplayName("Maintenance Operation Events")
    class MaintenanceOperationEvents {

        @Test
        @DisplayName("MAINTENANCE_TRIGGERED should have correct description")
        void maintenanceTriggeredShouldHaveDescription() {
            assertEquals(
                    "Maintenance operation triggered",
                    AuditEvent.MAINTENANCE_TRIGGERED.description());
        }

        @Test
        @DisplayName("MAINTENANCE_COMPLETED should have correct description")
        void maintenanceCompletedShouldHaveDescription() {
            assertEquals(
                    "Maintenance operation completed",
                    AuditEvent.MAINTENANCE_COMPLETED.description());
        }

        @Test
        @DisplayName("MAINTENANCE_FAILED should have correct description")
        void maintenanceFailedShouldHaveDescription() {
            assertEquals(
                    "Maintenance operation failed", AuditEvent.MAINTENANCE_FAILED.description());
        }
    }

    @Nested
    @DisplayName("Security Events")
    class SecurityEvents {

        @Test
        @DisplayName("SUSPICIOUS_ACTIVITY should have correct description")
        void suspiciousActivityShouldHaveDescription() {
            assertEquals(
                    "Suspicious activity detected", AuditEvent.SUSPICIOUS_ACTIVITY.description());
        }

        @Test
        @DisplayName("INVALID_REQUEST should have correct description")
        void invalidRequestShouldHaveDescription() {
            assertEquals("Invalid request received", AuditEvent.INVALID_REQUEST.description());
        }
    }

    @Nested
    @DisplayName("Enum Properties")
    class EnumProperties {

        @Test
        @DisplayName("should have expected number of events")
        void shouldHaveExpectedNumberOfEvents() {
            assertEquals(21, AuditEvent.values().length);
        }

        @Test
        @DisplayName("should parse from string")
        void shouldParseFromString() {
            assertEquals(AuditEvent.AUTH_SUCCESS, AuditEvent.valueOf("AUTH_SUCCESS"));
            assertEquals(AuditEvent.POLICY_CREATED, AuditEvent.valueOf("POLICY_CREATED"));
        }

        @Test
        @DisplayName("all events should have non-empty descriptions")
        void allEventsShouldHaveNonEmptyDescriptions() {
            for (AuditEvent event : AuditEvent.values()) {
                assertNotNull(event.description());
                assertFalse(
                        event.description().isEmpty(),
                        event.name() + " should have non-empty description");
            }
        }
    }
}
