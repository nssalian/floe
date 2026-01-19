package com.floe.core.auth;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("AuditLog")
class AuditLogTest {

    @Nested
    @DisplayName("Record")
    class RecordTests {

        @Test
        @DisplayName("should create AuditLog with all fields")
        void shouldCreateAuditLogWithAllFields() {
            Instant now = Instant.now();
            Map<String, Object> details = Map.of("key", "value");

            AuditLog log =
                    new AuditLog(
                            1L,
                            now,
                            "AUTH_SUCCESS",
                            "Authentication successful",
                            "INFO",
                            "user-123",
                            "john.doe",
                            "API_KEY",
                            "/api/v1/policies",
                            "GET",
                            "192.168.1.1",
                            "Mozilla/5.0",
                            details,
                            now);

            assertEquals(1L, log.id());
            assertEquals(now, log.timestamp());
            assertEquals("AUTH_SUCCESS", log.eventType());
            assertEquals("Authentication successful", log.eventDescription());
            assertEquals("INFO", log.severity());
            assertEquals("user-123", log.userId());
            assertEquals("john.doe", log.username());
            assertEquals("API_KEY", log.authMethod());
            assertEquals("/api/v1/policies", log.resource());
            assertEquals("GET", log.httpMethod());
            assertEquals("192.168.1.1", log.ipAddress());
            assertEquals("Mozilla/5.0", log.userAgent());
            assertEquals(details, log.details());
            assertEquals(now, log.createdAt());
        }

        @Test
        @DisplayName("should allow null fields")
        void shouldAllowNullFields() {
            AuditLog log =
                    new AuditLog(
                            null, null, null, null, null, null, null, null, null, null, null, null,
                            null, null);

            assertNull(log.id());
            assertNull(log.timestamp());
            assertNull(log.eventType());
        }
    }

    @Nested
    @DisplayName("Builder Defaults")
    class BuilderDefaults {

        @Test
        @DisplayName("should set default timestamp")
        void builderShouldSetDefaultTimestamp() {
            Instant before = Instant.now();
            AuditLog log = AuditLog.builder().build();
            Instant after = Instant.now();

            assertNotNull(log.timestamp());
            assertFalse(log.timestamp().isBefore(before));
            assertFalse(log.timestamp().isAfter(after));
        }

        @Test
        @DisplayName("should set default severity to INFO")
        void builderShouldSetDefaultSeverity() {
            AuditLog log = AuditLog.builder().build();
            assertEquals("INFO", log.severity());
        }

        @Test
        @DisplayName("should set default createdAt")
        void builderShouldSetDefaultCreatedAt() {
            Instant before = Instant.now();
            AuditLog log = AuditLog.builder().build();
            Instant after = Instant.now();

            assertNotNull(log.createdAt());
            assertFalse(log.createdAt().isBefore(before));
            assertFalse(log.createdAt().isAfter(after));
        }
    }

    @Nested
    @DisplayName("Builder Fields")
    class BuilderFields {

        @Test
        @DisplayName("should set id")
        void builderShouldSetId() {
            AuditLog log = AuditLog.builder().id(123L).build();
            assertEquals(123L, log.id());
        }

        @Test
        @DisplayName("should set timestamp")
        void builderShouldSetTimestamp() {
            Instant timestamp = Instant.parse("2025-12-15T10:30:00Z");
            AuditLog log = AuditLog.builder().timestamp(timestamp).build();
            assertEquals(timestamp, log.timestamp());
        }

        @Test
        @DisplayName("should set eventType")
        void builderShouldSetEventType() {
            AuditLog log = AuditLog.builder().eventType("POLICY_CREATED").build();
            assertEquals("POLICY_CREATED", log.eventType());
        }

        @Test
        @DisplayName("should set eventDescription")
        void builderShouldSetEventDescription() {
            AuditLog log = AuditLog.builder().eventDescription("Policy was created").build();
            assertEquals("Policy was created", log.eventDescription());
        }

        @Test
        @DisplayName("should set severity")
        void builderShouldSetSeverity() {
            AuditLog log = AuditLog.builder().severity("WARN").build();
            assertEquals("WARN", log.severity());
        }

        @Test
        @DisplayName("should set userId")
        void builderShouldSetUserId() {
            AuditLog log = AuditLog.builder().userId("user-abc").build();
            assertEquals("user-abc", log.userId());
        }

        @Test
        @DisplayName("should set username")
        void builderShouldSetUsername() {
            AuditLog log = AuditLog.builder().username("alice").build();
            assertEquals("alice", log.username());
        }

        @Test
        @DisplayName("should set authMethod")
        void builderShouldSetAuthMethod() {
            AuditLog log = AuditLog.builder().authMethod("OIDC").build();
            assertEquals("OIDC", log.authMethod());
        }

        @Test
        @DisplayName("should set resource")
        void builderShouldSetResource() {
            AuditLog log = AuditLog.builder().resource("/api/v1/tables").build();
            assertEquals("/api/v1/tables", log.resource());
        }

        @Test
        @DisplayName("should set httpMethod")
        void builderShouldSetHttpMethod() {
            AuditLog log = AuditLog.builder().httpMethod("POST").build();
            assertEquals("POST", log.httpMethod());
        }

        @Test
        @DisplayName("should set ipAddress")
        void builderShouldSetIpAddress() {
            AuditLog log = AuditLog.builder().ipAddress("10.0.0.1").build();
            assertEquals("10.0.0.1", log.ipAddress());
        }

        @Test
        @DisplayName("should set userAgent")
        void builderShouldSetUserAgent() {
            AuditLog log = AuditLog.builder().userAgent("curl/7.68.0").build();
            assertEquals("curl/7.68.0", log.userAgent());
        }

        @Test
        @DisplayName("should set details")
        void builderShouldSetDetails() {
            Map<String, Object> details = Map.of("policyId", "pol-123", "action", "create");
            AuditLog log = AuditLog.builder().details(details).build();
            assertEquals(details, log.details());
        }

        @Test
        @DisplayName("should set createdAt")
        void builderShouldSetCreatedAt() {
            Instant createdAt = Instant.parse("2025-12-15T10:30:00Z");
            AuditLog log = AuditLog.builder().createdAt(createdAt).build();
            assertEquals(createdAt, log.createdAt());
        }

        @Test
        @DisplayName("should support method chaining")
        void builderShouldSupportMethodChaining() {
            Instant now = Instant.now();
            Map<String, Object> details = Map.of("reason", "test");

            AuditLog log =
                    AuditLog.builder()
                            .id(1L)
                            .timestamp(now)
                            .eventType("AUTH_FAILED_INVALID_KEY")
                            .eventDescription("Invalid API key")
                            .severity("WARN")
                            .userId("unknown")
                            .username("unknown")
                            .authMethod("API_KEY")
                            .resource("/api/v1/policies")
                            .httpMethod("GET")
                            .ipAddress("192.168.1.100")
                            .userAgent("test-client")
                            .details(details)
                            .createdAt(now)
                            .build();

            assertEquals(1L, log.id());
            assertEquals("AUTH_FAILED_INVALID_KEY", log.eventType());
            assertEquals("WARN", log.severity());
            assertEquals("unknown", log.userId());
            assertEquals(details, log.details());
        }
    }

    @Nested
    @DisplayName("Equality")
    class EqualityTests {

        @Test
        @DisplayName("should be equal with same values")
        void shouldBeEqualWithSameValues() {
            Instant now = Instant.now();

            AuditLog log1 =
                    AuditLog.builder()
                            .id(1L)
                            .timestamp(now)
                            .eventType("TEST")
                            .createdAt(now)
                            .build();

            AuditLog log2 =
                    AuditLog.builder()
                            .id(1L)
                            .timestamp(now)
                            .eventType("TEST")
                            .createdAt(now)
                            .build();

            assertEquals(log1, log2);
            assertEquals(log1.hashCode(), log2.hashCode());
        }

        @Test
        @DisplayName("should not be equal with different values")
        void shouldNotBeEqualWithDifferentValues() {
            AuditLog log1 = AuditLog.builder().id(1L).eventType("TEST1").build();
            AuditLog log2 = AuditLog.builder().id(2L).eventType("TEST2").build();

            assertNotEquals(log1, log2);
        }
    }
}
