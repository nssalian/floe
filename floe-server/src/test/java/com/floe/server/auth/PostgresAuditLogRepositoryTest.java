package com.floe.server.auth;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.floe.core.auth.AuditLog;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration")
@Testcontainers
class PostgresAuditLogRepositoryTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:15-alpine")
                    .withDatabaseName("floe_test")
                    .withUsername("test")
                    .withPassword("test");

    private PostgresAuditLogRepository repository;
    private PGSimpleDataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());

        // Create table if not exists
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    """
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    event_type VARCHAR(100) NOT NULL,
                    event_description TEXT,
                    severity VARCHAR(20) NOT NULL,
                    user_id VARCHAR(255),
                    username VARCHAR(255),
                    auth_method VARCHAR(50),
                    resource VARCHAR(500),
                    http_method VARCHAR(10),
                    ip_address VARCHAR(45),
                    user_agent TEXT,
                    details JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
                """);
            // Clear table for each test
            stmt.execute("TRUNCATE TABLE audit_logs");
        }

        repository = new PostgresAuditLogRepository();

        // Inject dependencies via reflection
        var dataSourceField = PostgresAuditLogRepository.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        dataSourceField.set(repository, dataSource);

        var objectMapperField = PostgresAuditLogRepository.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(repository, new ObjectMapper());
    }

    private AuditLog createTestLog(String eventType, String userId) {
        return new AuditLog(
                null,
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                eventType,
                "Test event",
                "INFO",
                userId,
                "testuser",
                "API_KEY",
                "/api/v1/test",
                "GET",
                "127.0.0.1",
                "TestAgent/1.0",
                Map.of("key", "value"),
                null);
    }

    @Nested
    class Save {

        @Test
        void shouldSaveAuditLog() {
            AuditLog log = createTestLog("REQUEST_AUTHORIZED", "user-1");

            AuditLog saved = repository.save(log);

            assertNotNull(saved.id());
            assertNotNull(saved.createdAt());
            assertEquals(log.eventType(), saved.eventType());
            assertEquals(log.userId(), saved.userId());
        }

        @Test
        void shouldSaveLogWithAllFields() {
            Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            AuditLog log =
                    new AuditLog(
                            null,
                            now,
                            "AUTH_SUCCESS",
                            "User authenticated successfully",
                            "INFO",
                            "user-123",
                            "admin@example.com",
                            "OIDC",
                            "/api/v1/policies",
                            "POST",
                            "192.168.1.100",
                            "Mozilla/5.0",
                            Map.of("action", "create", "count", 5),
                            null);

            AuditLog saved = repository.save(log);

            assertNotNull(saved.id());
            assertEquals(now, saved.timestamp());
            assertEquals("AUTH_SUCCESS", saved.eventType());
            assertEquals("User authenticated successfully", saved.eventDescription());
            assertEquals("INFO", saved.severity());
            assertEquals("user-123", saved.userId());
            assertEquals("admin@example.com", saved.username());
            assertEquals("OIDC", saved.authMethod());
            assertEquals("/api/v1/policies", saved.resource());
            assertEquals("POST", saved.httpMethod());
            assertEquals("192.168.1.100", saved.ipAddress());
            assertEquals("Mozilla/5.0", saved.userAgent());
        }

        @Test
        void shouldSaveLogWithNullOptionalFields() {
            AuditLog log =
                    new AuditLog(
                            null,
                            Instant.now().truncatedTo(ChronoUnit.MILLIS),
                            "SYSTEM_EVENT",
                            null,
                            "DEBUG",
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);

            AuditLog saved = repository.save(log);

            assertNotNull(saved.id());
            assertEquals("SYSTEM_EVENT", saved.eventType());
            assertNull(saved.eventDescription());
            assertNull(saved.userId());
            assertNull(saved.details());
        }
    }

    @Nested
    class FindById {

        @Test
        void shouldFindById() {
            AuditLog log = repository.save(createTestLog("TEST_EVENT", "user-1"));

            Optional<AuditLog> found = repository.findById(log.id());

            assertTrue(found.isPresent());
            assertEquals(log.id(), found.get().id());
            assertEquals("TEST_EVENT", found.get().eventType());
        }

        @Test
        void shouldReturnEmptyWhenIdNotFound() {
            Optional<AuditLog> found = repository.findById(999999L);

            assertTrue(found.isEmpty());
        }
    }

    @Nested
    class FindByTimeRange {

        @Test
        void shouldFindByTimeRange() {
            Instant now = Instant.now();
            Instant hourAgo = now.minus(1, ChronoUnit.HOURS);
            Instant hourAhead = now.plus(1, ChronoUnit.HOURS);

            repository.save(createTestLog("EVENT_1", "user-1"));
            repository.save(createTestLog("EVENT_2", "user-2"));

            List<AuditLog> logs = repository.findByTimeRange(hourAgo, hourAhead, 100);

            assertEquals(2, logs.size());
        }

        @Test
        void shouldRespectLimit() {
            for (int i = 0; i < 10; i++) {
                repository.save(createTestLog("EVENT_" + i, "user-" + i));
            }

            List<AuditLog> logs =
                    repository.findByTimeRange(
                            Instant.now().minus(1, ChronoUnit.HOURS),
                            Instant.now().plus(1, ChronoUnit.HOURS),
                            5);

            assertEquals(5, logs.size());
        }

        @Test
        void shouldReturnEmptyWhenNoLogsInRange() {
            Instant futureStart = Instant.now().plus(1, ChronoUnit.DAYS);
            Instant futureEnd = Instant.now().plus(2, ChronoUnit.DAYS);

            repository.save(createTestLog("EVENT_1", "user-1"));

            List<AuditLog> logs = repository.findByTimeRange(futureStart, futureEnd, 100);

            assertTrue(logs.isEmpty());
        }
    }

    @Nested
    class FindByUserId {

        @Test
        void shouldFindByUserId() {
            repository.save(createTestLog("EVENT_1", "user-target"));
            repository.save(createTestLog("EVENT_2", "user-target"));
            repository.save(createTestLog("EVENT_3", "user-other"));

            List<AuditLog> logs = repository.findByUserId("user-target", 100);

            assertEquals(2, logs.size());
            assertTrue(logs.stream().allMatch(l -> "user-target".equals(l.userId())));
        }

        @Test
        void shouldReturnEmptyWhenUserIdNotFound() {
            repository.save(createTestLog("EVENT_1", "user-1"));

            List<AuditLog> logs = repository.findByUserId("nonexistent", 100);

            assertTrue(logs.isEmpty());
        }

        @Test
        void shouldRespectLimit() {
            for (int i = 0; i < 10; i++) {
                repository.save(createTestLog("EVENT_" + i, "same-user"));
            }

            List<AuditLog> logs = repository.findByUserId("same-user", 3);

            assertEquals(3, logs.size());
        }
    }

    @Nested
    class FindByEventType {

        @Test
        void shouldFindByEventType() {
            repository.save(createTestLog("AUTH_SUCCESS", "user-1"));
            repository.save(createTestLog("AUTH_SUCCESS", "user-2"));
            repository.save(createTestLog("AUTH_FAILURE", "user-3"));

            List<AuditLog> logs = repository.findByEventType("AUTH_SUCCESS", 100);

            assertEquals(2, logs.size());
            assertTrue(logs.stream().allMatch(l -> "AUTH_SUCCESS".equals(l.eventType())));
        }

        @Test
        void shouldReturnEmptyWhenEventTypeNotFound() {
            repository.save(createTestLog("EVENT_A", "user-1"));

            List<AuditLog> logs = repository.findByEventType("NONEXISTENT", 100);

            assertTrue(logs.isEmpty());
        }
    }

    @Nested
    class FindBySeverity {

        @Test
        void shouldFindBySeverity() {
            AuditLog infoLog =
                    new AuditLog(
                            null,
                            Instant.now().truncatedTo(ChronoUnit.MILLIS),
                            "EVENT_1",
                            "desc",
                            "INFO",
                            "user-1",
                            "user",
                            "API_KEY",
                            "/api",
                            "GET",
                            "127.0.0.1",
                            "agent",
                            null,
                            null);
            AuditLog errorLog =
                    new AuditLog(
                            null,
                            Instant.now().truncatedTo(ChronoUnit.MILLIS),
                            "EVENT_2",
                            "desc",
                            "ERROR",
                            "user-2",
                            "user",
                            "API_KEY",
                            "/api",
                            "GET",
                            "127.0.0.1",
                            "agent",
                            null,
                            null);

            repository.save(infoLog);
            repository.save(errorLog);

            List<AuditLog> logs = repository.findBySeverity("ERROR", 100);

            assertEquals(1, logs.size());
            assertEquals("ERROR", logs.get(0).severity());
        }
    }

    @Nested
    class CountByEventType {

        @Test
        void shouldCountByEventType() {
            Instant now = Instant.now();
            repository.save(createTestLog("TARGET_EVENT", "user-1"));
            repository.save(createTestLog("TARGET_EVENT", "user-2"));
            repository.save(createTestLog("OTHER_EVENT", "user-3"));

            long count =
                    repository.countByEventType(
                            "TARGET_EVENT",
                            now.minus(1, ChronoUnit.HOURS),
                            now.plus(1, ChronoUnit.HOURS));

            assertEquals(2, count);
        }

        @Test
        void shouldReturnZeroWhenNoMatches() {
            Instant now = Instant.now();
            repository.save(createTestLog("SOME_EVENT", "user-1"));

            long count =
                    repository.countByEventType(
                            "NONEXISTENT",
                            now.minus(1, ChronoUnit.HOURS),
                            now.plus(1, ChronoUnit.HOURS));

            assertEquals(0, count);
        }
    }

    @Nested
    class DeleteOlderThan {

        @Test
        void shouldDeleteOlderThan() throws Exception {
            // Insert old log directly with old timestamp
            try (Connection conn = dataSource.getConnection();
                    Statement stmt = conn.createStatement()) {
                stmt.execute(
                        """
                    INSERT INTO audit_logs (timestamp, event_type, severity)
                    VALUES (NOW() - INTERVAL '100 days', 'OLD_EVENT', 'INFO')
                    """);
            }

            // Insert recent log
            repository.save(createTestLog("RECENT_EVENT", "user-1"));

            long deleted = repository.deleteOlderThan(Instant.now().minus(30, ChronoUnit.DAYS));

            assertEquals(1, deleted);
            // Recent log should still exist
            List<AuditLog> remaining = repository.findByEventType("RECENT_EVENT", 100);
            assertEquals(1, remaining.size());
        }

        @Test
        void shouldReturnZeroWhenNothingToDelete() {
            repository.save(createTestLog("RECENT_EVENT", "user-1"));

            long deleted = repository.deleteOlderThan(Instant.now().minus(365, ChronoUnit.DAYS));

            assertEquals(0, deleted);
        }
    }

    @Nested
    class DetailsJsonHandling {

        @Test
        void shouldPersistAndRetrieveComplexDetails() {
            Map<String, Object> details =
                    Map.of(
                            "stringValue",
                            "hello",
                            "numberValue",
                            42,
                            "booleanValue",
                            true,
                            "nestedMap",
                            Map.of("inner", "value"));

            AuditLog log =
                    new AuditLog(
                            null,
                            Instant.now().truncatedTo(ChronoUnit.MILLIS),
                            "COMPLEX_EVENT",
                            "Event with complex details",
                            "INFO",
                            "user-1",
                            "testuser",
                            "API_KEY",
                            "/api/test",
                            "POST",
                            "127.0.0.1",
                            "TestAgent",
                            details,
                            null);

            AuditLog saved = repository.save(log);
            Optional<AuditLog> found = repository.findById(saved.id());

            assertTrue(found.isPresent());
            assertNotNull(found.get().details());
            assertEquals("hello", found.get().details().get("stringValue"));
            assertEquals(42, found.get().details().get("numberValue"));
            assertEquals(true, found.get().details().get("booleanValue"));
        }
    }
}
