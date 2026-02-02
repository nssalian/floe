package com.floe.core.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration")
@Testcontainers
class PostgresOperationStoreMetricsTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:15-alpine")
                    .withDatabaseName("floe_test")
                    .withUsername("test")
                    .withPassword("test");

    private PostgresOperationStore store;

    @BeforeEach
    void setUp() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());

        store = new PostgresOperationStore(dataSource);
        store.initializeSchema();
        store.clear();
    }

    @Test
    void schemaHasNewColumns() throws Exception {
        try (Connection conn = postgres.createConnection("")) {
            ResultSet rs =
                    conn.getMetaData().getColumns(null, null, "maintenance_operations", null);

            boolean engineType = false;
            boolean executionId = false;
            boolean scheduleId = false;
            boolean policyVersion = false;
            boolean normalizedMetrics = false;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                if ("engine_type".equals(name)) {
                    engineType = true;
                } else if ("execution_id".equals(name)) {
                    executionId = true;
                } else if ("schedule_id".equals(name)) {
                    scheduleId = true;
                } else if ("policy_version".equals(name)) {
                    policyVersion = true;
                } else if ("normalized_metrics".equals(name)) {
                    normalizedMetrics = true;
                }
            }

            assertTrue(engineType);
            assertTrue(executionId);
            assertTrue(scheduleId);
            assertTrue(policyVersion);
            assertTrue(normalizedMetrics);
        }
    }

    @Test
    void savePersistsNewFields() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();

        OperationRecord record =
                OperationRecord.builder()
                        .id(id)
                        .catalog("demo")
                        .namespace("db")
                        .tableName("table")
                        .engineType("SPARK")
                        .executionId("exec-1")
                        .scheduleId("schedule-1")
                        .policyVersion("v1")
                        .status(OperationStatus.SUCCESS)
                        .startedAt(now)
                        .completedAt(now)
                        .normalizedMetrics(Map.of("bytesRewritten", 10))
                        .build();

        store.createOperation(record);

        Optional<OperationRecord> found = store.findById(id);
        assertTrue(found.isPresent());
        assertEquals("SPARK", found.get().engineType());
        assertEquals("exec-1", found.get().executionId());
        assertEquals("schedule-1", found.get().scheduleId());
        assertEquals("v1", found.get().policyVersion());
    }

    @Test
    void normalizedMetricsJsonRoundTrip() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();

        OperationRecord record =
                OperationRecord.builder()
                        .id(id)
                        .catalog("demo")
                        .namespace("db")
                        .tableName("table")
                        .status(OperationStatus.SUCCESS)
                        .startedAt(now)
                        .completedAt(now)
                        .normalizedMetrics(Map.of("bytesRewritten", 42))
                        .build();

        store.createOperation(record);

        Optional<OperationRecord> found = store.findById(id);
        assertTrue(found.isPresent());
        assertNotNull(found.get().normalizedMetrics());
        assertEquals(
                42, ((Number) found.get().normalizedMetrics().get("bytesRewritten")).intValue());
    }
}
