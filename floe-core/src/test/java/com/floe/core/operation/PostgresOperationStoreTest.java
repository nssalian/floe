/*
 * Copyright 2026 The Floe Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.floe.core.operation;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.maintenance.MaintenanceOperation;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
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
class PostgresOperationStoreTest {

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

    private OperationRecord createTestRecord(String catalog, String namespace, String tableName) {
        return OperationRecord.builder()
                .catalog(catalog)
                .namespace(namespace)
                .tableName(tableName)
                .status(OperationStatus.PENDING)
                .build();
    }

    // initializeSchema Tests

    @Test
    void shouldInitializeSchemaIdempotently() {
        // Schema already initialized in setUp, but calling again should be safe
        assertDoesNotThrow(() -> store.initializeSchema());
    }

    // createOperation Tests

    @Test
    void shouldCreateOperation() {
        OperationRecord record = createTestRecord("demo", "test", "events");

        OperationRecord created = store.createOperation(record);

        assertNotNull(created.id());
        assertEquals("demo", created.catalog());
        assertEquals(1, store.count());
    }

    @Test
    void shouldCreateOperationWithAllFields() {
        UUID id = UUID.randomUUID();
        UUID policyId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        OperationResults.SingleOperationResult opResult =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "SUCCEEDED",
                        5000,
                        Map.of("files", 10),
                        null);
        OperationResults results = new OperationResults(List.of(opResult), Map.of("total", 10));

        OperationRecord record =
                OperationRecord.builder()
                        .id(id)
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .policyName("cleanup-policy")
                        .policyId(policyId)
                        .status(OperationStatus.SUCCESS)
                        .startedAt(now)
                        .completedAt(now.plus(5, ChronoUnit.MINUTES))
                        .results(results)
                        .errorMessage(null)
                        .createdAt(now)
                        .build();

        store.createOperation(record);

        Optional<OperationRecord> found = store.findById(id);
        assertTrue(found.isPresent());
        assertEquals("cleanup-policy", found.get().policyName());
        assertEquals(policyId, found.get().policyId());
        assertNotNull(found.get().results());
        assertEquals(1, found.get().results().operations().size());
    }

    @Test
    void shouldGenerateIdIfNotProvided() {
        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .build();

        OperationRecord created = store.createOperation(record);

        assertNotNull(created.id());
    }

    @Test
    void shouldSetCreatedAtIfNotProvided() {
        Instant before = Instant.now();

        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .build();

        OperationRecord created = store.createOperation(record);

        assertNotNull(created.createdAt());
        assertFalse(created.createdAt().isBefore(before.minusSeconds(1)));
    }

    // updateStatus Tests

    @Test
    void shouldUpdateStatus() {
        OperationRecord record = createTestRecord("demo", "test", "events");
        OperationRecord created = store.createOperation(record);

        store.updateStatus(created.id(), OperationStatus.SUCCESS, OperationResults.empty());

        store.findById(created.id())
                .ifPresent(
                        updated -> {
                            assertEquals(OperationStatus.SUCCESS, updated.status());
                            assertNotNull(updated.results());
                        });
    }

    @Test
    void shouldSetCompletedAtWhenTerminal() {
        OperationRecord record = createTestRecord("demo", "test", "events");
        OperationRecord created = store.createOperation(record);

        store.updateStatus(created.id(), OperationStatus.SUCCESS, null);

        store.findById(created.id())
                .ifPresent(
                        updated -> {
                            assertEquals(OperationStatus.SUCCESS, updated.status());
                            assertNotNull(updated.completedAt());
                        });
    }

    @Test
    void shouldNotSetCompletedAtWhenNotTerminal() {
        OperationRecord record = createTestRecord("demo", "test", "events");
        OperationRecord created = store.createOperation(record);

        store.updateStatus(created.id(), OperationStatus.RUNNING, null);

        store.findById(created.id())
                .ifPresent(
                        updated -> {
                            assertEquals(OperationStatus.RUNNING, updated.status());
                            assertNull(updated.completedAt());
                        });
    }

    @Test
    void shouldReturnEmptyWhenUpdatingNonexistent() {
        UUID id = UUID.randomUUID();
        store.updateStatus(id, OperationStatus.SUCCESS, null);

        store.findById(id)
                .ifPresent(
                        updated -> {
                            fail("Should not find any record");
                        });
    }

    // markFailed Tests

    @Test
    void shouldMarkFailed() {
        OperationRecord record = createTestRecord("demo", "test", "events");
        OperationRecord created = store.createOperation(record);

        store.markFailed(created.id(), "Something went wrong");

        store.findById(created.id())
                .ifPresent(
                        updated -> {
                            assertEquals(OperationStatus.FAILED, updated.status());
                            assertEquals("Something went wrong", updated.errorMessage());
                            assertNotNull(updated.completedAt());
                        });
    }

    @Test
    void shouldReturnEmptyWhenMarkingNonexistentAsFailed() {
        UUID id = UUID.randomUUID();
        store.markFailed(id, "error");

        store.findById(id)
                .ifPresent(
                        updated -> {
                            fail("Should not find any record");
                        });
    }

    // markRunning Tests

    @Test
    void shouldMarkRunning() {
        OperationRecord record = createTestRecord("demo", "test", "events");
        OperationRecord created = store.createOperation(record);

        Optional<OperationRecord> running = store.markRunning(created.id());

        assertTrue(running.isPresent());
        assertEquals(OperationStatus.RUNNING, running.get().status());
        assertNotNull(running.get().startedAt());
    }

    @Test
    void shouldReturnEmptyWhenMarkingNonexistentAsRunning() {
        Optional<OperationRecord> running = store.markRunning(UUID.randomUUID());

        assertTrue(running.isEmpty());
    }

    // findById Tests

    @Test
    void shouldFindById() {
        OperationRecord record = createTestRecord("demo", "test", "events");
        OperationRecord created = store.createOperation(record);

        Optional<OperationRecord> found = store.findById(created.id());

        assertTrue(found.isPresent());
        assertEquals(created.id(), found.get().id());
    }

    @Test
    void shouldReturnEmptyWhenNotFound() {
        Optional<OperationRecord> found = store.findById(UUID.randomUUID());

        assertTrue(found.isEmpty());
    }

    // findByTable Tests

    @Test
    void shouldFindByTable() {
        store.createOperation(createTestRecord("demo", "test", "events"));
        store.createOperation(createTestRecord("demo", "test", "events"));
        store.createOperation(createTestRecord("demo", "test", "users"));

        List<OperationRecord> results = store.findByTable("demo", "test", "events", 10);

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(r -> r.tableName().equals("events")));
    }

    @Test
    void shouldRespectLimitInFindByTable() {
        for (int i = 0; i < 10; i++) {
            store.createOperation(createTestRecord("demo", "test", "events"));
        }

        List<OperationRecord> results = store.findByTable("demo", "test", "events", 5);

        assertEquals(5, results.size());
    }

    // findRecent Tests

    @Test
    void shouldFindRecent() {
        store.createOperation(createTestRecord("demo", "test", "events"));
        store.createOperation(createTestRecord("demo", "test", "users"));
        store.createOperation(createTestRecord("demo", "test", "orders"));

        List<OperationRecord> results = store.findRecent(10);

        assertEquals(3, results.size());
    }

    @Test
    void shouldFindRecentWithOffset() {
        for (int i = 0; i < 10; i++) {
            store.createOperation(createTestRecord("demo", "test", "table" + i));
        }

        List<OperationRecord> results = store.findRecent(5, 3);

        assertEquals(5, results.size());
    }

    // findByStatus Tests

    @Test
    void shouldFindByStatus() {
        store.createOperation(createTestRecord("demo", "test", "t1"));
        OperationRecord running = store.createOperation(createTestRecord("demo", "test", "t2"));
        store.markRunning(running.id());

        List<OperationRecord> pendingRecords = store.findByStatus(OperationStatus.PENDING, 10);
        List<OperationRecord> runningRecords = store.findByStatus(OperationStatus.RUNNING, 10);

        assertEquals(1, pendingRecords.size());
        assertEquals(1, runningRecords.size());
    }

    @Test
    void shouldFindByStatusWithOffset() {
        for (int i = 0; i < 10; i++) {
            store.createOperation(createTestRecord("demo", "test", "table" + i));
        }

        List<OperationRecord> results = store.findByStatus(OperationStatus.PENDING, 5, 2);

        assertEquals(5, results.size());
    }

    // countByStatus Tests

    @Test
    void shouldCountByStatus() {
        store.createOperation(createTestRecord("demo", "test", "t1"));
        store.createOperation(createTestRecord("demo", "test", "t2"));
        OperationRecord r3 = store.createOperation(createTestRecord("demo", "test", "t3"));
        store.markRunning(r3.id());

        assertEquals(2, store.countByStatus(OperationStatus.PENDING));
        assertEquals(1, store.countByStatus(OperationStatus.RUNNING));
        assertEquals(0, store.countByStatus(OperationStatus.SUCCESS));
    }

    // findInTimeRange Tests

    @Test
    void shouldFindInTimeRange() {
        Instant now = Instant.now();
        Instant start = now.minus(1, ChronoUnit.HOURS);
        Instant end = now.plus(1, ChronoUnit.HOURS);

        store.createOperation(createTestRecord("demo", "test", "events"));

        List<OperationRecord> results = store.findInTimeRange(start, end, 10);

        assertEquals(1, results.size());
    }

    // count Tests

    @Test
    void shouldReturnZeroCountWhenEmpty() {
        assertEquals(0, store.count());
    }

    @Test
    void shouldReturnCorrectCount() {
        store.createOperation(createTestRecord("demo", "test", "t1"));
        store.createOperation(createTestRecord("demo", "test", "t2"));
        store.createOperation(createTestRecord("demo", "test", "t3"));

        assertEquals(3, store.count());
    }

    // clear Tests

    @Test
    void shouldClearAllRecords() {
        store.createOperation(createTestRecord("demo", "test", "t1"));
        store.createOperation(createTestRecord("demo", "test", "t2"));

        store.clear();

        assertEquals(0, store.count());
    }

    // deleteOlderThan Tests

    @Test
    void shouldDeleteOlderThan() {
        // Create an old record by manipulating startedAt
        OperationRecord oldRecord =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("old-table")
                        .startedAt(Instant.now().minus(10, ChronoUnit.DAYS))
                        .createdAt(Instant.now().minus(10, ChronoUnit.DAYS))
                        .build();
        store.createOperation(oldRecord);

        // Create a new record
        store.createOperation(createTestRecord("demo", "test", "new-table"));

        int deleted = store.deleteOlderThan(Duration.ofDays(5));

        assertEquals(1, deleted);
        assertEquals(1, store.count());
    }

    // getStats Tests

    @Test
    void shouldGetStats() {
        store.createOperation(createTestRecord("demo", "test", "t1"));
        OperationRecord r2 = store.createOperation(createTestRecord("demo", "test", "t2"));
        store.updateStatus(r2.id(), OperationStatus.SUCCESS, null);
        OperationRecord r3 = store.createOperation(createTestRecord("demo", "test", "t3"));
        store.markFailed(r3.id(), "error");

        OperationStats stats = store.getStats(Duration.ofHours(1));

        assertEquals(3, stats.totalOperations());
        assertEquals(1, stats.successCount());
        assertEquals(1, stats.failedCount());
        assertEquals(1, stats.runningCount()); // PENDING counts as running
    }

    @Test
    void shouldGetEmptyStatsWhenNoRecords() {
        OperationStats stats = store.getStats(Duration.ofHours(1));

        assertEquals(0, stats.totalOperations());
        assertEquals(0, stats.successCount());
    }

    // getStatsForTable Tests

    @Test
    void shouldGetStatsForSpecificTable() {
        store.createOperation(createTestRecord("demo", "test", "events"));
        OperationRecord r2 = store.createOperation(createTestRecord("demo", "test", "events"));
        store.updateStatus(r2.id(), OperationStatus.SUCCESS, null);
        store.createOperation(createTestRecord("demo", "test", "users")); // different table

        OperationStats stats =
                store.getStatsForTable("demo", "test", "events", Duration.ofHours(1));

        assertEquals(2, stats.totalOperations());
        assertEquals(1, stats.successCount());
    }

    // Stats counting by status Tests

    @Test
    void shouldCountPartialFailuresInStats() {
        OperationRecord record = store.createOperation(createTestRecord("demo", "test", "t1"));
        store.updateStatus(record.id(), OperationStatus.PARTIAL_FAILURE, null);

        OperationStats stats = store.getStats(Duration.ofHours(1));

        assertEquals(1, stats.partialFailureCount());
    }

    @Test
    void shouldCountNoPolicyInStats() {
        OperationRecord record = store.createOperation(createTestRecord("demo", "test", "t1"));
        store.updateStatus(record.id(), OperationStatus.NO_POLICY, null);

        OperationStats stats = store.getStats(Duration.ofHours(1));

        assertEquals(1, stats.noPolicyCount());
    }

    @Test
    void shouldCountNoOperationsInStats() {
        OperationRecord record = store.createOperation(createTestRecord("demo", "test", "t1"));
        store.updateStatus(record.id(), OperationStatus.NO_OPERATIONS, null);

        OperationStats stats = store.getStats(Duration.ofHours(1));

        assertEquals(1, stats.noOperationsCount());
    }

    // JSON Serialization Tests

    @Test
    void shouldSerializeAndDeserializeResults() {
        OperationResults.SingleOperationResult opResult =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "SUCCEEDED",
                        5000,
                        Map.of("files_rewritten", 10, "bytes_saved", 1024),
                        null);

        OperationResults results =
                new OperationResults(List.of(opResult), Map.of("total_files", 10));

        OperationRecord record =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .status(OperationStatus.SUCCESS)
                        .results(results)
                        .build();

        OperationRecord created = store.createOperation(record);
        Optional<OperationRecord> found = store.findById(created.id());

        assertTrue(found.isPresent());
        assertNotNull(found.get().results());
        assertEquals(1, found.get().results().operations().size());
        assertEquals(
                MaintenanceOperation.Type.REWRITE_DATA_FILES,
                found.get().results().operations().get(0).operationType());
    }
}
