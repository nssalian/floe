package com.floe.core.operation;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryOperationStoreTest {

    private InMemoryOperationStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryOperationStore();
    }

    private OperationRecord createTestRecord(String catalog, String namespace, String tableName) {
        return OperationRecord.builder()
                .catalog(catalog)
                .namespace(namespace)
                .tableName(tableName)
                .status(OperationStatus.PENDING)
                .build();
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
    void shouldGenerateIdIfNotProvided() {
        OperationRecord record =
                OperationRecord.builder()
                        .id(null)
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
                        .createdAt(null)
                        .build();

        OperationRecord created = store.createOperation(record);

        assertNotNull(created.createdAt());
        assertFalse(created.createdAt().isBefore(before));
    }

    @Test
    void shouldPreserveProvidedId() {
        UUID id = UUID.randomUUID();
        OperationRecord record =
                OperationRecord.builder()
                        .id(id)
                        .catalog("demo")
                        .namespace("test")
                        .tableName("events")
                        .build();

        OperationRecord created = store.createOperation(record);

        assertEquals(id, created.id());
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
                            assertNull(updated.completedAt());
                        });
    }

    @Test
    void shouldSetResultsWhenProvided() {
        OperationRecord record = createTestRecord("demo", "test", "events");
        OperationRecord created = store.createOperation(record);
        OperationResults results = OperationResults.empty();

        store.updateStatus(created.id(), OperationStatus.SUCCESS, results);

        store.findById(created.id())
                .ifPresent(
                        updated -> {
                            assertEquals(results, updated.results());
                        });
    }

    @Test
    void shouldReturnEmptyWhenUpdatingNonexistent() {
        UUID id = UUID.randomUUID();
        store.updateStatus(id, OperationStatus.SUCCESS, null);

        store.findById(id)
                .ifPresentOrElse(
                        updated -> fail("Should not have found a record"), () -> assertTrue(true));
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
                            assertNotNull(updated.completedAt());
                        });
    }

    @Test
    void shouldReturnEmptyWhenMarkingNonexistentAsFailed() {
        UUID id = UUID.randomUUID();
        store.markFailed(id, "error");

        store.findById(id)
                .ifPresentOrElse(
                        updated -> fail("Should not have found a record"), () -> assertTrue(true));
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

    @Test
    void shouldReturnEmptyListWhenNoMatchingTable() {
        store.createOperation(createTestRecord("demo", "test", "events"));

        List<OperationRecord> results = store.findByTable("demo", "test", "users", 10);

        assertTrue(results.isEmpty());
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
    void shouldRespectLimitInFindRecent() {
        for (int i = 0; i < 10; i++) {
            store.createOperation(createTestRecord("demo", "test", "table" + i));
        }

        List<OperationRecord> results = store.findRecent(5);

        assertEquals(5, results.size());
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

    @Test
    void shouldExcludeRecordsOutsideTimeRange() {
        Instant now = Instant.now();
        Instant start = now.plus(1, ChronoUnit.HOURS);
        Instant end = now.plus(2, ChronoUnit.HOURS);

        store.createOperation(createTestRecord("demo", "test", "events"));

        List<OperationRecord> results = store.findInTimeRange(start, end, 10);

        assertTrue(results.isEmpty());
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
        // Create an old record
        OperationRecord oldRecord =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("test")
                        .tableName("old-table")
                        .createdAt(Instant.now().minus(10, ChronoUnit.DAYS))
                        .build();
        store.createOperation(oldRecord);

        // Create a new record
        store.createOperation(createTestRecord("demo", "test", "new-table"));

        int deleted = store.deleteOlderThan(Duration.ofDays(5));

        assertEquals(1, deleted);
        assertEquals(1, store.count());
    }

    @Test
    void shouldNotDeleteRecentRecords() {
        store.createOperation(createTestRecord("demo", "test", "t1"));
        store.createOperation(createTestRecord("demo", "test", "t2"));

        int deleted = store.deleteOlderThan(Duration.ofDays(30));

        assertEquals(0, deleted);
        assertEquals(2, store.count());
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

    @Test
    void shouldReturnEmptyStatsForNonexistentTable() {
        store.createOperation(createTestRecord("demo", "test", "events"));

        OperationStats stats =
                store.getStatsForTable("demo", "test", "nonexistent", Duration.ofHours(1));

        assertEquals(0, stats.totalOperations());
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
}
