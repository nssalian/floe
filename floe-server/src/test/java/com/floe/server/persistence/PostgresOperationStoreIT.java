package com.floe.server.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.floe.core.operation.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.*;

/** Integration tests for PostgresOperationStore. */
@Tag("integration")
@DisplayName("PostgresOperationStore Integration Tests")
class PostgresOperationStoreIT extends PostgresTestBase {

    private PostgresOperationStore store;

    @BeforeEach
    void setUp() {
        store = new PostgresOperationStore(getDataSource());
        store.initializeSchema();
        store.clear();
    }

    @Nested
    @DisplayName("create and retrieve")
    class CreateAndRetrieve {

        @Test
        @DisplayName("should create and retrieve operation by ID")
        void shouldCreateAndRetrieveById() {
            OperationRecord record = createTestRecord("test-catalog", "test-ns", "test-table");

            OperationRecord created = store.createOperation(record);
            Optional<OperationRecord> retrieved = store.findById(created.id());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().catalog()).isEqualTo("test-catalog");
            assertThat(retrieved.get().namespace()).isEqualTo("test-ns");
            assertThat(retrieved.get().tableName()).isEqualTo("test-table");
            assertThat(retrieved.get().status()).isEqualTo(OperationStatus.PENDING);
        }

        @Test
        @DisplayName("should generate ID if not provided")
        void shouldGenerateIdIfNotProvided() {
            OperationRecord record =
                    OperationRecord.builder()
                            .catalog("catalog")
                            .namespace("ns")
                            .tableName("table")
                            .status(OperationStatus.PENDING)
                            .startedAt(Instant.now())
                            .build();

            OperationRecord created = store.createOperation(record);

            assertThat(created.id()).isNotNull();
        }

        @Test
        @DisplayName("should return empty when not found")
        void shouldReturnEmptyWhenNotFound() {
            Optional<OperationRecord> retrieved = store.findById(UUID.randomUUID());

            assertThat(retrieved).isEmpty();
        }
    }

    @Nested
    @DisplayName("status updates")
    class StatusUpdates {

        @Test
        @DisplayName("should update status with results")
        void shouldUpdateStatusWithResults() {
            OperationRecord record = createTestRecord("cat", "ns", "table");
            OperationRecord created = store.createOperation(record);

            OperationResults results =
                    new OperationResults(
                            List.of(),
                            Map.of(
                                    "rewrittenFiles",
                                    10,
                                    "addedFiles",
                                    5,
                                    "rewrittenBytes",
                                    1024000));

            store.updateStatus(created.id(), OperationStatus.SUCCESS, results);

            OperationRecord updated = store.findById(created.id()).orElseThrow();
            assertThat(updated.status()).isEqualTo(OperationStatus.SUCCESS);
            assertThat(updated.completedAt()).isNotNull();
            assertThat(updated.results()).isNotNull();
            assertThat(updated.results().aggregatedMetrics()).containsEntry("rewrittenFiles", 10);
        }

        @Test
        @DisplayName("should mark operation as failed")
        void shouldMarkAsFailed() {
            OperationRecord record = createTestRecord("cat", "ns", "table");
            OperationRecord created = store.createOperation(record);

            store.markFailed(created.id(), "Connection timeout");

            Optional<OperationRecord> failed = store.findById(created.id());

            assertThat(failed).isPresent();
            assertThat(failed.get().status()).isEqualTo(OperationStatus.FAILED);
            assertThat(failed.get().errorMessage()).isEqualTo("Connection timeout");
            assertThat(failed.get().completedAt()).isNotNull();
        }

        @Test
        @DisplayName("should mark operation as running")
        void shouldMarkAsRunning() {
            OperationRecord record = createTestRecord("cat", "ns", "table");
            OperationRecord created = store.createOperation(record);

            Optional<OperationRecord> running = store.markRunning(created.id());

            assertThat(running).isPresent();
            assertThat(running.get().status()).isEqualTo(OperationStatus.RUNNING);
        }

        @Test
        @DisplayName("should return empty when updating non-existent operation")
        void shouldReturnEmptyWhenUpdatingNonExistent() {
            UUID nonExistentId = UUID.randomUUID();
            store.updateStatus(nonExistentId, OperationStatus.SUCCESS, null);

            Optional<OperationRecord> updated = store.findById(nonExistentId);
            assertThat(updated).isEmpty();
        }
    }

    @Nested
    @DisplayName("find operations")
    class FindOperations {

        @Test
        @DisplayName("should find by table")
        void shouldFindByTable() {
            store.createOperation(createTestRecord("cat", "ns", "table1"));
            store.createOperation(createTestRecord("cat", "ns", "table1"));
            store.createOperation(createTestRecord("cat", "ns", "table2"));

            List<OperationRecord> results = store.findByTable("cat", "ns", "table1", 10);

            assertThat(results).hasSize(2);
            assertThat(results).allMatch(r -> r.tableName().equals("table1"));
        }

        @Test
        @DisplayName("should find recent operations")
        void shouldFindRecent() {
            for (int i = 0; i < 10; i++) {
                store.createOperation(createTestRecord("cat", "ns", "table" + i));
            }

            List<OperationRecord> recent = store.findRecent(5);

            assertThat(recent).hasSize(5);
        }

        @Test
        @DisplayName("should find recent with pagination")
        void shouldFindRecentWithPagination() {
            for (int i = 0; i < 10; i++) {
                store.createOperation(createTestRecord("cat", "ns", "table" + i));
            }

            List<OperationRecord> page1 = store.findRecent(3, 0);
            List<OperationRecord> page2 = store.findRecent(3, 3);

            assertThat(page1).hasSize(3);
            assertThat(page2).hasSize(3);
            assertThat(page1).doesNotContainAnyElementsOf(page2);
        }

        @Test
        @DisplayName("should find by status")
        void shouldFindByStatus() {
            store.createOperation(createTestRecord("cat", "ns", "t1"));
            OperationRecord running = store.createOperation(createTestRecord("cat", "ns", "t2"));
            store.markRunning(running.id());

            List<OperationRecord> pendingOps = store.findByStatus(OperationStatus.PENDING, 10);
            List<OperationRecord> runningOps = store.findByStatus(OperationStatus.RUNNING, 10);

            assertThat(pendingOps).hasSize(1);
            assertThat(runningOps).hasSize(1);
        }

        @Test
        @DisplayName("should find in time range")
        void shouldFindInTimeRange() {
            Instant now = Instant.now();
            store.createOperation(createTestRecord("cat", "ns", "table"));

            List<OperationRecord> results =
                    store.findInTimeRange(
                            now.minus(Duration.ofMinutes(1)), now.plus(Duration.ofMinutes(1)), 10);

            assertThat(results).hasSize(1);
        }
    }

    @Nested
    @DisplayName("counting")
    class Counting {

        @Test
        @DisplayName("should count by status")
        void shouldCountByStatus() {
            store.createOperation(createTestRecord("cat", "ns", "t1"));
            store.createOperation(createTestRecord("cat", "ns", "t2"));
            OperationRecord r3 = store.createOperation(createTestRecord("cat", "ns", "t3"));
            store.markFailed(r3.id(), "error");

            long pendingCount = store.countByStatus(OperationStatus.PENDING);
            long failedCount = store.countByStatus(OperationStatus.FAILED);

            assertThat(pendingCount).isEqualTo(2);
            assertThat(failedCount).isEqualTo(1);
        }

        @Test
        @DisplayName("should count total operations")
        void shouldCountTotal() {
            store.createOperation(createTestRecord("cat", "ns", "t1"));
            store.createOperation(createTestRecord("cat", "ns", "t2"));
            store.createOperation(createTestRecord("cat", "ns", "t3"));

            long count = store.count();

            assertThat(count).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("statistics")
    class Statistics {

        @Test
        @DisplayName("should get stats for time window")
        void shouldGetStatsForWindow() {
            OperationRecord r1 = store.createOperation(createTestRecord("cat", "ns", "t1"));
            OperationRecord r2 = store.createOperation(createTestRecord("cat", "ns", "t2"));
            store.createOperation(createTestRecord("cat", "ns", "t3")); // r3 stays pending

            store.updateStatus(r1.id(), OperationStatus.SUCCESS, null);
            store.markFailed(r2.id(), "error");

            OperationStats stats = store.getStats(Duration.ofHours(1));

            assertThat(stats.totalOperations()).isEqualTo(3);
            assertThat(stats.successCount()).isEqualTo(1);
            assertThat(stats.failedCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("should get stats for specific table")
        void shouldGetStatsForTable() {
            store.createOperation(createTestRecord("cat", "ns", "target-table"));
            store.createOperation(createTestRecord("cat", "ns", "target-table"));
            store.createOperation(createTestRecord("cat", "ns", "other-table"));

            OperationStats stats =
                    store.getStatsForTable("cat", "ns", "target-table", Duration.ofHours(1));

            assertThat(stats.totalOperations()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("cleanup")
    class Cleanup {

        @Test
        @DisplayName("should delete operations older than duration")
        void shouldDeleteOlderThan() {
            // Create operation with old timestamp
            OperationRecord oldRecord =
                    OperationRecord.builder()
                            .id(UUID.randomUUID())
                            .catalog("cat")
                            .namespace("ns")
                            .tableName("old-table")
                            .status(OperationStatus.SUCCESS)
                            .startedAt(Instant.now().minus(Duration.ofDays(100)))
                            .createdAt(Instant.now().minus(Duration.ofDays(100)))
                            .build();

            store.createOperation(oldRecord);
            store.createOperation(createTestRecord("cat", "ns", "new-table"));

            int deleted = store.deleteOlderThan(Duration.ofDays(30));

            assertThat(deleted).isEqualTo(1);
            assertThat(store.count()).isEqualTo(1);
        }

        @Test
        @DisplayName("should clear all operations")
        void shouldClearAll() {
            store.createOperation(createTestRecord("cat", "ns", "t1"));
            store.createOperation(createTestRecord("cat", "ns", "t2"));

            store.clear();

            assertThat(store.count()).isZero();
        }
    }

    @Nested
    @DisplayName("policy association")
    class PolicyAssociation {

        @Test
        @DisplayName("should store policy name and ID")
        void shouldStorePolicyInfo() {
            UUID policyId = UUID.randomUUID();
            OperationRecord record =
                    OperationRecord.builder()
                            .id(UUID.randomUUID())
                            .catalog("cat")
                            .namespace("ns")
                            .tableName("table")
                            .policyName("daily-compaction")
                            .policyId(policyId)
                            .status(OperationStatus.PENDING)
                            .startedAt(Instant.now())
                            .createdAt(Instant.now())
                            .build();

            store.createOperation(record);
            Optional<OperationRecord> retrieved = store.findById(record.id());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().policyName()).isEqualTo("daily-compaction");
            assertThat(retrieved.get().policyId()).isEqualTo(policyId);
        }
    }

    // Helper Methods

    private OperationRecord createTestRecord(String catalog, String namespace, String tableName) {
        return OperationRecord.builder()
                .id(UUID.randomUUID())
                .catalog(catalog)
                .namespace(namespace)
                .tableName(tableName)
                .status(OperationStatus.PENDING)
                .startedAt(Instant.now())
                .createdAt(Instant.now())
                .build();
    }
}
