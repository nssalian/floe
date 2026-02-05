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

package com.floe.server.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthIssue;
import com.floe.core.health.HealthReport;
import com.floe.core.health.PostgresTableHealthStore;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.*;

/** Integration tests for PostgresTableHealthStore. */
@Tag("integration")
@DisplayName("PostgresTableHealthStore Integration Tests")
class PostgresTableHealthStoreIT extends PostgresTestBase {

    private PostgresTableHealthStore store;

    private static final TableIdentifier TABLE_1 =
            new TableIdentifier("test-catalog", "test-ns", "table1");
    private static final TableIdentifier TABLE_2 =
            new TableIdentifier("test-catalog", "test-ns", "table2");

    @BeforeEach
    void setUp() {
        store = new PostgresTableHealthStore(getDataSource());
        store.initializeSchema();
        store.clear();
    }

    @Nested
    @DisplayName("schema initialization")
    class SchemaInitialization {

        @Test
        @DisplayName("should create table and indices")
        void shouldCreateTableAndIndices() {
            // initializeSchema is called in setUp, just verify it doesn't throw
            assertThat(store.count()).isZero();

            assertThat(indexExists("idx_health_reports_table")).isTrue();
            assertThat(indexExists("idx_health_reports_assessed_at")).isTrue();
        }

        @Test
        @DisplayName("should be idempotent")
        void shouldBeIdempotent() {
            // Call initializeSchema multiple times - should not throw
            store.initializeSchema();
            store.initializeSchema();

            assertThat(store.count()).isZero();

            assertThat(indexExists("idx_health_reports_table")).isTrue();
            assertThat(indexExists("idx_health_reports_assessed_at")).isTrue();
        }
    }

    @Nested
    @DisplayName("save and retrieve")
    class SaveAndRetrieve {

        @Test
        @DisplayName("should save and retrieve health report")
        void shouldSaveAndRetrieve() {
            HealthReport report = createTestReport(TABLE_1, 100, 10);

            store.save(report);

            List<HealthReport> reports =
                    store.findHistory(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table(), 10);

            assertThat(reports).hasSize(1);
            assertThat(reports.get(0).tableIdentifier()).isEqualTo(TABLE_1);
            assertThat(reports.get(0).dataFileCount()).isEqualTo(100);
            assertThat(reports.get(0).smallFileCount()).isEqualTo(10);
        }

        @Test
        @DisplayName("should preserve all report fields in JSONB")
        void shouldPreserveAllFields() {
            Instant now = Instant.now();
            HealthReport report =
                    HealthReport.builder(TABLE_1)
                            .assessedAt(now)
                            .snapshotCount(5)
                            .oldestSnapshotTimestamp(now.minus(7, ChronoUnit.DAYS))
                            .newestSnapshotTimestamp(now.minus(1, ChronoUnit.HOURS))
                            .dataFileCount(100)
                            .totalDataSizeBytes(1024 * 1024 * 1024L)
                            .minFileSizeBytes(1024 * 1024)
                            .maxFileSizeBytes(512 * 1024 * 1024)
                            .avgFileSizeBytes(128 * 1024 * 1024)
                            .smallFileCount(10)
                            .largeFileCount(5)
                            .deleteFileCount(3)
                            .positionDeleteFileCount(2)
                            .equalityDeleteFileCount(1)
                            .manifestCount(8)
                            .totalManifestSizeBytes(50 * 1024 * 1024)
                            .partitionCount(50)
                            .issues(
                                    List.of(
                                            HealthIssue.warning(
                                                    HealthIssue.Type.TOO_MANY_SMALL_FILES,
                                                    "10% small files")))
                            .build();

            store.save(report);

            List<HealthReport> reports =
                    store.findHistory(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table(), 10);

            assertThat(reports).hasSize(1);
            HealthReport retrieved = reports.get(0);

            assertThat(retrieved.snapshotCount()).isEqualTo(5);
            assertThat(retrieved.dataFileCount()).isEqualTo(100);
            assertThat(retrieved.totalDataSizeBytes()).isEqualTo(1024 * 1024 * 1024L);
            assertThat(retrieved.minFileSizeBytes()).isEqualTo(1024 * 1024);
            assertThat(retrieved.maxFileSizeBytes()).isEqualTo(512 * 1024 * 1024);
            assertThat(retrieved.avgFileSizeBytes()).isEqualTo(128 * 1024 * 1024);
            assertThat(retrieved.smallFileCount()).isEqualTo(10);
            assertThat(retrieved.largeFileCount()).isEqualTo(5);
            assertThat(retrieved.deleteFileCount()).isEqualTo(3);
            assertThat(retrieved.positionDeleteFileCount()).isEqualTo(2);
            assertThat(retrieved.equalityDeleteFileCount()).isEqualTo(1);
            assertThat(retrieved.manifestCount()).isEqualTo(8);
            assertThat(retrieved.totalManifestSizeBytes()).isEqualTo(50 * 1024 * 1024);
            assertThat(retrieved.partitionCount()).isEqualTo(50);
            assertThat(retrieved.issues()).hasSize(1);
            assertThat(retrieved.issues().get(0).type())
                    .isEqualTo(HealthIssue.Type.TOO_MANY_SMALL_FILES);
        }
    }

    @Nested
    @DisplayName("find history")
    class FindHistory {

        @Test
        @DisplayName("should filter by table")
        void shouldFilterByTable() {
            store.save(createTestReport(TABLE_1, 100, 10));
            store.save(createTestReport(TABLE_1, 110, 8));
            store.save(createTestReport(TABLE_2, 200, 20));

            List<HealthReport> table1History =
                    store.findHistory(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table(), 10);
            List<HealthReport> table2History =
                    store.findHistory(TABLE_2.catalog(), TABLE_2.namespace(), TABLE_2.table(), 10);

            assertThat(table1History).hasSize(2);
            assertThat(table1History).allMatch(r -> r.tableIdentifier().equals(TABLE_1));
            assertThat(table2History).hasSize(1);
            assertThat(table2History).allMatch(r -> r.tableIdentifier().equals(TABLE_2));
        }

        @Test
        @DisplayName("should order by assessed_at descending")
        void shouldOrderByAssessedAtDescending() {
            Instant now = Instant.now();
            store.save(
                    HealthReport.builder(TABLE_1)
                            .assessedAt(now.minus(2, ChronoUnit.HOURS))
                            .dataFileCount(100)
                            .build());
            store.save(
                    HealthReport.builder(TABLE_1)
                            .assessedAt(now.minus(1, ChronoUnit.HOURS))
                            .dataFileCount(110)
                            .build());
            store.save(HealthReport.builder(TABLE_1).assessedAt(now).dataFileCount(120).build());

            List<HealthReport> history =
                    store.findHistory(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table(), 10);

            assertThat(history).hasSize(3);
            // Most recent first
            assertThat(history.get(0).dataFileCount()).isEqualTo(120);
            assertThat(history.get(1).dataFileCount()).isEqualTo(110);
            assertThat(history.get(2).dataFileCount()).isEqualTo(100);
        }

        @Test
        @DisplayName("should respect limit")
        void shouldRespectLimit() {
            for (int i = 0; i < 20; i++) {
                store.save(createTestReport(TABLE_1, 100 + i, 10));
            }

            List<HealthReport> history =
                    store.findHistory(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table(), 5);

            assertThat(history).hasSize(5);
        }

        @Test
        @DisplayName("should return empty for unknown table")
        void shouldReturnEmptyForUnknownTable() {
            store.save(createTestReport(TABLE_1, 100, 10));

            List<HealthReport> history = store.findHistory("unknown", "unknown", "unknown", 10);

            assertThat(history).isEmpty();
        }
    }

    @Nested
    @DisplayName("find latest")
    class FindLatest {

        @Test
        @DisplayName("should return latest report per table")
        void shouldReturnLatestPerTable() {
            Instant now = Instant.now();

            // Add multiple reports for table1
            store.save(
                    HealthReport.builder(TABLE_1)
                            .assessedAt(now.minus(2, ChronoUnit.HOURS))
                            .dataFileCount(100)
                            .build());
            store.save(HealthReport.builder(TABLE_1).assessedAt(now).dataFileCount(120).build());

            // Add report for table2
            store.save(
                    HealthReport.builder(TABLE_2)
                            .assessedAt(now.minus(1, ChronoUnit.HOURS))
                            .dataFileCount(200)
                            .build());

            List<HealthReport> latest = store.findLatest(10);

            // Should return 2 reports (one per table), the latest for each
            assertThat(latest).hasSize(2);
        }

        @Test
        @DisplayName("should order latest reports by assessed_at descending")
        void shouldOrderLatestReportsByAssessedAtDescending() {
            Instant now = Instant.now();
            TableIdentifier tableA = new TableIdentifier("catalog", "ns", "tableA");
            TableIdentifier tableB = new TableIdentifier("catalog", "ns", "tableB");

            store.save(
                    HealthReport.builder(tableA)
                            .assessedAt(now.minus(2, ChronoUnit.HOURS))
                            .build());
            store.save(
                    HealthReport.builder(tableB)
                            .assessedAt(now.minus(1, ChronoUnit.HOURS))
                            .build());
            store.save(HealthReport.builder(tableA).assessedAt(now).build());

            List<HealthReport> latest = store.findLatest(10);

            assertThat(latest).hasSize(2);
            assertThat(latest.get(0).assessedAt()).isAfterOrEqualTo(latest.get(1).assessedAt());
        }

        @Test
        @DisplayName("should respect limit")
        void shouldRespectLimit() {
            for (int i = 0; i < 10; i++) {
                TableIdentifier table = new TableIdentifier("catalog", "ns", "table" + i);
                store.save(createTestReport(table, 100, 10));
            }

            List<HealthReport> latest = store.findLatest(5);

            assertThat(latest).hasSize(5);
        }
    }

    @Nested
    @DisplayName("find latest for table")
    class FindLatestForTable {

        @Test
        @DisplayName("should return most recent report")
        void shouldReturnMostRecent() {
            Instant now = Instant.now();
            store.save(
                    HealthReport.builder(TABLE_1)
                            .assessedAt(now.minus(1, ChronoUnit.HOURS))
                            .dataFileCount(100)
                            .build());
            store.save(HealthReport.builder(TABLE_1).assessedAt(now).dataFileCount(150).build());

            Optional<HealthReport> latest =
                    store.findLatestForTable(
                            TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table());

            assertThat(latest).isPresent();
            assertThat(latest.get().dataFileCount()).isEqualTo(150);
        }

        @Test
        @DisplayName("should return empty when no reports exist")
        void shouldReturnEmptyWhenNoReports() {
            Optional<HealthReport> latest =
                    store.findLatestForTable(
                            TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table());

            assertThat(latest).isEmpty();
        }
    }

    @Nested
    @DisplayName("counting and cleanup")
    class CountingAndCleanup {

        @Test
        @DisplayName("should count total reports")
        void shouldCountTotal() {
            store.save(createTestReport(TABLE_1, 100, 10));
            store.save(createTestReport(TABLE_1, 110, 8));
            store.save(createTestReport(TABLE_2, 200, 20));

            assertThat(store.count()).isEqualTo(3);
        }

        @Test
        @DisplayName("should clear all reports")
        void shouldClearAll() {
            store.save(createTestReport(TABLE_1, 100, 10));
            store.save(createTestReport(TABLE_2, 200, 20));

            store.clear();

            assertThat(store.count()).isZero();
        }
    }

    // Helper methods

    private HealthReport createTestReport(
            TableIdentifier tableId, int dataFileCount, int smallFileCount) {
        return HealthReport.builder(tableId)
                .assessedAt(Instant.now())
                .dataFileCount(dataFileCount)
                .smallFileCount(smallFileCount)
                .build();
    }

    private boolean indexExists(String indexName) {
        String sql =
                "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'table_health_reports' AND indexname = ?";
        try (var conn = getDataSource().getConnection();
                var stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, indexName);
            try (var rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to check index: " + indexName, e);
        }
    }
}
