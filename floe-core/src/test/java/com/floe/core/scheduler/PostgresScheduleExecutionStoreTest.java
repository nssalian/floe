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

package com.floe.core.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration")
@Testcontainers
@DisplayName("PostgresScheduleExecutionStore")
class PostgresScheduleExecutionStoreTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("floe_test")
                    .withUsername("test")
                    .withPassword("test");

    private static HikariDataSource dataSource;
    private PostgresScheduleExecutionStore store;

    @BeforeAll
    static void setUpDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void tearDownDataSource() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @BeforeEach
    void setUp() {
        store = new PostgresScheduleExecutionStore(dataSource);
        store.initializeSchema();
        store.clear();
    }

    @Nested
    @DisplayName("initializeSchema")
    class InitializeSchemaTests {

        @Test
        @DisplayName("should create table and indexes without error")
        void shouldCreateTableAndIndexes() {
            // Schema already initialized in setUp, verify by counting
            assertThat(store.count()).isZero();
        }

        @Test
        @DisplayName("should be idempotent")
        void shouldBeIdempotent() {
            // Call initializeSchema again - should not throw
            store.initializeSchema();
            store.initializeSchema();
            assertThat(store.count()).isZero();
        }
    }

    @Nested
    @DisplayName("recordExecution and getRecord")
    class RecordExecutionTests {

        @Test
        @DisplayName("should record and retrieve execution")
        void shouldRecordAndRetrieveExecution() {
            Instant executedAt = Instant.parse("2025-01-15T10:00:00Z");
            Instant nextRunAt = Instant.parse("2025-01-16T10:00:00Z");

            store.recordExecution(
                    "policy-1", "REWRITE_DATA_FILES", "catalog.ns.table", executedAt, nextRunAt);

            var record = store.getRecord("policy-1", "REWRITE_DATA_FILES", "catalog.ns.table");

            assertThat(record).isPresent();
            assertThat(record.get().policyId()).isEqualTo("policy-1");
            assertThat(record.get().operationType()).isEqualTo("REWRITE_DATA_FILES");
            assertThat(record.get().tableKey()).isEqualTo("catalog.ns.table");
            assertThat(record.get().lastRunAt()).isEqualTo(executedAt);
            assertThat(record.get().nextRunAt()).isEqualTo(nextRunAt);
        }

        @Test
        @DisplayName("should return empty for non-existent record")
        void shouldReturnEmptyForNonExistentRecord() {
            var record = store.getRecord("non-existent", "REWRITE_DATA_FILES", "catalog.ns.table");

            assertThat(record).isEmpty();
        }

        @Test
        @DisplayName("should upsert existing record on re-execution")
        void shouldUpsertExistingRecordOnReExecution() {
            Instant firstRun = Instant.parse("2025-01-15T10:00:00Z");
            Instant firstNextRun = Instant.parse("2025-01-16T10:00:00Z");

            store.recordExecution(
                    "policy-1", "EXPIRE_SNAPSHOTS", "catalog.ns.table", firstRun, firstNextRun);

            Instant secondRun = Instant.parse("2025-01-16T10:00:00Z");
            Instant secondNextRun = Instant.parse("2025-01-17T10:00:00Z");

            store.recordExecution(
                    "policy-1", "EXPIRE_SNAPSHOTS", "catalog.ns.table", secondRun, secondNextRun);

            var record = store.getRecord("policy-1", "EXPIRE_SNAPSHOTS", "catalog.ns.table");

            assertThat(record).isPresent();
            assertThat(record.get().lastRunAt()).isEqualTo(secondRun);
            assertThat(record.get().nextRunAt()).isEqualTo(secondNextRun);
            assertThat(store.count()).isEqualTo(1);
        }

        @Test
        @DisplayName("should store separate records for different operations")
        void shouldStoreSeparateRecordsForDifferentOperations() {
            Instant now = Instant.now();

            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table",
                    now,
                    now.plusSeconds(3600));
            store.recordExecution(
                    "policy-1", "EXPIRE_SNAPSHOTS", "catalog.ns.table", now, now.plusSeconds(7200));

            assertThat(store.count()).isEqualTo(2);

            var rewriteRecord =
                    store.getRecord("policy-1", "REWRITE_DATA_FILES", "catalog.ns.table");
            var expireRecord = store.getRecord("policy-1", "EXPIRE_SNAPSHOTS", "catalog.ns.table");

            assertThat(rewriteRecord).isPresent();
            assertThat(expireRecord).isPresent();
            assertThat(rewriteRecord.get().nextRunAt())
                    .isNotEqualTo(expireRecord.get().nextRunAt());
        }

        @Test
        @DisplayName("should store separate records for different tables")
        void shouldStoreSeparateRecordsForDifferentTables() {
            Instant now = Instant.now();

            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table1",
                    now,
                    now.plusSeconds(3600));
            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table2",
                    now,
                    now.plusSeconds(3600));

            assertThat(store.count()).isEqualTo(2);
        }

        @Test
        @DisplayName("should handle null timestamps")
        void shouldHandleNullTimestamps() {
            store.recordExecution("policy-1", "REWRITE_DATA_FILES", "catalog.ns.table", null, null);

            var record = store.getRecord("policy-1", "REWRITE_DATA_FILES", "catalog.ns.table");

            assertThat(record).isPresent();
            assertThat(record.get().lastRunAt()).isNull();
            assertThat(record.get().nextRunAt()).isNull();
        }
    }

    @Nested
    @DisplayName("findDueRecords")
    class FindDueRecordsTests {

        @Test
        @DisplayName("should find records due before given time")
        void shouldFindRecordsDueBeforeGivenTime() {
            Instant checkTime = Instant.parse("2025-01-15T12:00:00Z");

            // Due (nextRunAt before checkTime)
            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table1",
                    Instant.parse("2025-01-14T10:00:00Z"),
                    Instant.parse("2025-01-15T10:00:00Z"));

            // Due (nextRunAt equals checkTime)
            store.recordExecution(
                    "policy-1",
                    "EXPIRE_SNAPSHOTS",
                    "catalog.ns.table1",
                    Instant.parse("2025-01-14T12:00:00Z"),
                    Instant.parse("2025-01-15T12:00:00Z"));

            // Not due (nextRunAt after checkTime)
            store.recordExecution(
                    "policy-1",
                    "ORPHAN_CLEANUP",
                    "catalog.ns.table1",
                    Instant.parse("2025-01-15T10:00:00Z"),
                    Instant.parse("2025-01-16T10:00:00Z"));

            List<ScheduleExecutionRecord> dueRecords = store.findDueRecords(checkTime);

            assertThat(dueRecords).hasSize(2);
            assertThat(dueRecords)
                    .extracting(ScheduleExecutionRecord::operationType)
                    .containsExactlyInAnyOrder("REWRITE_DATA_FILES", "EXPIRE_SNAPSHOTS");
        }

        @Test
        @DisplayName("should return empty list when no records are due")
        void shouldReturnEmptyListWhenNoRecordsAreDue() {
            Instant checkTime = Instant.parse("2025-01-15T08:00:00Z");

            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table1",
                    Instant.parse("2025-01-14T10:00:00Z"),
                    Instant.parse("2025-01-15T10:00:00Z"));

            List<ScheduleExecutionRecord> dueRecords = store.findDueRecords(checkTime);

            assertThat(dueRecords).isEmpty();
        }

        @Test
        @DisplayName("should include records with null nextRunAt as due")
        void shouldIncludeRecordsWithNullNextRunAtAsDue() {
            Instant checkTime = Instant.now();

            store.recordExecution(
                    "policy-1", "REWRITE_DATA_FILES", "catalog.ns.table1", Instant.now(), null);

            List<ScheduleExecutionRecord> dueRecords = store.findDueRecords(checkTime);

            assertThat(dueRecords).hasSize(1);
        }
    }

    @Nested
    @DisplayName("findByPolicy")
    class FindByPolicyTests {

        @Test
        @DisplayName("should find all records for a policy")
        void shouldFindAllRecordsForPolicy() {
            Instant now = Instant.now();

            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table1",
                    now,
                    now.plusSeconds(3600));
            store.recordExecution(
                    "policy-1",
                    "EXPIRE_SNAPSHOTS",
                    "catalog.ns.table1",
                    now,
                    now.plusSeconds(3600));
            store.recordExecution(
                    "policy-2",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table2",
                    now,
                    now.plusSeconds(3600));

            List<ScheduleExecutionRecord> policy1Records = store.findByPolicy("policy-1");

            assertThat(policy1Records).hasSize(2);
            assertThat(policy1Records).allMatch(record -> record.policyId().equals("policy-1"));
        }

        @Test
        @DisplayName("should return empty list for non-existent policy")
        void shouldReturnEmptyListForNonExistentPolicy() {
            List<ScheduleExecutionRecord> records = store.findByPolicy("non-existent");

            assertThat(records).isEmpty();
        }
    }

    @Nested
    @DisplayName("deleteByPolicy")
    class DeleteByPolicyTests {

        @Test
        @DisplayName("should delete all records for a policy")
        void shouldDeleteAllRecordsForPolicy() {
            Instant now = Instant.now();

            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table1",
                    now,
                    now.plusSeconds(3600));
            store.recordExecution(
                    "policy-1",
                    "EXPIRE_SNAPSHOTS",
                    "catalog.ns.table1",
                    now,
                    now.plusSeconds(3600));
            store.recordExecution(
                    "policy-2",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table2",
                    now,
                    now.plusSeconds(3600));

            int deleted = store.deleteByPolicy("policy-1");

            assertThat(deleted).isEqualTo(2);
            assertThat(store.count()).isEqualTo(1);
            assertThat(store.findByPolicy("policy-1")).isEmpty();
            assertThat(store.findByPolicy("policy-2")).hasSize(1);
        }

        @Test
        @DisplayName("should return zero when deleting non-existent policy")
        void shouldReturnZeroWhenDeletingNonExistentPolicy() {
            int deleted = store.deleteByPolicy("non-existent");

            assertThat(deleted).isZero();
        }
    }

    @Nested
    @DisplayName("deleteByTable")
    class DeleteByTableTests {

        @Test
        @DisplayName("should delete all records for a table")
        void shouldDeleteAllRecordsForTable() {
            Instant now = Instant.now();

            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table1",
                    now,
                    now.plusSeconds(3600));
            store.recordExecution(
                    "policy-1",
                    "EXPIRE_SNAPSHOTS",
                    "catalog.ns.table1",
                    now,
                    now.plusSeconds(3600));
            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table2",
                    now,
                    now.plusSeconds(3600));

            int deleted = store.deleteByTable("catalog.ns.table1");

            assertThat(deleted).isEqualTo(2);
            assertThat(store.count()).isEqualTo(1);
        }

        @Test
        @DisplayName("should return zero when deleting non-existent table")
        void shouldReturnZeroWhenDeletingNonExistentTable() {
            int deleted = store.deleteByTable("non-existent");

            assertThat(deleted).isZero();
        }
    }

    @Nested
    @DisplayName("clear and count")
    class ClearAndCountTests {

        @Test
        @DisplayName("should clear all records")
        void shouldClearAllRecords() {
            Instant now = Instant.now();

            store.recordExecution(
                    "policy-1",
                    "REWRITE_DATA_FILES",
                    "catalog.ns.table1",
                    now,
                    now.plusSeconds(3600));
            store.recordExecution(
                    "policy-2",
                    "EXPIRE_SNAPSHOTS",
                    "catalog.ns.table2",
                    now,
                    now.plusSeconds(3600));

            assertThat(store.count()).isEqualTo(2);

            store.clear();

            assertThat(store.count()).isZero();
        }

        @Test
        @DisplayName("should return zero count for empty store")
        void shouldReturnZeroCountForEmptyStore() {
            assertThat(store.count()).isZero();
        }
    }

    @Nested
    @DisplayName("persistence")
    class PersistenceTests {

        @Test
        @DisplayName("should persist data across store instances")
        void shouldPersistDataAcrossStoreInstances() {
            Instant executedAt = Instant.parse("2025-01-15T10:00:00Z");
            Instant nextRunAt = Instant.parse("2025-01-16T10:00:00Z");

            store.recordExecution(
                    "policy-1", "REWRITE_DATA_FILES", "catalog.ns.table", executedAt, nextRunAt);

            // Create a new store instance with same datasource
            PostgresScheduleExecutionStore newStore =
                    new PostgresScheduleExecutionStore(dataSource);

            var record = newStore.getRecord("policy-1", "REWRITE_DATA_FILES", "catalog.ns.table");

            assertThat(record).isPresent();
            assertThat(record.get().lastRunAt()).isEqualTo(executedAt);
            assertThat(record.get().nextRunAt()).isEqualTo(nextRunAt);
        }
    }
}
