package com.floe.core.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ScheduleExecutionRecord")
class ScheduleExecutionRecordTest {

    private static final String POLICY_ID = "policy-123";
    private static final String OPERATION_TYPE = "REWRITE_DATA_FILES";
    private static final String TABLE_KEY = "catalog.namespace.table";

    @Nested
    @DisplayName("compositeKey")
    class CompositeKeyTests {

        @Test
        @DisplayName("should create composite key from policy, operation, and table")
        void shouldCreateCompositeKey() {
            ScheduleExecutionRecord record =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .build();

            assertThat(record.compositeKey())
                    .isEqualTo("policy-123:REWRITE_DATA_FILES:catalog.namespace.table");
        }
    }

    @Nested
    @DisplayName("isDue")
    class IsDueTests {

        @Test
        @DisplayName("should be due when nextRunAt is null")
        void shouldBeDueWhenNextRunAtIsNull() {
            ScheduleExecutionRecord record =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .lastRunAt(Instant.now())
                            .nextRunAt(null)
                            .build();

            assertThat(record.isDue()).isTrue();
        }

        @Test
        @DisplayName("should be due when nextRunAt is in the past")
        void shouldBeDueWhenNextRunAtIsInPast() {
            Instant pastTime = Instant.now().minusSeconds(3600);
            ScheduleExecutionRecord record =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .lastRunAt(pastTime.minusSeconds(3600))
                            .nextRunAt(pastTime)
                            .build();

            assertThat(record.isDue()).isTrue();
        }

        @Test
        @DisplayName("should not be due when nextRunAt is in the future")
        void shouldNotBeDueWhenNextRunAtIsInFuture() {
            Instant futureTime = Instant.now().plusSeconds(3600);
            ScheduleExecutionRecord record =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .lastRunAt(Instant.now())
                            .nextRunAt(futureTime)
                            .build();

            assertThat(record.isDue()).isFalse();
        }

        @Test
        @DisplayName("should check due status at specific time")
        void shouldCheckDueStatusAtSpecificTime() {
            Instant checkTime = Instant.parse("2025-01-15T12:00:00Z");
            Instant nextRun = Instant.parse("2025-01-15T10:00:00Z");

            ScheduleExecutionRecord record =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .nextRunAt(nextRun)
                            .build();

            assertThat(record.isDue(checkTime)).isTrue();
        }

        @Test
        @DisplayName("should not be due before nextRunAt time")
        void shouldNotBeDueBeforeNextRunAt() {
            Instant checkTime = Instant.parse("2025-01-15T08:00:00Z");
            Instant nextRun = Instant.parse("2025-01-15T10:00:00Z");

            ScheduleExecutionRecord record =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .nextRunAt(nextRun)
                            .build();

            assertThat(record.isDue(checkTime)).isFalse();
        }
    }

    @Nested
    @DisplayName("withNewRun")
    class WithNewRunTests {

        @Test
        @DisplayName("should create new record with updated run times")
        void shouldCreateNewRecordWithUpdatedRunTimes() {
            Instant originalLastRun = Instant.parse("2025-01-14T10:00:00Z");
            Instant originalNextRun = Instant.parse("2025-01-15T10:00:00Z");

            ScheduleExecutionRecord original =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .lastRunAt(originalLastRun)
                            .nextRunAt(originalNextRun)
                            .build();

            Instant newLastRun = Instant.parse("2025-01-15T10:00:00Z");
            Instant newNextRun = Instant.parse("2025-01-16T10:00:00Z");

            ScheduleExecutionRecord updated = original.withNewRun(newLastRun, newNextRun);

            assertThat(updated.policyId()).isEqualTo(POLICY_ID);
            assertThat(updated.operationType()).isEqualTo(OPERATION_TYPE);
            assertThat(updated.tableKey()).isEqualTo(TABLE_KEY);
            assertThat(updated.lastRunAt()).isEqualTo(newLastRun);
            assertThat(updated.nextRunAt()).isEqualTo(newNextRun);
        }

        @Test
        @DisplayName("should not modify original record")
        void shouldNotModifyOriginalRecord() {
            Instant originalLastRun = Instant.parse("2025-01-14T10:00:00Z");
            Instant originalNextRun = Instant.parse("2025-01-15T10:00:00Z");

            ScheduleExecutionRecord original =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .lastRunAt(originalLastRun)
                            .nextRunAt(originalNextRun)
                            .build();

            original.withNewRun(Instant.now(), Instant.now().plusSeconds(3600));

            assertThat(original.lastRunAt()).isEqualTo(originalLastRun);
            assertThat(original.nextRunAt()).isEqualTo(originalNextRun);
        }
    }

    @Nested
    @DisplayName("builder")
    class BuilderTests {

        @Test
        @DisplayName("should build record with all fields")
        void shouldBuildRecordWithAllFields() {
            Instant lastRun = Instant.now();
            Instant nextRun = lastRun.plusSeconds(86400);

            ScheduleExecutionRecord record =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .lastRunAt(lastRun)
                            .nextRunAt(nextRun)
                            .build();

            assertThat(record.policyId()).isEqualTo(POLICY_ID);
            assertThat(record.operationType()).isEqualTo(OPERATION_TYPE);
            assertThat(record.tableKey()).isEqualTo(TABLE_KEY);
            assertThat(record.lastRunAt()).isEqualTo(lastRun);
            assertThat(record.nextRunAt()).isEqualTo(nextRun);
        }

        @Test
        @DisplayName("should build record with null optional fields")
        void shouldBuildRecordWithNullOptionalFields() {
            ScheduleExecutionRecord record =
                    ScheduleExecutionRecord.builder()
                            .policyId(POLICY_ID)
                            .operationType(OPERATION_TYPE)
                            .tableKey(TABLE_KEY)
                            .build();

            assertThat(record.lastRunAt()).isNull();
            assertThat(record.nextRunAt()).isNull();
        }
    }
}
