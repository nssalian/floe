package com.floe.core.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

class OperationStatsTest {

    // Record Tests

    @Test
    void shouldCreateStatsWithAllFields() {
        Instant start = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant end = Instant.now();

        OperationStats stats =
                new OperationStats(
                        100, // total
                        80, // success
                        5, // failed
                        10, // partial
                        3, // running
                        1, // noPolicy
                        1, // noOperations
                        start, end);

        assertEquals(100, stats.totalOperations());
        assertEquals(80, stats.successCount());
        assertEquals(5, stats.failedCount());
        assertEquals(10, stats.partialFailureCount());
        assertEquals(3, stats.runningCount());
        assertEquals(1, stats.noPolicyCount());
        assertEquals(1, stats.noOperationsCount());
        assertEquals(start, stats.windowStart());
        assertEquals(end, stats.windowEnd());
    }

    // withFailuresCount Tests

    @Test
    void shouldCalculateWithFailuresCount() {
        OperationStats stats =
                OperationStats.builder().failedCount(5).partialFailureCount(10).build();

        assertEquals(15, stats.withFailuresCount());
    }

    @Test
    void shouldReturnZeroWithFailuresWhenNoFailures() {
        OperationStats stats =
                OperationStats.builder().failedCount(0).partialFailureCount(0).build();

        assertEquals(0, stats.withFailuresCount());
    }

    // completedCount Tests

    @Test
    void shouldCalculateCompletedCount() {
        OperationStats stats =
                OperationStats.builder().totalOperations(100).runningCount(15).build();

        assertEquals(85, stats.completedCount());
    }

    @Test
    void shouldReturnZeroCompletedWhenAllRunning() {
        OperationStats stats =
                OperationStats.builder().totalOperations(10).runningCount(10).build();

        assertEquals(0, stats.completedCount());
    }

    // successRate Tests

    @Test
    void shouldCalculateSuccessRate() {
        OperationStats stats =
                OperationStats.builder()
                        .totalOperations(100)
                        .successCount(80)
                        .runningCount(0)
                        .build();

        assertEquals(80.0, stats.successRate(), 0.001);
    }

    @Test
    void shouldReturnZeroSuccessRateWhenNoCompleted() {
        OperationStats stats =
                OperationStats.builder()
                        .totalOperations(10)
                        .successCount(0)
                        .runningCount(10)
                        .build();

        assertEquals(0.0, stats.successRate(), 0.001);
    }

    @Test
    void shouldReturn100PercentWhenAllSuccessful() {
        OperationStats stats =
                OperationStats.builder()
                        .totalOperations(50)
                        .successCount(50)
                        .runningCount(0)
                        .build();

        assertEquals(100.0, stats.successRate(), 0.001);
    }

    @Test
    void shouldHandlePartialSuccessRate() {
        OperationStats stats =
                OperationStats.builder()
                        .totalOperations(10)
                        .successCount(3)
                        .runningCount(0)
                        .build();

        assertEquals(30.0, stats.successRate(), 0.001);
    }

    // windowDuration Tests

    @Test
    void shouldCalculateWindowDuration() {
        Instant start = Instant.now().minus(2, ChronoUnit.HOURS);
        Instant end = Instant.now();

        OperationStats stats = OperationStats.builder().windowStart(start).windowEnd(end).build();

        Duration duration = stats.windowDuration();
        assertTrue(duration.toMinutes() >= 119 && duration.toMinutes() <= 121);
    }

    // empty() Tests

    @Test
    void shouldCreateEmptyStats() {
        Instant start = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant end = Instant.now();

        OperationStats stats = OperationStats.empty(start, end);

        assertEquals(0, stats.totalOperations());
        assertEquals(0, stats.successCount());
        assertEquals(0, stats.failedCount());
        assertEquals(0, stats.partialFailureCount());
        assertEquals(0, stats.runningCount());
        assertEquals(0, stats.noPolicyCount());
        assertEquals(0, stats.noOperationsCount());
        assertEquals(start, stats.windowStart());
        assertEquals(end, stats.windowEnd());
    }

    // Builder Tests

    @Test
    void builderShouldSetTotalOperations() {
        OperationStats stats = OperationStats.builder().totalOperations(42).build();

        assertEquals(42, stats.totalOperations());
    }

    @Test
    void builderShouldSetSuccessCount() {
        OperationStats stats = OperationStats.builder().successCount(25).build();

        assertEquals(25, stats.successCount());
    }

    @Test
    void builderShouldSetFailedCount() {
        OperationStats stats = OperationStats.builder().failedCount(5).build();

        assertEquals(5, stats.failedCount());
    }

    @Test
    void builderShouldSetPartialFailureCount() {
        OperationStats stats = OperationStats.builder().partialFailureCount(3).build();

        assertEquals(3, stats.partialFailureCount());
    }

    @Test
    void builderShouldSetRunningCount() {
        OperationStats stats = OperationStats.builder().runningCount(7).build();

        assertEquals(7, stats.runningCount());
    }

    @Test
    void builderShouldSetNoPolicyCount() {
        OperationStats stats = OperationStats.builder().noPolicyCount(2).build();

        assertEquals(2, stats.noPolicyCount());
    }

    @Test
    void builderShouldSetNoOperationsCount() {
        OperationStats stats = OperationStats.builder().noOperationsCount(1).build();

        assertEquals(1, stats.noOperationsCount());
    }

    @Test
    void builderShouldSetWindowStart() {
        Instant start = Instant.parse("2025-01-15T10:00:00Z");
        OperationStats stats = OperationStats.builder().windowStart(start).build();

        assertEquals(start, stats.windowStart());
    }

    @Test
    void builderShouldSetWindowEnd() {
        Instant end = Instant.parse("2025-01-15T11:00:00Z");
        OperationStats stats = OperationStats.builder().windowEnd(end).build();

        assertEquals(end, stats.windowEnd());
    }

    @Test
    void builderShouldSupportMethodChaining() {
        Instant start = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant end = Instant.now();

        OperationStats stats =
                OperationStats.builder()
                        .totalOperations(100)
                        .successCount(80)
                        .failedCount(5)
                        .partialFailureCount(10)
                        .runningCount(3)
                        .noPolicyCount(1)
                        .noOperationsCount(1)
                        .windowStart(start)
                        .windowEnd(end)
                        .build();

        assertEquals(100, stats.totalOperations());
        assertEquals(80, stats.successCount());
        assertEquals(5, stats.failedCount());
        assertEquals(10, stats.partialFailureCount());
        assertEquals(3, stats.runningCount());
        assertEquals(1, stats.noPolicyCount());
        assertEquals(1, stats.noOperationsCount());
        assertEquals(start, stats.windowStart());
        assertEquals(end, stats.windowEnd());
    }
}
