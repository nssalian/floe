package com.floe.core.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OperationStatsTest {

    @Test
    void fromRecordsCalculatesFailureRate() {
        List<OperationRecord> records =
                List.of(
                        recordWithStatus(OperationStatus.SUCCESS),
                        recordWithStatus(OperationStatus.FAILED),
                        recordWithStatus(OperationStatus.SUCCESS),
                        recordWithStatus(OperationStatus.PARTIAL_FAILURE));

        OperationStats stats = OperationStats.fromRecords(records, 4);

        assertEquals(50.0, stats.failureRate());
    }

    @Test
    void fromRecordsCalculatesAverageChanges() {
        List<OperationRecord> records =
                List.of(recordWithMetrics(100, 0), recordWithMetrics(300, 0));

        OperationStats stats = OperationStats.fromRecords(records, 2);

        assertEquals(200.0, stats.averageChanges());
    }

    @Test
    void fromRecordsCountsZeroChangeRuns() {
        List<OperationRecord> records =
                List.of(recordWithMetrics(0, 0), recordWithMetrics(10, 0), recordWithMetrics(0, 0));

        OperationStats stats = OperationStats.fromRecords(records, 3);

        assertEquals(2, stats.zeroChangeRuns());
    }

    @Test
    void fromRecordsLastNRuns() {
        List<OperationRecord> records =
                List.of(
                        recordWithStatus(OperationStatus.SUCCESS),
                        recordWithStatus(OperationStatus.SUCCESS),
                        recordWithStatus(OperationStatus.SUCCESS),
                        recordWithStatus(OperationStatus.SUCCESS));

        OperationStats stats = OperationStats.fromRecords(records, 2);

        assertEquals(2, stats.totalOperations());
    }

    private OperationRecord recordWithStatus(OperationStatus status) {
        return OperationRecord.builder()
                .catalog("demo")
                .namespace("db")
                .tableName("table")
                .status(status)
                .startedAt(Instant.now())
                .normalizedMetrics(Map.of())
                .build();
    }

    private OperationRecord recordWithMetrics(long bytes, long files) {
        return OperationRecord.builder()
                .catalog("demo")
                .namespace("db")
                .tableName("table")
                .status(OperationStatus.SUCCESS)
                .startedAt(Instant.now())
                .normalizedMetrics(
                        Map.of(
                                NormalizedMetrics.BYTES_REWRITTEN, bytes,
                                NormalizedMetrics.FILES_REWRITTEN, files))
                .build();
    }
}
