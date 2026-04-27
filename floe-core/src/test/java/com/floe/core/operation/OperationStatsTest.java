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
