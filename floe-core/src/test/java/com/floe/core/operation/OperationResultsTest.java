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

import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.orchestrator.OperationResult;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OperationResultsTest {

    // Record Tests

    @Test
    void shouldCreateResultsWithOperationsAndMetrics() {
        OperationResults.SingleOperationResult op1 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "SUCCEEDED",
                        5000,
                        Map.of("files_rewritten", 10),
                        null);

        OperationResults.SingleOperationResult op2 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        "SUCCEEDED",
                        2000,
                        Map.of("snapshots_expired", 5),
                        null);

        Map<String, Object> aggregated = Map.of("total_files", 15);

        OperationResults results = new OperationResults(List.of(op1, op2), aggregated);

        assertEquals(2, results.operations().size());
        assertEquals(aggregated, results.aggregatedMetrics());
    }

    // empty() Tests

    @Test
    void shouldCreateEmptyResults() {
        OperationResults results = OperationResults.empty();

        assertTrue(results.operations().isEmpty());
        assertTrue(results.aggregatedMetrics().isEmpty());
    }

    // from() Tests

    @Test
    void shouldCreateFromOperationResults() {
        Instant start = Instant.now();
        Instant end = start.plusSeconds(5);

        OperationResult result1 =
                new OperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        start,
                        end,
                        Map.of("files_rewritten", 10),
                        null);

        OperationResult result2 =
                new OperationResult(
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        ExecutionStatus.SUCCEEDED,
                        start,
                        start.plusSeconds(3),
                        Map.of("snapshots_expired", 8),
                        null);

        OperationResults results = OperationResults.from(List.of(result1, result2));

        assertEquals(2, results.operations().size());

        OperationResults.SingleOperationResult op1 = results.operations().get(0);
        assertEquals(MaintenanceOperation.Type.REWRITE_DATA_FILES, op1.operationType());
        assertEquals("SUCCEEDED", op1.status());
        assertEquals(5000, op1.durationMs());
        assertEquals(10, op1.metrics().get("files_rewritten"));
    }

    @Test
    void shouldCreateFromEmptyList() {
        OperationResults results = OperationResults.from(List.of());

        assertTrue(results.operations().isEmpty());
        assertTrue(results.aggregatedMetrics().isEmpty());
    }

    @Test
    void shouldAggregateMetricsFromMultipleResults() {
        Instant start = Instant.now();

        OperationResult result1 =
                new OperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        start,
                        start.plusSeconds(5),
                        Map.of("key1", "value1"),
                        null);

        OperationResult result2 =
                new OperationResult(
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        ExecutionStatus.SUCCEEDED,
                        start,
                        start.plusSeconds(3),
                        Map.of("key2", "value2"),
                        null);

        OperationResults results = OperationResults.from(List.of(result1, result2));

        assertTrue(results.aggregatedMetrics().containsKey("key1"));
        assertTrue(results.aggregatedMetrics().containsKey("key2"));
    }

    // successCount Tests

    @Test
    void shouldCountSuccessfulOperations() {
        OperationResults.SingleOperationResult op1 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "SUCCEEDED",
                        5000,
                        Map.of(),
                        null);
        OperationResults.SingleOperationResult op2 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        "SUCCEEDED",
                        2000,
                        Map.of(),
                        null);
        OperationResults.SingleOperationResult op3 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.ORPHAN_CLEANUP,
                        "FAILED",
                        1000,
                        Map.of(),
                        "error");

        OperationResults results = new OperationResults(List.of(op1, op2, op3), Map.of());

        assertEquals(2, results.successCount());
    }

    @Test
    void shouldReturnZeroSuccessCountWhenNoSuccesses() {
        OperationResults.SingleOperationResult op =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "FAILED",
                        5000,
                        Map.of(),
                        "error");

        OperationResults results = new OperationResults(List.of(op), Map.of());

        assertEquals(0, results.successCount());
    }

    // failedCount Tests

    @Test
    void shouldCountFailedOperations() {
        OperationResults.SingleOperationResult op1 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "FAILED",
                        5000,
                        Map.of(),
                        "error1");
        OperationResults.SingleOperationResult op2 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        "SUCCEEDED",
                        2000,
                        Map.of(),
                        null);
        OperationResults.SingleOperationResult op3 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.ORPHAN_CLEANUP,
                        "FAILED",
                        1000,
                        Map.of(),
                        "error2");

        OperationResults results = new OperationResults(List.of(op1, op2, op3), Map.of());

        assertEquals(2, results.failedCount());
    }

    @Test
    void shouldReturnZeroFailedCountWhenNoFailures() {
        OperationResults.SingleOperationResult op =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "SUCCEEDED",
                        5000,
                        Map.of(),
                        null);

        OperationResults results = new OperationResults(List.of(op), Map.of());

        assertEquals(0, results.failedCount());
    }

    // skippedCount Tests

    @Test
    void shouldCountSkippedOperations() {
        OperationResults.SingleOperationResult op1 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES, "SKIPPED", 0, Map.of(), null);
        OperationResults.SingleOperationResult op2 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        "SUCCEEDED",
                        2000,
                        Map.of(),
                        null);
        OperationResults.SingleOperationResult op3 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.ORPHAN_CLEANUP, "SKIPPED", 0, Map.of(), null);

        OperationResults results = new OperationResults(List.of(op1, op2, op3), Map.of());

        assertEquals(2, results.skippedCount());
    }

    // totalDurationMs Tests

    @Test
    void shouldCalculateTotalDuration() {
        OperationResults.SingleOperationResult op1 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "SUCCEEDED",
                        5000,
                        Map.of(),
                        null);
        OperationResults.SingleOperationResult op2 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        "SUCCEEDED",
                        3000,
                        Map.of(),
                        null);
        OperationResults.SingleOperationResult op3 =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.ORPHAN_CLEANUP,
                        "SUCCEEDED",
                        2000,
                        Map.of(),
                        null);

        OperationResults results = new OperationResults(List.of(op1, op2, op3), Map.of());

        assertEquals(10000, results.totalDurationMs());
    }

    @Test
    void shouldReturnZeroDurationForEmptyOperations() {
        OperationResults results = OperationResults.empty();

        assertEquals(0, results.totalDurationMs());
    }

    // SingleOperationResult Tests

    @Test
    void singleOperationResultShouldCreateFromOperationResult() {
        Instant start = Instant.now();
        Instant end = start.plusMillis(5500);

        OperationResult result =
                new OperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        start,
                        end,
                        Map.of("files", 10),
                        null);

        OperationResults.SingleOperationResult single =
                OperationResults.SingleOperationResult.from(result);

        assertEquals(MaintenanceOperation.Type.REWRITE_DATA_FILES, single.operationType());
        assertEquals("SUCCEEDED", single.status());
        assertEquals(5500, single.durationMs());
        assertEquals(10, single.metrics().get("files"));
        assertNull(single.errorMessage());
    }

    @Test
    void singleOperationResultShouldHandleFailedOperation() {
        Instant start = Instant.now();

        OperationResult result =
                new OperationResult(
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        ExecutionStatus.FAILED,
                        start,
                        start.plusMillis(1000),
                        Map.of(),
                        "Something went wrong");

        OperationResults.SingleOperationResult single =
                OperationResults.SingleOperationResult.from(result);

        assertEquals(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS, single.operationType());
        assertEquals("FAILED", single.status());
        assertEquals("Something went wrong", single.errorMessage());
    }

    @Test
    void singleOperationResultShouldInitializeNullMetricsToEmptyMap() {
        OperationResults.SingleOperationResult single =
                new OperationResults.SingleOperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        "SUCCEEDED",
                        5000,
                        null,
                        null);

        assertNotNull(single.metrics());
        assertTrue(single.metrics().isEmpty());
    }
}
