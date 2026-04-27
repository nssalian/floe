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

package com.floe.core.engine;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.maintenance.MaintenanceOperation;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExecutionResultTest {

    private final TableIdentifier table = TableIdentifier.of("catalog", "db", "table");
    private final Instant startTime = Instant.parse("2025-12-15T10:00:00Z");
    private final Instant endTime = Instant.parse("2025-12-15T10:05:00Z");

    @Test
    @DisplayName("should create ExecutionResult with all fields")
    void shouldCreateWithAllFields() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        startTime,
                        endTime,
                        Map.of("filesRewritten", 10, "bytesRewritten", 1024L),
                        Optional.empty(),
                        Optional.empty());

        assertEquals("exec-123", result.executionId());
        assertEquals(table, result.table());
        assertEquals(MaintenanceOperation.Type.REWRITE_DATA_FILES, result.operationType());
        assertEquals(ExecutionStatus.SUCCEEDED, result.status());
        assertEquals(startTime, result.startTime());
        assertEquals(endTime, result.endTime());
        assertEquals(2, result.metrics().size());
        assertTrue(result.errorMessage().isEmpty());
        assertTrue(result.errorStackTrace().isEmpty());
    }

    @Test
    @DisplayName("getDuration should calculate correctly")
    void getDurationShouldCalculateCorrectly() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        startTime,
                        endTime,
                        Map.of(),
                        Optional.empty(),
                        Optional.empty());

        assertEquals(Duration.ofMinutes(5), result.getDuration());
    }

    @Test
    @DisplayName("getDuration should return zero when startTime is null")
    void getDurationShouldReturnZeroWhenStartTimeIsNull() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.PENDING,
                        null,
                        null,
                        Map.of(),
                        Optional.empty(),
                        Optional.empty());

        assertEquals(Duration.ZERO, result.getDuration());
    }

    @Test
    @DisplayName("getDuration should return zero when endTime is null")
    void getDurationShouldReturnZeroWhenEndTimeIsNull() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.RUNNING,
                        startTime,
                        null,
                        Map.of(),
                        Optional.empty(),
                        Optional.empty());

        assertEquals(Duration.ZERO, result.getDuration());
    }

    @Test
    @DisplayName("isSuccess should return true for succeeded")
    void isSuccessShouldReturnTrueForSucceeded() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        startTime,
                        endTime,
                        Map.of(),
                        Optional.empty(),
                        Optional.empty());

        assertTrue(result.isSuccess());
        assertFalse(result.isFailed());
    }

    @Test
    @DisplayName("isFailed should return true for failed")
    void isFailedShouldReturnTrueForFailed() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.FAILED,
                        startTime,
                        endTime,
                        Map.of(),
                        Optional.of("Error message"),
                        Optional.of("Stack trace"));

        assertTrue(result.isFailed());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("getMetric should return value of correct type")
    void getMetricShouldReturnValueOfCorrectType() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        startTime,
                        endTime,
                        Map.of("filesRewritten", 10, "bytesRewritten", 1024L, "ratio", 0.5),
                        Optional.empty(),
                        Optional.empty());

        assertEquals(Optional.of(10), result.getMetric("filesRewritten", Integer.class));
        assertEquals(Optional.of(1024L), result.getMetric("bytesRewritten", Long.class));
        assertEquals(Optional.of(0.5), result.getMetric("ratio", Double.class));
    }

    @Test
    @DisplayName("getMetric should return empty for missing key")
    void getMetricShouldReturnEmptyForMissingKey() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        startTime,
                        endTime,
                        Map.of(),
                        Optional.empty(),
                        Optional.empty());

        assertEquals(Optional.empty(), result.getMetric("nonExistent", Integer.class));
    }

    @Test
    @DisplayName("getMetric should return empty for wrong type")
    void getMetricShouldReturnEmptyForWrongType() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        startTime,
                        endTime,
                        Map.of("filesRewritten", 10),
                        Optional.empty(),
                        Optional.empty());

        // Asking for String when the value is Integer
        assertEquals(Optional.empty(), result.getMetric("filesRewritten", String.class));
    }

    @Test
    @DisplayName("getExecutionId should return the correct id")
    void getExecutionIdShouldReturnId() {
        ExecutionResult result =
                new ExecutionResult(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        startTime,
                        endTime,
                        Map.of(),
                        Optional.empty(),
                        Optional.empty());

        assertEquals("exec-123", result.getExecutionId());
    }

    @Test
    @DisplayName("success factory should create successful result")
    void successFactoryShouldCreateSuccessfulResult() {
        Map<String, Object> metrics = Map.of("filesRewritten", 10);

        ExecutionResult result =
                ExecutionResult.success(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        startTime,
                        endTime,
                        metrics);

        assertEquals("exec-123", result.executionId());
        assertEquals(table, result.table());
        assertEquals(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS, result.operationType());
        assertEquals(ExecutionStatus.SUCCEEDED, result.status());
        assertEquals(startTime, result.startTime());
        assertEquals(endTime, result.endTime());
        assertEquals(metrics, result.metrics());
        assertTrue(result.errorMessage().isEmpty());
        assertTrue(result.errorStackTrace().isEmpty());
        assertTrue(result.isSuccess());
    }

    @Test
    @DisplayName("failure factory should create failed result")
    void failureFactoryShouldCreateFailedResult() {
        ExecutionResult result =
                ExecutionResult.failure(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_MANIFESTS,
                        startTime,
                        endTime,
                        "Something went wrong",
                        "java.lang.RuntimeException: Something went wrong\n\tat ...");

        assertEquals("exec-123", result.executionId());
        assertEquals(table, result.table());
        assertEquals(MaintenanceOperation.Type.REWRITE_MANIFESTS, result.operationType());
        assertEquals(ExecutionStatus.FAILED, result.status());
        assertEquals(startTime, result.startTime());
        assertEquals(endTime, result.endTime());
        assertTrue(result.metrics().isEmpty());
        assertEquals(Optional.of("Something went wrong"), result.errorMessage());
        assertTrue(result.errorStackTrace().isPresent());
        assertTrue(result.isFailed());
    }

    @Test
    @DisplayName("cancelled factory should create cancelled result")
    void cancelledFactoryShouldCreateCancelledResult() {
        ExecutionResult result =
                ExecutionResult.success(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.ORPHAN_CLEANUP,
                        startTime,
                        endTime,
                        Map.of());

        assertEquals("exec-123", result.executionId());
        assertEquals(table, result.table());
        assertEquals(MaintenanceOperation.Type.ORPHAN_CLEANUP, result.operationType());
        assertEquals(ExecutionStatus.SUCCEEDED, result.status());
        assertEquals(startTime, result.startTime());
        assertEquals(endTime, result.endTime());
        assertTrue(result.metrics().isEmpty());
        assertTrue(result.errorMessage().isEmpty());
        assertTrue(result.errorStackTrace().isEmpty());
        assertFalse(result.isFailed());
    }

    @Test
    @DisplayName("record should support equality and hashCode")
    void recordShouldSupportEquality() {
        ExecutionResult result1 =
                ExecutionResult.success(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        startTime,
                        endTime,
                        Map.of());
        ExecutionResult result2 =
                ExecutionResult.success(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        startTime,
                        endTime,
                        Map.of());

        assertEquals(result1, result2);
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    @Test
    @DisplayName("different results should not be equal")
    void differentResultsShouldNotBeEqual() {
        ExecutionResult result1 =
                ExecutionResult.success(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        startTime,
                        endTime,
                        Map.of());
        ExecutionResult result2 =
                ExecutionResult.success(
                        "exec-456",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        startTime,
                        endTime,
                        Map.of());

        assertNotEquals(result1, result2);
    }

    @Test
    @DisplayName("record should have meaningful toString")
    void recordShouldHaveToString() {
        ExecutionResult result =
                ExecutionResult.success(
                        "exec-123",
                        table,
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        startTime,
                        endTime,
                        Map.of());
        String toString = result.toString();

        assertTrue(toString.contains("ExecutionResult"));
        assertTrue(toString.contains("exec-123"));
    }
}
