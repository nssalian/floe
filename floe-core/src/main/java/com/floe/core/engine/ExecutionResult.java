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

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.maintenance.MaintenanceOperation;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Result of executing a maintenance operation.
 *
 * @param executionId unique execution identifier
 * @param table target table
 * @param operationType type of operation executed
 * @param status execution status
 * @param startTime when execution started
 * @param endTime when execution ended
 * @param metrics operation metrics
 * @param errorMessage error message if failed
 * @param errorStackTrace stack trace if failed
 */
public record ExecutionResult(
        String executionId,
        TableIdentifier table,
        MaintenanceOperation.Type operationType,
        ExecutionStatus status,
        Instant startTime,
        Instant endTime,
        Map<String, Object> metrics,
        Optional<String> errorMessage,
        Optional<String> errorStackTrace) {
    /** Duration of the execution. */
    public java.time.Duration getDuration() {
        if (endTime == null || startTime == null) {
            return java.time.Duration.ZERO;
        }
        return java.time.Duration.between(startTime, endTime);
    }

    /** Check if execution was successful. */
    public boolean isSuccess() {
        return status == ExecutionStatus.SUCCEEDED;
    }

    /** Check if execution failed. */
    public boolean isFailed() {
        return status == ExecutionStatus.FAILED;
    }

    /** Get a metric value with a default. */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getMetric(String key, Class<T> type) {
        Object value = metrics.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (type.isInstance(value)) {
            return Optional.of((T) value);
        }
        return Optional.empty();
    }

    /** Get execution id */
    public String getExecutionId() {
        return executionId;
    }

    /**
     * Create a successful result.
     *
     * @param executionId unique execution identifier
     * @param table target table
     * @param operationType type of operation executed
     * @param startTime when execution started
     * @param endTime when execution ended
     * @param metrics operation metrics
     * @return a successful execution result
     */
    public static ExecutionResult success(
            String executionId,
            TableIdentifier table,
            MaintenanceOperation.Type operationType,
            Instant startTime,
            Instant endTime,
            Map<String, Object> metrics) {
        return new ExecutionResult(
                executionId,
                table,
                operationType,
                ExecutionStatus.SUCCEEDED,
                startTime,
                endTime,
                metrics,
                Optional.empty(),
                Optional.empty());
    }

    /**
     * Create a failed result.
     *
     * @param executionId unique execution identifier
     * @param table target table
     * @param operationType type of operation executed
     * @param startTime when execution started
     * @param endTime when execution ended
     * @param errorMessage error message describing the failure
     * @param errorStackTrace stack trace of the error
     * @return a failed execution result
     */
    public static ExecutionResult failure(
            String executionId,
            TableIdentifier table,
            MaintenanceOperation.Type operationType,
            Instant startTime,
            Instant endTime,
            String errorMessage,
            String errorStackTrace) {
        return new ExecutionResult(
                executionId,
                table,
                operationType,
                ExecutionStatus.FAILED,
                startTime,
                endTime,
                Map.of(),
                Optional.of(errorMessage),
                Optional.of(errorStackTrace));
    }

    /**
     * Create a cancelled result.
     *
     * @param executionId unique execution identifier
     * @param table target table
     * @param operationType type of operation executed
     * @param startTime when execution started
     * @param endTime when execution ended
     * @return a cancelled execution result
     */
    public static ExecutionResult cancelled(
            String executionId,
            TableIdentifier table,
            MaintenanceOperation.Type operationType,
            Instant startTime,
            Instant endTime) {
        return new ExecutionResult(
                executionId,
                table,
                operationType,
                ExecutionStatus.CANCELLED,
                startTime,
                endTime,
                Map.of(),
                Optional.empty(),
                Optional.empty());
    }

    /**
     * Create a skipped result (operation not supported for this catalog/engine).
     *
     * @param executionId unique execution identifier
     * @param table target table
     * @param operationType type of operation executed
     * @param time the time of the skip
     * @param reason reason the operation was skipped
     * @return a skipped execution result
     */
    public static ExecutionResult skipped(
            String executionId,
            TableIdentifier table,
            MaintenanceOperation.Type operationType,
            Instant time,
            String reason) {
        return new ExecutionResult(
                executionId,
                table,
                operationType,
                ExecutionStatus.SKIPPED,
                time,
                time,
                Map.of("skipReason", reason),
                Optional.of(reason),
                Optional.empty());
    }
}
