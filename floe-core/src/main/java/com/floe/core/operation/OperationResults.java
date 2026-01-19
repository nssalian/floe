package com.floe.core.operation;

import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.orchestrator.OperationResult;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Aggregated results from a maintenance run, capturing per-operation outcomes.
 *
 * @param operations list of individual operation results
 * @param aggregatedMetrics combined metrics from all operations
 */
public record OperationResults(
        List<SingleOperationResult> operations, Map<String, Object> aggregatedMetrics) {
    /** Result of a single operation within the maintenance run. */
    public record SingleOperationResult(
            MaintenanceOperation.Type operationType,
            String status,
            long durationMs,
            Map<String, Object> metrics,
            String errorMessage) {
        public static SingleOperationResult from(OperationResult result) {
            return new SingleOperationResult(
                    result.operationType(),
                    result.status().name(),
                    result.duration().toMillis(),
                    result.metrics(),
                    result.errorMessage());
        }

        /** Default constructor for Jackson deserialization. */
        public SingleOperationResult {
            // Ensure metrics is never null
            if (metrics == null) {
                metrics = Map.of();
            }
        }
    }

    /** Create OperationResults from a list of OperationResult. */
    public static OperationResults from(List<OperationResult> results) {
        List<SingleOperationResult> ops =
                results.stream().map(SingleOperationResult::from).toList();

        Map<String, Object> aggregated =
                results.stream()
                        .flatMap(r -> r.metrics().entrySet().stream())
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v2));

        return new OperationResults(ops, aggregated);
    }

    /** Create empty results (for NO_POLICY, NO_OPERATIONS cases). */
    public static OperationResults empty() {
        return new OperationResults(List.of(), Map.of());
    }

    /** Count of successful operations. */
    public long successCount() {
        return operations.stream().filter(op -> "SUCCEEDED".equals(op.status())).count();
    }

    /** Count of failed operations. */
    public long failedCount() {
        return operations.stream().filter(op -> "FAILED".equals(op.status())).count();
    }

    /** Count of skipped operations. */
    public long skippedCount() {
        return operations.stream().filter(op -> "SKIPPED".equals(op.status())).count();
    }

    /** Total duration of all operations in milliseconds. */
    public long totalDurationMs() {
        return operations.stream().mapToLong(SingleOperationResult::durationMs).sum();
    }
}
