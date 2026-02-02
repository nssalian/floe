package com.floe.core.orchestrator;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.ExecutionStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Result of a maintenance orchestration run for a single table.
 *
 * <p>Aggregates the results of all maintenance operations executed for a table, including timing
 * information, individual operation results, and overall status.
 *
 * @param orchestrationId unique identifier for this orchestration run
 * @param table the table that was maintained
 * @param policyName the policy that was applied (null if no policy matched)
 * @param status overall status of the orchestration
 * @param startTime when the orchestration started
 * @param endTime when the orchestration completed (null if still running)
 * @param operationResults results of individual maintenance operations
 * @param message human-readable status message
 */
public record OrchestratorResult(
        String orchestrationId,
        TableIdentifier table,
        String policyName,
        Status status,
        Instant startTime,
        Instant endTime,
        List<OperationResult> operationResults,
        String message) {
    public enum Status {
        /** All operations completed successfully */
        SUCCESS,
        /** At least one operation failed */
        PARTIAL_FAILURE,
        /** All operations failed */
        FAILED,
        /** No matching policy found for the table */
        NO_POLICY,
        /** Policy matched but no operations were enabled/selected */
        NO_OPERATIONS,
        /** Orchestration is still running */
        RUNNING,
    }

    public Duration duration() {
        if (endTime == null) {
            return Duration.between(startTime, Instant.now());
        }
        return Duration.between(startTime, endTime);
    }

    public long successCount() {
        return operationResults.stream()
                .filter(r -> r.status() == ExecutionStatus.SUCCEEDED)
                .count();
    }

    public long failedCount() {
        return operationResults.stream().filter(r -> r.status() == ExecutionStatus.FAILED).count();
    }

    public long skippedCount() {
        return operationResults.stream()
                .filter(
                        r ->
                                r.status() == ExecutionStatus.CANCELLED
                                        || r.status() == ExecutionStatus.SKIPPED)
                .count();
    }

    public Map<String, Object> aggregateMetrics() {
        return operationResults.stream()
                .flatMap(r -> r.metrics().entrySet().stream())
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                OrchestratorResult::mergeMetricValues));
    }

    private static Object mergeMetricValues(Object left, Object right) {
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            if (leftNumber instanceof Float
                    || leftNumber instanceof Double
                    || rightNumber instanceof Float
                    || rightNumber instanceof Double) {
                return leftNumber.doubleValue() + rightNumber.doubleValue();
            }
            return leftNumber.longValue() + rightNumber.longValue();
        }
        return right;
    }

    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }

    public boolean hasFailures() {
        return status == Status.FAILED || status == Status.PARTIAL_FAILURE;
    }

    public static OrchestratorResult noPolicy(
            String orchestrationId, TableIdentifier table, Instant startTime) {
        return new OrchestratorResult(
                orchestrationId,
                table,
                null,
                Status.NO_POLICY,
                startTime,
                Instant.now(),
                List.of(),
                "No matching policy found for table");
    }

    public static OrchestratorResult noPolicyWithMessage(
            String orchestrationId, TableIdentifier table, Instant startTime, String message) {
        return new OrchestratorResult(
                orchestrationId,
                table,
                null,
                Status.NO_POLICY,
                startTime,
                Instant.now(),
                List.of(),
                message);
    }

    public static OrchestratorResult noOperations(
            String orchestrationId, TableIdentifier table, String policyName, Instant startTime) {
        return new OrchestratorResult(
                orchestrationId,
                table,
                policyName,
                Status.NO_OPERATIONS,
                startTime,
                Instant.now(),
                List.of(),
                "No operations enabled or selected");
    }

    public static OrchestratorResult running(
            String orchestrationId,
            TableIdentifier table,
            String policyName,
            Instant startTime,
            List<OperationResult> operationResults) {
        return new OrchestratorResult(
                orchestrationId,
                table,
                policyName,
                Status.RUNNING,
                startTime,
                null,
                operationResults,
                "Orchestration is still running");
    }

    public static OrchestratorResult completed(
            String orchestrationId,
            TableIdentifier table,
            String policyName,
            Instant startTime,
            List<OperationResult> operationResults) {
        long successCount =
                operationResults.stream()
                        .filter(r -> r.status() == ExecutionStatus.SUCCEEDED)
                        .count();
        long failedCount =
                operationResults.stream().filter(r -> r.status() == ExecutionStatus.FAILED).count();

        Status finalStatus;
        if (failedCount == 0 && successCount > 0) {
            finalStatus = Status.SUCCESS;
        } else if (failedCount > 0 && successCount > 0) {
            finalStatus = Status.PARTIAL_FAILURE;
        } else if (failedCount > 0) {
            finalStatus = Status.FAILED;
        } else {
            finalStatus = Status.NO_OPERATIONS;
        }

        return new OrchestratorResult(
                orchestrationId,
                table,
                policyName,
                finalStatus,
                startTime,
                Instant.now(),
                operationResults,
                "Orchestration completed");
    }
}
