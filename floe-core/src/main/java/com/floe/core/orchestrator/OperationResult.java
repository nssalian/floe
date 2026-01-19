package com.floe.core.orchestrator;

import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Result of a single maintenance operation within an orchestration run.
 *
 * @param operationType type of operation
 * @param status execution status
 * @param startTime when operation started
 * @param endTime when operation ended
 * @param metrics operation metrics
 * @param errorMessage error message if failed
 */
public record OperationResult(
        MaintenanceOperation.Type operationType,
        ExecutionStatus status,
        Instant startTime,
        Instant endTime,
        Map<String, Object> metrics,
        String errorMessage) {
    public Duration duration() {
        if (endTime == null) {
            return Duration.between(startTime, Instant.now());
        }
        return Duration.between(startTime, endTime);
    }

    public boolean isSuccess() {
        return status == ExecutionStatus.SUCCEEDED;
    }

    public boolean isFailed() {
        return status == ExecutionStatus.FAILED;
    }

    public boolean isSkipped() {
        return status == ExecutionStatus.CANCELLED;
    }

    public static OperationResult skipped(MaintenanceOperation.Type type, String reason) {
        return new OperationResult(
                type, ExecutionStatus.CANCELLED, Instant.now(), Instant.now(), Map.of(), reason);
    }
}
