package com.floe.core.orchestrator;

import com.floe.core.health.HealthIssue;
import com.floe.core.maintenance.MaintenanceOperation;
import java.util.List;

/**
 * Represents a maintenance operation planned by the MaintenancePlanner.
 *
 * @param operationType the type of operation to perform
 * @param reason human-readable explanation of why this operation was planned
 * @param severity the severity level driving this operation
 * @param triggerReasons list of trigger conditions that caused this operation to be planned
 * @param conditionBased true if triggered by signal-based conditions, false if health-threshold
 *     based
 * @param forcedByCriticalPipeline true if forced due to critical pipeline deadline
 */
public record PlannedOperation(
        MaintenanceOperation.Type operationType,
        String reason,
        HealthIssue.Severity severity,
        List<String> triggerReasons,
        boolean conditionBased,
        boolean forcedByCriticalPipeline) {
    /** Compact constructor to ensure triggerReasons is never null. */
    public PlannedOperation {
        if (triggerReasons == null) {
            triggerReasons = List.of();
        }
    }

    /**
     * Creates a planned operation for a critical health issue.
     *
     * @param operationType the operation type
     * @param reason the reason for planning this operation
     * @return a new PlannedOperation with CRITICAL severity
     */
    public static PlannedOperation critical(
            MaintenanceOperation.Type operationType, String reason) {
        return new PlannedOperation(
                operationType, reason, HealthIssue.Severity.CRITICAL, List.of(), false, false);
    }

    /**
     * Creates a planned operation for a warning health issue.
     *
     * @param operationType the operation type
     * @param reason the reason for planning this operation
     * @return a new PlannedOperation with WARNING severity
     */
    public static PlannedOperation warning(MaintenanceOperation.Type operationType, String reason) {
        return new PlannedOperation(
                operationType, reason, HealthIssue.Severity.WARNING, List.of(), false, false);
    }

    /**
     * Creates a planned operation for an informational health issue.
     *
     * @param operationType the operation type
     * @param reason the reason for planning this operation
     * @return a new PlannedOperation with INFO severity
     */
    public static PlannedOperation info(MaintenanceOperation.Type operationType, String reason) {
        return new PlannedOperation(
                operationType, reason, HealthIssue.Severity.INFO, List.of(), false, false);
    }

    /**
     * Creates a planned operation triggered by signal-based conditions.
     *
     * @param operationType the operation type
     * @param triggerReasons the conditions that triggered this operation
     * @param forcedByCriticalPipeline true if forced due to critical pipeline deadline
     * @return a new PlannedOperation with condition-based trigger
     */
    public static PlannedOperation triggered(
            MaintenanceOperation.Type operationType,
            List<String> triggerReasons,
            boolean forcedByCriticalPipeline) {
        HealthIssue.Severity severity =
                forcedByCriticalPipeline
                        ? HealthIssue.Severity.CRITICAL
                        : HealthIssue.Severity.WARNING;
        String reason =
                forcedByCriticalPipeline
                        ? "Critical pipeline deadline exceeded"
                        : String.join("; ", triggerReasons);
        return new PlannedOperation(
                operationType, reason, severity, triggerReasons, true, forcedByCriticalPipeline);
    }

    /**
     * Checks if this operation has critical severity.
     *
     * @return true if severity is CRITICAL
     */
    public boolean isCritical() {
        return severity == HealthIssue.Severity.CRITICAL;
    }

    /**
     * Checks if this operation has warning or worse severity.
     *
     * @return true if severity is WARNING or CRITICAL
     */
    public boolean isWarningOrWorse() {
        return (severity == HealthIssue.Severity.WARNING
                || severity == HealthIssue.Severity.CRITICAL);
    }

    /**
     * Checks if this operation was triggered by conditions rather than health thresholds.
     *
     * @return true if condition-based
     */
    public boolean isConditionBased() {
        return conditionBased;
    }
}
