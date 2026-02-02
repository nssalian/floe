package com.floe.core.orchestrator;

import com.floe.core.health.HealthReport;
import com.floe.core.health.HealthThresholds;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OperationType;
import com.floe.core.policy.TriggerConditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans maintenance operations based on health reports, operation history, and policy settings.
 *
 * <p>The planner analyzes table health and recent operation statistics to determine which
 * maintenance operations should be executed. It respects policy-level operation enablement and can
 * use custom thresholds defined at the policy level.
 */
public class MaintenancePlanner {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenancePlanner.class);

    private final TriggerEvaluator triggerEvaluator;

    public MaintenancePlanner() {
        this.triggerEvaluator = new TriggerEvaluator();
    }

    public MaintenancePlanner(TriggerEvaluator triggerEvaluator) {
        this.triggerEvaluator = triggerEvaluator;
    }

    /**
     * Plans maintenance operations based on health and operation history.
     *
     * @param healthReport the latest health assessment for the table (may be null for backward
     *     compatibility)
     * @param policy the maintenance policy for the table
     * @param thresholds the thresholds to use (policy-level or defaults)
     * @return list of planned operations with reasons
     */
    public List<PlannedOperation> plan(
            HealthReport healthReport, MaintenancePolicy policy, HealthThresholds thresholds) {
        List<PlannedOperation> planned = new ArrayList<>();

        // If no health report, return empty (fallback to legacy behavior handled by orchestrator)
        if (healthReport == null) {
            LOG.debug("No health report available, planner returning empty list");
            return planned;
        }

        // Use provided thresholds or defaults
        HealthThresholds effectiveThresholds =
                thresholds != null ? thresholds : HealthThresholds.defaults();

        // Plan operations based on health issues
        planRewriteDataFiles(healthReport, effectiveThresholds, policy, planned);
        planExpireSnapshots(healthReport, effectiveThresholds, policy, planned);
        planRewriteManifests(healthReport, effectiveThresholds, policy, planned);
        planOrphanCleanup(healthReport, effectiveThresholds, policy, planned);

        LOG.debug(
                "Planned {} operations for table {}",
                planned.size(),
                healthReport.tableIdentifier().toQualifiedName());

        return planned;
    }

    /**
     * Plans maintenance operations with trigger condition evaluation.
     *
     * <p>This method extends the basic planning with signal-based triggering. If the policy has
     * trigger conditions configured, operations will only be planned if conditions are met.
     *
     * @param healthReport the latest health assessment for the table
     * @param policy the maintenance policy for the table
     * @param thresholds the thresholds to use (policy-level or defaults)
     * @param lastOperationTimes map of operation type to last operation time
     * @param now current time for evaluation
     * @return list of planned operations that passed trigger evaluation
     */
    public List<PlannedOperation> planWithTriggerEvaluation(
            HealthReport healthReport,
            MaintenancePolicy policy,
            HealthThresholds thresholds,
            Map<OperationType, Instant> lastOperationTimes,
            Instant now) {
        TriggerConditions conditions = policy.effectiveTriggerConditions();

        // If no trigger conditions, fall back to standard planning
        if (conditions == null) {
            LOG.debug("No trigger conditions configured, using standard planning");
            return plan(healthReport, policy, thresholds);
        }

        List<PlannedOperation> planned = new ArrayList<>();

        // Evaluate each operation type that has config
        for (OperationType opType : OperationType.values()) {
            if (!policy.hasOperationConfig(opType)) {
                continue;
            }

            Instant lastOpTime = lastOperationTimes != null ? lastOperationTimes.get(opType) : null;

            TriggerEvaluator.EvaluationResult evalResult =
                    triggerEvaluator.evaluate(conditions, healthReport, lastOpTime, opType, now);

            if (evalResult.shouldTrigger()) {
                MaintenanceOperation.Type maintenanceType = toMaintenanceType(opType);
                PlannedOperation op =
                        PlannedOperation.triggered(
                                maintenanceType,
                                evalResult.metConditions(),
                                evalResult.forcedByCriticalPipeline());
                planned.add(op);

                LOG.debug("Trigger conditions met for {}: {}", opType, evalResult.metConditions());
            } else {
                LOG.debug(
                        "Trigger conditions not met for {}: unmet={}",
                        opType,
                        evalResult.unmetConditions());
            }
        }

        LOG.debug(
                "Planned {} operations with trigger evaluation for table {}",
                planned.size(),
                healthReport != null
                        ? healthReport.tableIdentifier().toQualifiedName()
                        : "unknown");

        return planned;
    }

    /**
     * Evaluates trigger conditions for a specific operation type.
     *
     * @param conditions the trigger conditions
     * @param healthReport the health report
     * @param lastOperationTime time of last operation
     * @param operationType the operation type
     * @param now current time
     * @return evaluation result
     */
    public TriggerEvaluator.EvaluationResult evaluateTrigger(
            TriggerConditions conditions,
            HealthReport healthReport,
            Instant lastOperationTime,
            OperationType operationType,
            Instant now) {
        return triggerEvaluator.evaluate(
                conditions, healthReport, lastOperationTime, operationType, now);
    }

    private MaintenanceOperation.Type toMaintenanceType(OperationType opType) {
        return switch (opType) {
            case REWRITE_DATA_FILES -> MaintenanceOperation.Type.REWRITE_DATA_FILES;
            case EXPIRE_SNAPSHOTS -> MaintenanceOperation.Type.EXPIRE_SNAPSHOTS;
            case ORPHAN_CLEANUP -> MaintenanceOperation.Type.ORPHAN_CLEANUP;
            case REWRITE_MANIFESTS -> MaintenanceOperation.Type.REWRITE_MANIFESTS;
        };
    }

    private void planRewriteDataFiles(
            HealthReport report,
            HealthThresholds thresholds,
            MaintenancePolicy policy,
            List<PlannedOperation> planned) {
        if (!policy.hasOperationConfig(OperationType.REWRITE_DATA_FILES)) {
            return;
        }

        // Check for small file issues
        double smallFilePercent = report.smallFilePercentage();
        if (smallFilePercent >= thresholds.smallFilePercentCritical()) {
            planned.add(
                    PlannedOperation.critical(
                            MaintenanceOperation.Type.REWRITE_DATA_FILES,
                            String.format(
                                    "Critical: %.1f%% small files (threshold: %.1f%%)",
                                    smallFilePercent, thresholds.smallFilePercentCritical())));
            return; // Already planning, no need to add warning level
        }
        if (smallFilePercent >= thresholds.smallFilePercentWarning()) {
            planned.add(
                    PlannedOperation.warning(
                            MaintenanceOperation.Type.REWRITE_DATA_FILES,
                            String.format(
                                    "Warning: %.1f%% small files (threshold: %.1f%%)",
                                    smallFilePercent, thresholds.smallFilePercentWarning())));
            return;
        }

        // Check for large file issues
        double largeFilePercent = report.largeFilePercentage();
        if (largeFilePercent >= thresholds.largeFilePercentCritical()) {
            planned.add(
                    PlannedOperation.critical(
                            MaintenanceOperation.Type.REWRITE_DATA_FILES,
                            String.format(
                                    "Critical: %.1f%% large files (threshold: %.1f%%)",
                                    largeFilePercent, thresholds.largeFilePercentCritical())));
            return;
        }
        if (largeFilePercent >= thresholds.largeFilePercentWarning()) {
            planned.add(
                    PlannedOperation.warning(
                            MaintenanceOperation.Type.REWRITE_DATA_FILES,
                            String.format(
                                    "Warning: %.1f%% large files (threshold: %.1f%%)",
                                    largeFilePercent, thresholds.largeFilePercentWarning())));
            return;
        }

        // Check for high file count
        if (report.dataFileCount() >= thresholds.fileCountCritical()) {
            planned.add(
                    PlannedOperation.critical(
                            MaintenanceOperation.Type.REWRITE_DATA_FILES,
                            String.format(
                                    "Critical: %d data files (threshold: %d)",
                                    report.dataFileCount(), thresholds.fileCountCritical())));
        } else if (report.dataFileCount() >= thresholds.fileCountWarning()) {
            planned.add(
                    PlannedOperation.warning(
                            MaintenanceOperation.Type.REWRITE_DATA_FILES,
                            String.format(
                                    "Warning: %d data files (threshold: %d)",
                                    report.dataFileCount(), thresholds.fileCountWarning())));
        }
    }

    private void planExpireSnapshots(
            HealthReport report,
            HealthThresholds thresholds,
            MaintenancePolicy policy,
            List<PlannedOperation> planned) {
        if (!policy.hasOperationConfig(OperationType.EXPIRE_SNAPSHOTS)) {
            return;
        }

        // Check snapshot count
        if (report.snapshotCount() >= thresholds.snapshotCountCritical()) {
            planned.add(
                    PlannedOperation.critical(
                            MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                            String.format(
                                    "Critical: %d snapshots (threshold: %d)",
                                    report.snapshotCount(), thresholds.snapshotCountCritical())));
            return;
        }
        if (report.snapshotCount() >= thresholds.snapshotCountWarning()) {
            planned.add(
                    PlannedOperation.warning(
                            MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                            String.format(
                                    "Warning: %d snapshots (threshold: %d)",
                                    report.snapshotCount(), thresholds.snapshotCountWarning())));
            return;
        }

        // Check oldest snapshot age
        Long oldestAgeDays = report.oldestSnapshotAgeDays();
        if (oldestAgeDays != null) {
            long ageDays = oldestAgeDays;
            if (ageDays >= thresholds.snapshotAgeCriticalDays()) {
                planned.add(
                        PlannedOperation.critical(
                                MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                                String.format(
                                        "Critical: oldest snapshot is %d days old (threshold: %d)",
                                        ageDays, thresholds.snapshotAgeCriticalDays())));
            } else if (ageDays >= thresholds.snapshotAgeWarningDays()) {
                planned.add(
                        PlannedOperation.warning(
                                MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                                String.format(
                                        "Warning: oldest snapshot is %d days old (threshold: %d)",
                                        ageDays, thresholds.snapshotAgeWarningDays())));
            }
        }
    }

    private void planRewriteManifests(
            HealthReport report,
            HealthThresholds thresholds,
            MaintenancePolicy policy,
            List<PlannedOperation> planned) {
        if (!policy.hasOperationConfig(OperationType.REWRITE_MANIFESTS)) {
            return;
        }

        // Check manifest count
        if (report.manifestCount() >= thresholds.manifestCountCritical()) {
            planned.add(
                    PlannedOperation.critical(
                            MaintenanceOperation.Type.REWRITE_MANIFESTS,
                            String.format(
                                    "Critical: %d manifests (threshold: %d)",
                                    report.manifestCount(), thresholds.manifestCountCritical())));
            return;
        }
        if (report.manifestCount() >= thresholds.manifestCountWarning()) {
            planned.add(
                    PlannedOperation.warning(
                            MaintenanceOperation.Type.REWRITE_MANIFESTS,
                            String.format(
                                    "Warning: %d manifests (threshold: %d)",
                                    report.manifestCount(), thresholds.manifestCountWarning())));
            return;
        }

        // Check manifest list size
        long manifestSizeBytes = report.totalManifestSizeBytes();
        if (manifestSizeBytes >= thresholds.manifestSizeCriticalBytes()) {
            planned.add(
                    PlannedOperation.critical(
                            MaintenanceOperation.Type.REWRITE_MANIFESTS,
                            String.format(
                                    "Critical: manifest list size %.2f MB (threshold: %.2f MB)",
                                    manifestSizeBytes / (1024.0 * 1024.0),
                                    thresholds.manifestSizeCriticalBytes() / (1024.0 * 1024.0))));
        } else if (manifestSizeBytes >= thresholds.manifestSizeWarningBytes()) {
            planned.add(
                    PlannedOperation.warning(
                            MaintenanceOperation.Type.REWRITE_MANIFESTS,
                            String.format(
                                    "Warning: manifest list size %.2f MB (threshold: %.2f MB)",
                                    manifestSizeBytes / (1024.0 * 1024.0),
                                    thresholds.manifestSizeWarningBytes() / (1024.0 * 1024.0))));
        }
    }

    private void planOrphanCleanup(
            HealthReport report,
            HealthThresholds thresholds,
            MaintenancePolicy policy,
            List<PlannedOperation> planned) {
        if (!policy.hasOperationConfig(OperationType.ORPHAN_CLEANUP)) {
            return;
        }

        // Check delete file count
        if (report.deleteFileCount() >= thresholds.deleteFileCountCritical()) {
            planned.add(
                    PlannedOperation.critical(
                            MaintenanceOperation.Type.ORPHAN_CLEANUP,
                            String.format(
                                    "Critical: %d delete files (threshold: %d)",
                                    report.deleteFileCount(),
                                    thresholds.deleteFileCountCritical())));
            return;
        }
        if (report.deleteFileCount() >= thresholds.deleteFileCountWarning()) {
            planned.add(
                    PlannedOperation.warning(
                            MaintenanceOperation.Type.ORPHAN_CLEANUP,
                            String.format(
                                    "Warning: %d delete files (threshold: %d)",
                                    report.deleteFileCount(),
                                    thresholds.deleteFileCountWarning())));
            return;
        }

        // Check delete file ratio
        double deleteRatio = report.deleteFileRatio();
        if (deleteRatio >= thresholds.deleteFileRatioCritical()) {
            planned.add(
                    PlannedOperation.critical(
                            MaintenanceOperation.Type.ORPHAN_CLEANUP,
                            String.format(
                                    "Critical: delete file ratio %.2f (threshold: %.2f)",
                                    deleteRatio, thresholds.deleteFileRatioCritical())));
        } else if (deleteRatio >= thresholds.deleteFileRatioWarning()) {
            planned.add(
                    PlannedOperation.warning(
                            MaintenanceOperation.Type.ORPHAN_CLEANUP,
                            String.format(
                                    "Warning: delete file ratio %.2f (threshold: %.2f)",
                                    deleteRatio, thresholds.deleteFileRatioWarning())));
        }
    }
}
