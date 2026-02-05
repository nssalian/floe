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

package com.floe.core.orchestrator;

import com.floe.core.health.HealthReport;
import com.floe.core.policy.OperationType;
import com.floe.core.policy.TriggerConditions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates trigger conditions against health metrics to determine if maintenance should run.
 *
 * <p>The evaluator checks various health signals against configured thresholds and determines
 * whether conditions are met for triggering maintenance operations. It uses OR logic - if any
 * condition is met, the operation should trigger.
 */
public class TriggerEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(TriggerEvaluator.class);

    /**
     * Result of evaluating trigger conditions.
     *
     * @param shouldTrigger whether the operation should be triggered
     * @param metConditions list of conditions that were met
     * @param unmetConditions list of conditions that were not met
     * @param nextEligibleTime earliest time when the operation can next be triggered (based on min
     *     interval)
     * @param forcedByCriticalPipeline true if triggered due to critical pipeline deadline
     */
    public record EvaluationResult(
            boolean shouldTrigger,
            List<String> metConditions,
            List<String> unmetConditions,
            Instant nextEligibleTime,
            boolean forcedByCriticalPipeline) {
        public static EvaluationResult alwaysTrigger() {
            return new EvaluationResult(
                    true, List.of("No conditions configured"), List.of(), null, false);
        }

        public static EvaluationResult blocked(String reason, Instant nextEligible) {
            return new EvaluationResult(false, List.of(), List.of(reason), nextEligible, false);
        }
    }

    /**
     * Evaluates whether trigger conditions are met for an operation.
     *
     * @param conditions the trigger conditions to evaluate (null means always trigger)
     * @param health the latest health report for the table
     * @param lastOperationTime the time of the last operation of this type (null if never run)
     * @param operationType the type of operation being evaluated
     * @param now the current time
     * @return evaluation result indicating whether to trigger and why
     */
    public EvaluationResult evaluate(
            TriggerConditions conditions,
            HealthReport health,
            Instant lastOperationTime,
            OperationType operationType,
            Instant now) {
        String tableName =
                health != null && health.tableIdentifier() != null
                        ? health.tableIdentifier().toQualifiedName()
                        : "unknown";

        // Null conditions means always trigger (backward compatible)
        if (conditions == null) {
            LOG.debug(
                    "No trigger conditions configured, always triggering (table={}, opType={})",
                    tableName,
                    operationType);
            return EvaluationResult.alwaysTrigger();
        }

        List<String> metConditions = new ArrayList<>();
        List<String> unmetConditions = new ArrayList<>();

        if (isCriticalPipelineOverdue(conditions, lastOperationTime, now)) {
            LOG.info(
                    "Critical pipeline deadline exceeded, forcing trigger (table={}, opType={})",
                    tableName,
                    operationType);
            return new EvaluationResult(
                    true, List.of("Critical pipeline deadline exceeded"), List.of(), null, true);
        }

        // Check minimum interval first
        Instant nextEligibleTime = null;
        Integer minInterval = conditions.getMinIntervalMinutes(operationType);
        if (minInterval != null && lastOperationTime != null) {
            Instant eligibleTime = lastOperationTime.plus(Duration.ofMinutes(minInterval));
            if (eligibleTime.isAfter(now)) {
                nextEligibleTime = eligibleTime;
                long minutesRemaining = Duration.between(now, eligibleTime).toMinutes();

                LOG.debug(
                        "Min interval not elapsed, {} minutes remaining until eligible (table={}, opType={})",
                        minutesRemaining,
                        tableName,
                        operationType);
                return EvaluationResult.blocked(
                        String.format("Min interval: %d minutes remaining", minutesRemaining),
                        eligibleTime);
            }
        }

        // If no health data, we can't evaluate conditions
        if (health == null) {
            // If conditions require health data but none is available, don't trigger
            if (conditions.hasConditions()) {
                LOG.debug(
                        "No health data available, cannot evaluate conditions (table={}, opType={})",
                        tableName,
                        operationType);
                return new EvaluationResult(
                        false,
                        List.of(),
                        List.of("No health data available"),
                        nextEligibleTime,
                        false);
            }
            return EvaluationResult.alwaysTrigger();
        }

        // Evaluate each condition (OR logic - any met condition triggers)
        boolean anyConditionMet = false;

        // Small file percentage
        if (conditions.smallFilePercentageAbove() != null) {
            double actual = health.smallFilePercentage();
            if (actual >= conditions.smallFilePercentageAbove()) {
                metConditions.add(
                        String.format(
                                "Small file %%: %.1f%% >= %.1f%%",
                                actual, conditions.smallFilePercentageAbove()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Small file %%: %.1f%% < %.1f%%",
                                actual, conditions.smallFilePercentageAbove()));
            }
        }

        // Small file count
        if (conditions.smallFileCountAbove() != null) {
            long actual = health.smallFileCount();
            if (actual >= conditions.smallFileCountAbove()) {
                metConditions.add(
                        String.format(
                                "Small file count: %d >= %d",
                                actual, conditions.smallFileCountAbove()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Small file count: %d < %d",
                                actual, conditions.smallFileCountAbove()));
            }
        }

        // Total file size
        if (conditions.totalFileSizeAboveBytes() != null) {
            long actual = health.totalDataSizeBytes();
            if (actual >= conditions.totalFileSizeAboveBytes()) {
                metConditions.add(
                        String.format(
                                "Total size: %d bytes >= %d bytes",
                                actual, conditions.totalFileSizeAboveBytes()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Total size: %d bytes < %d bytes",
                                actual, conditions.totalFileSizeAboveBytes()));
            }
        }

        // Delete file count
        if (conditions.deleteFileCountAbove() != null) {
            int actual = health.deleteFileCount();
            if (actual >= conditions.deleteFileCountAbove()) {
                metConditions.add(
                        String.format(
                                "Delete file count: %d >= %d",
                                actual, conditions.deleteFileCountAbove()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Delete file count: %d < %d",
                                actual, conditions.deleteFileCountAbove()));
            }
        }

        // Delete file ratio
        if (conditions.deleteFileRatioAbove() != null) {
            double actual = health.deleteFileRatio();
            if (actual >= conditions.deleteFileRatioAbove()) {
                metConditions.add(
                        String.format(
                                "Delete file ratio: %.2f >= %.2f",
                                actual, conditions.deleteFileRatioAbove()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Delete file ratio: %.2f < %.2f",
                                actual, conditions.deleteFileRatioAbove()));
            }
        }

        // Snapshot count
        if (conditions.snapshotCountAbove() != null) {
            int actual = health.snapshotCount();
            if (actual >= conditions.snapshotCountAbove()) {
                metConditions.add(
                        String.format(
                                "Snapshot count: %d >= %d",
                                actual, conditions.snapshotCountAbove()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Snapshot count: %d < %d",
                                actual, conditions.snapshotCountAbove()));
            }
        }

        // Snapshot age
        if (conditions.snapshotAgeAboveDays() != null && health.oldestSnapshotAgeDays() != null) {
            long actual = health.oldestSnapshotAgeDays();
            if (actual >= conditions.snapshotAgeAboveDays()) {
                metConditions.add(
                        String.format(
                                "Snapshot age: %d days >= %d days",
                                actual, conditions.snapshotAgeAboveDays()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Snapshot age: %d days < %d days",
                                actual, conditions.snapshotAgeAboveDays()));
            }
        }

        // Partition count
        if (conditions.partitionCountAbove() != null) {
            int actual = health.partitionCount();
            if (actual >= conditions.partitionCountAbove()) {
                metConditions.add(
                        String.format(
                                "Partition count: %d >= %d",
                                actual, conditions.partitionCountAbove()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Partition count: %d < %d",
                                actual, conditions.partitionCountAbove()));
            }
        }

        // Partition skew
        if (conditions.partitionSkewAbove() != null) {
            double actual = health.partitionSkew();
            if (actual >= conditions.partitionSkewAbove()) {
                metConditions.add(
                        String.format(
                                "Partition skew: %.2fx >= %.2fx",
                                actual, conditions.partitionSkewAbove()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Partition skew: %.2fx < %.2fx",
                                actual, conditions.partitionSkewAbove()));
            }
        }

        // Manifest size
        if (conditions.manifestSizeAboveBytes() != null) {
            long actual = health.totalManifestSizeBytes();
            if (actual >= conditions.manifestSizeAboveBytes()) {
                metConditions.add(
                        String.format(
                                "Manifest size: %d bytes >= %d bytes",
                                actual, conditions.manifestSizeAboveBytes()));
                anyConditionMet = true;
            } else {
                unmetConditions.add(
                        String.format(
                                "Manifest size: %d bytes < %d bytes",
                                actual, conditions.manifestSizeAboveBytes()));
            }
        }

        // If no conditions were specified, always trigger
        if (!conditions.hasConditions()) {
            LOG.debug("No threshold conditions configured, triggering");
            return new EvaluationResult(
                    true, List.of("No conditions configured"), List.of(), null, false);
        }

        boolean shouldTrigger = shouldTrigger(conditions, anyConditionMet, unmetConditions);
        LOG.debug(
                "Trigger evaluation complete: shouldTrigger={}, metConditions={}, unmetConditions={} (table={}, opType={})",
                shouldTrigger,
                metConditions.size(),
                unmetConditions.size(),
                tableName,
                operationType);

        return new EvaluationResult(
                shouldTrigger, metConditions, unmetConditions, nextEligibleTime, false);
    }

    private boolean shouldTrigger(
            TriggerConditions conditions, boolean anyConditionMet, List<String> unmetConditions) {
        if (conditions.triggerLogic() == TriggerConditions.TriggerLogic.AND) {
            return conditions.hasConditions() && unmetConditions.isEmpty();
        }
        return anyConditionMet;
    }

    private boolean isCriticalPipelineOverdue(
            TriggerConditions conditions, Instant lastOperationTime, Instant now) {
        if (!conditions.isCriticalPipeline()
                || conditions.criticalPipelineMaxDelayMinutes() == null
                || lastOperationTime == null) {
            return false;
        }
        Instant maxDelayTime =
                lastOperationTime.plus(
                        Duration.ofMinutes(conditions.criticalPipelineMaxDelayMinutes()));
        return !now.isBefore(maxDelayTime);
    }
}
