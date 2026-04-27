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

package com.floe.core.policy;

import java.util.Map;

/**
 * Conditions that must be met before a maintenance operation is triggered.
 *
 * <p>When trigger conditions are set on a policy, the scheduler will evaluate health metrics
 * against these thresholds before executing maintenance. Operations are only triggered when at
 * least one condition is met (OR logic), unless no conditions are specified (always trigger).
 *
 * @param smallFilePercentageAbove trigger when small file percentage exceeds this value
 * @param smallFileCountAbove trigger when small file count exceeds this value
 * @param totalFileSizeAboveBytes trigger when total file size exceeds this value
 * @param deleteFileCountAbove trigger when delete file count exceeds this value
 * @param deleteFileRatioAbove trigger when delete file ratio exceeds this value
 * @param snapshotCountAbove trigger when snapshot count exceeds this value
 * @param snapshotAgeAboveDays trigger when oldest snapshot age exceeds this value
 * @param partitionCountAbove trigger when partition count exceeds this value
 * @param partitionSkewAbove trigger when partition skew exceeds this value
 * @param manifestSizeAboveBytes trigger when manifest size exceeds this value
 * @param minIntervalMinutes minimum time between operations (global)
 * @param perOperationMinIntervalMinutes minimum intervals per operation type
 * @param criticalPipeline if true, force trigger when max delay is exceeded
 * @param criticalPipelineMaxDelayMinutes max delay before forcing trigger for critical pipelines
 * @param triggerLogic how multiple conditions are combined (OR or AND)
 */
public record TriggerConditions(
        // Data file conditions
        Double smallFilePercentageAbove,
        Long smallFileCountAbove,
        Long totalFileSizeAboveBytes,

        // Delete file conditions
        Integer deleteFileCountAbove,
        Double deleteFileRatioAbove,

        // Snapshot conditions
        Integer snapshotCountAbove,
        Integer snapshotAgeAboveDays,

        // Partition conditions
        Integer partitionCountAbove,
        Double partitionSkewAbove,

        // Manifest conditions
        Long manifestSizeAboveBytes,

        // Time-based gates
        Integer minIntervalMinutes,
        Map<OperationType, Integer> perOperationMinIntervalMinutes,

        // Critical pipeline settings
        Boolean criticalPipeline,
        Integer criticalPipelineMaxDelayMinutes,
        TriggerLogic triggerLogic) {
    /** Compact constructor to ensure nulls are handled consistently. */
    public TriggerConditions {
        if (perOperationMinIntervalMinutes == null) {
            perOperationMinIntervalMinutes = Map.of();
        }
        if (criticalPipeline == null) {
            criticalPipeline = false;
        }
        if (triggerLogic == null) {
            triggerLogic = TriggerLogic.OR;
        }
    }

    /**
     * Returns default trigger conditions with reasonable thresholds.
     *
     * <p>These defaults trigger when tables show moderate maintenance need.
     */
    public static TriggerConditions defaults() {
        return new TriggerConditions(
                20.0, // 20% small files
                100L, // 100 small files
                null, // no total size trigger
                50, // 50 delete files
                0.10, // 10% delete ratio
                50, // 50 snapshots
                7, // 7 days old
                null, // no partition count trigger
                null, // no partition skew trigger
                100 * 1024 * 1024L, // 100 MB manifest size
                60, // 60 minute minimum interval
                Map.of(),
                false,
                null,
                TriggerLogic.OR);
    }

    /**
     * Returns trigger conditions that always trigger (no conditions checked).
     *
     * <p>Use this for backward compatibility or when you want pure cron-based scheduling.
     */
    public static TriggerConditions alwaysTrigger() {
        return new TriggerConditions(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Map.of(),
                false,
                null,
                TriggerLogic.OR);
    }

    /**
     * Returns conservative trigger conditions with higher thresholds.
     *
     * <p>Use this for tables where you want to minimize maintenance frequency.
     */
    public static TriggerConditions conservative() {
        return new TriggerConditions(
                40.0, // 40% small files
                500L, // 500 small files
                null, // no total size trigger
                200, // 200 delete files
                0.25, // 25% delete ratio
                100, // 100 snapshots
                14, // 14 days old
                null, // no partition count trigger
                null, // no partition skew trigger
                500 * 1024 * 1024L, // 500 MB manifest size
                120, // 120 minute minimum interval
                Map.of(),
                false,
                null,
                TriggerLogic.OR);
    }

    public enum TriggerLogic {
        OR,
        AND,
    }

    /**
     * Checks if this has any conditions that would gate execution.
     *
     * @return true if at least one threshold condition is set
     */
    public boolean hasConditions() {
        return (smallFilePercentageAbove != null
                || smallFileCountAbove != null
                || totalFileSizeAboveBytes != null
                || deleteFileCountAbove != null
                || deleteFileRatioAbove != null
                || snapshotCountAbove != null
                || snapshotAgeAboveDays != null
                || partitionCountAbove != null
                || partitionSkewAbove != null
                || manifestSizeAboveBytes != null);
    }

    /**
     * Checks if this is a critical pipeline configuration.
     *
     * @return true if critical pipeline is enabled
     */
    public boolean isCriticalPipeline() {
        return Boolean.TRUE.equals(criticalPipeline);
    }

    /**
     * Gets the minimum interval for a specific operation type.
     *
     * @param operationType the operation type
     * @return the minimum interval in minutes, or the global minimum if not specified
     */
    public Integer getMinIntervalMinutes(OperationType operationType) {
        if (perOperationMinIntervalMinutes.containsKey(operationType)) {
            return perOperationMinIntervalMinutes.get(operationType);
        }
        return minIntervalMinutes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Double smallFilePercentageAbove;
        private Long smallFileCountAbove;
        private Long totalFileSizeAboveBytes;
        private Integer deleteFileCountAbove;
        private Double deleteFileRatioAbove;
        private Integer snapshotCountAbove;
        private Integer snapshotAgeAboveDays;
        private Integer partitionCountAbove;
        private Double partitionSkewAbove;
        private Long manifestSizeAboveBytes;
        private Integer minIntervalMinutes;
        private Map<OperationType, Integer> perOperationMinIntervalMinutes = Map.of();
        private Boolean criticalPipeline = false;
        private Integer criticalPipelineMaxDelayMinutes;
        private TriggerLogic triggerLogic = TriggerLogic.OR;

        public Builder smallFilePercentageAbove(Double smallFilePercentageAbove) {
            this.smallFilePercentageAbove = smallFilePercentageAbove;
            return this;
        }

        public Builder smallFileCountAbove(Long smallFileCountAbove) {
            this.smallFileCountAbove = smallFileCountAbove;
            return this;
        }

        public Builder totalFileSizeAboveBytes(Long totalFileSizeAboveBytes) {
            this.totalFileSizeAboveBytes = totalFileSizeAboveBytes;
            return this;
        }

        public Builder deleteFileCountAbove(Integer deleteFileCountAbove) {
            this.deleteFileCountAbove = deleteFileCountAbove;
            return this;
        }

        public Builder deleteFileRatioAbove(Double deleteFileRatioAbove) {
            this.deleteFileRatioAbove = deleteFileRatioAbove;
            return this;
        }

        public Builder snapshotCountAbove(Integer snapshotCountAbove) {
            this.snapshotCountAbove = snapshotCountAbove;
            return this;
        }

        public Builder snapshotAgeAboveDays(Integer snapshotAgeAboveDays) {
            this.snapshotAgeAboveDays = snapshotAgeAboveDays;
            return this;
        }

        public Builder partitionCountAbove(Integer partitionCountAbove) {
            this.partitionCountAbove = partitionCountAbove;
            return this;
        }

        public Builder partitionSkewAbove(Double partitionSkewAbove) {
            this.partitionSkewAbove = partitionSkewAbove;
            return this;
        }

        public Builder manifestSizeAboveBytes(Long manifestSizeAboveBytes) {
            this.manifestSizeAboveBytes = manifestSizeAboveBytes;
            return this;
        }

        public Builder minIntervalMinutes(Integer minIntervalMinutes) {
            this.minIntervalMinutes = minIntervalMinutes;
            return this;
        }

        public Builder perOperationMinIntervalMinutes(
                Map<OperationType, Integer> perOperationMinIntervalMinutes) {
            this.perOperationMinIntervalMinutes =
                    perOperationMinIntervalMinutes != null
                            ? Map.copyOf(perOperationMinIntervalMinutes)
                            : Map.of();
            return this;
        }

        public Builder criticalPipeline(Boolean criticalPipeline) {
            this.criticalPipeline = criticalPipeline;
            return this;
        }

        public Builder criticalPipelineMaxDelayMinutes(Integer criticalPipelineMaxDelayMinutes) {
            this.criticalPipelineMaxDelayMinutes = criticalPipelineMaxDelayMinutes;
            return this;
        }

        public Builder triggerLogic(TriggerLogic triggerLogic) {
            this.triggerLogic = triggerLogic;
            return this;
        }

        public TriggerConditions build() {
            return new TriggerConditions(
                    smallFilePercentageAbove,
                    smallFileCountAbove,
                    totalFileSizeAboveBytes,
                    deleteFileCountAbove,
                    deleteFileRatioAbove,
                    snapshotCountAbove,
                    snapshotAgeAboveDays,
                    partitionCountAbove,
                    partitionSkewAbove,
                    manifestSizeAboveBytes,
                    minIntervalMinutes,
                    perOperationMinIntervalMinutes,
                    criticalPipeline,
                    criticalPipelineMaxDelayMinutes,
                    triggerLogic);
        }
    }
}
