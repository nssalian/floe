package com.floe.server.api;

import com.floe.core.health.HealthThresholds;

/**
 * Response DTO containing all health threshold values.
 *
 * <p>Exposes the thresholds used during health assessment so clients can understand what values
 * triggered issues.
 */
public record HealthThresholdsResponse(
        // Small file thresholds
        long smallFileSizeBytes,
        double smallFilePercentWarning,
        double smallFilePercentCritical,

        // Large file thresholds
        long largeFileSizeBytes,
        double largeFilePercentWarning,
        double largeFilePercentCritical,

        // File count thresholds
        int fileCountWarning,
        int fileCountCritical,

        // Snapshot thresholds
        int snapshotCountWarning,
        int snapshotCountCritical,
        int snapshotAgeWarningDays,
        int snapshotAgeCriticalDays,

        // Delete file thresholds
        int deleteFileCountWarning,
        int deleteFileCountCritical,
        double deleteFileRatioWarning,
        double deleteFileRatioCritical,

        // Manifest thresholds
        int manifestCountWarning,
        int manifestCountCritical,
        long manifestSizeWarningBytes,
        long manifestSizeCriticalBytes,

        // Partition thresholds
        int partitionCountWarning,
        int partitionCountCritical,
        double partitionSkewWarning,
        double partitionSkewCritical,

        // Stale metadata thresholds
        int staleMetadataWarningDays,
        int staleMetadataCriticalDays) {
    /**
     * Create a response from core HealthThresholds.
     *
     * @param thresholds the core thresholds object
     * @return the response DTO
     */
    public static HealthThresholdsResponse from(HealthThresholds thresholds) {
        return new HealthThresholdsResponse(
                thresholds.smallFileSizeBytes(),
                thresholds.smallFilePercentWarning(),
                thresholds.smallFilePercentCritical(),
                thresholds.largeFileSizeBytes(),
                thresholds.largeFilePercentWarning(),
                thresholds.largeFilePercentCritical(),
                thresholds.fileCountWarning(),
                thresholds.fileCountCritical(),
                thresholds.snapshotCountWarning(),
                thresholds.snapshotCountCritical(),
                thresholds.snapshotAgeWarningDays(),
                thresholds.snapshotAgeCriticalDays(),
                thresholds.deleteFileCountWarning(),
                thresholds.deleteFileCountCritical(),
                thresholds.deleteFileRatioWarning(),
                thresholds.deleteFileRatioCritical(),
                thresholds.manifestCountWarning(),
                thresholds.manifestCountCritical(),
                thresholds.manifestSizeWarningBytes(),
                thresholds.manifestSizeCriticalBytes(),
                thresholds.partitionCountWarning(),
                thresholds.partitionCountCritical(),
                thresholds.partitionSkewWarning(),
                thresholds.partitionSkewCritical(),
                thresholds.staleMetadataWarningDays(),
                thresholds.staleMetadataCriticalDays());
    }
}
