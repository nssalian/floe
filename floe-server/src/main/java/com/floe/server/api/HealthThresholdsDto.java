package com.floe.server.api;

import com.floe.core.health.HealthThresholds;

/**
 * DTO for health thresholds in policy API requests.
 *
 * <p>All fields are optional - unset fields will use default values from HealthThresholds.
 */
public record HealthThresholdsDto(
        // Small file thresholds
        Long smallFileSizeBytes,
        Double smallFilePercentWarning,
        Double smallFilePercentCritical,

        // Large file thresholds
        Long largeFileSizeBytes,
        Double largeFilePercentWarning,
        Double largeFilePercentCritical,

        // File count thresholds
        Integer fileCountWarning,
        Integer fileCountCritical,

        // Snapshot thresholds
        Integer snapshotCountWarning,
        Integer snapshotCountCritical,
        Integer snapshotAgeWarningDays,
        Integer snapshotAgeCriticalDays,

        // Delete file thresholds
        Integer deleteFileCountWarning,
        Integer deleteFileCountCritical,
        Double deleteFileRatioWarning,
        Double deleteFileRatioCritical,

        // Manifest thresholds
        Integer manifestCountWarning,
        Integer manifestCountCritical,
        Long manifestSizeWarningBytes,
        Long manifestSizeCriticalBytes,

        // Partition thresholds
        Integer partitionCountWarning,
        Integer partitionCountCritical,
        Double partitionSkewWarning,
        Double partitionSkewCritical,

        // Stale metadata thresholds
        Integer staleMetadataWarningDays,
        Integer staleMetadataCriticalDays) {
    /**
     * Convert to domain HealthThresholds. Uses defaults for any unset fields.
     *
     * @return HealthThresholds with values from this DTO or defaults
     */
    public HealthThresholds toThresholds() {
        HealthThresholds defaults = HealthThresholds.defaults();

        long resolvedSmallFileSizeBytes =
                smallFileSizeBytes != null ? smallFileSizeBytes : defaults.smallFileSizeBytes();
        double resolvedSmallFilePercentWarning =
                smallFilePercentWarning != null
                        ? smallFilePercentWarning
                        : defaults.smallFilePercentWarning();
        double resolvedSmallFilePercentCritical =
                smallFilePercentCritical != null
                        ? smallFilePercentCritical
                        : defaults.smallFilePercentCritical();
        long resolvedLargeFileSizeBytes =
                largeFileSizeBytes != null ? largeFileSizeBytes : defaults.largeFileSizeBytes();
        double resolvedLargeFilePercentWarning =
                largeFilePercentWarning != null
                        ? largeFilePercentWarning
                        : defaults.largeFilePercentWarning();
        double resolvedLargeFilePercentCritical =
                largeFilePercentCritical != null
                        ? largeFilePercentCritical
                        : defaults.largeFilePercentCritical();
        int resolvedFileCountWarning =
                fileCountWarning != null ? fileCountWarning : defaults.fileCountWarning();
        int resolvedFileCountCritical =
                fileCountCritical != null ? fileCountCritical : defaults.fileCountCritical();
        int resolvedSnapshotCountWarning =
                snapshotCountWarning != null
                        ? snapshotCountWarning
                        : defaults.snapshotCountWarning();
        int resolvedSnapshotCountCritical =
                snapshotCountCritical != null
                        ? snapshotCountCritical
                        : defaults.snapshotCountCritical();
        int resolvedSnapshotAgeWarningDays =
                snapshotAgeWarningDays != null
                        ? snapshotAgeWarningDays
                        : defaults.snapshotAgeWarningDays();
        int resolvedSnapshotAgeCriticalDays =
                snapshotAgeCriticalDays != null
                        ? snapshotAgeCriticalDays
                        : defaults.snapshotAgeCriticalDays();
        int resolvedDeleteFileCountWarning =
                deleteFileCountWarning != null
                        ? deleteFileCountWarning
                        : defaults.deleteFileCountWarning();
        int resolvedDeleteFileCountCritical =
                deleteFileCountCritical != null
                        ? deleteFileCountCritical
                        : defaults.deleteFileCountCritical();
        double resolvedDeleteFileRatioWarning =
                deleteFileRatioWarning != null
                        ? deleteFileRatioWarning
                        : defaults.deleteFileRatioWarning();
        double resolvedDeleteFileRatioCritical =
                deleteFileRatioCritical != null
                        ? deleteFileRatioCritical
                        : defaults.deleteFileRatioCritical();
        int resolvedManifestCountWarning =
                manifestCountWarning != null
                        ? manifestCountWarning
                        : defaults.manifestCountWarning();
        int resolvedManifestCountCritical =
                manifestCountCritical != null
                        ? manifestCountCritical
                        : defaults.manifestCountCritical();
        long resolvedManifestSizeWarningBytes =
                manifestSizeWarningBytes != null
                        ? manifestSizeWarningBytes
                        : defaults.manifestSizeWarningBytes();
        long resolvedManifestSizeCriticalBytes =
                manifestSizeCriticalBytes != null
                        ? manifestSizeCriticalBytes
                        : defaults.manifestSizeCriticalBytes();
        int resolvedPartitionCountWarning =
                partitionCountWarning != null
                        ? partitionCountWarning
                        : defaults.partitionCountWarning();
        int resolvedPartitionCountCritical =
                partitionCountCritical != null
                        ? partitionCountCritical
                        : defaults.partitionCountCritical();
        double resolvedPartitionSkewWarning =
                partitionSkewWarning != null
                        ? partitionSkewWarning
                        : defaults.partitionSkewWarning();
        double resolvedPartitionSkewCritical =
                partitionSkewCritical != null
                        ? partitionSkewCritical
                        : defaults.partitionSkewCritical();
        int resolvedStaleMetadataWarningDays =
                staleMetadataWarningDays != null
                        ? staleMetadataWarningDays
                        : defaults.staleMetadataWarningDays();
        int resolvedStaleMetadataCriticalDays =
                staleMetadataCriticalDays != null
                        ? staleMetadataCriticalDays
                        : defaults.staleMetadataCriticalDays();

        validateRange(
                "smallFilePercent",
                resolvedSmallFilePercentWarning,
                resolvedSmallFilePercentCritical);
        validateRange(
                "largeFilePercent",
                resolvedLargeFilePercentWarning,
                resolvedLargeFilePercentCritical);
        validateRange("fileCount", resolvedFileCountWarning, resolvedFileCountCritical);
        validateRange("snapshotCount", resolvedSnapshotCountWarning, resolvedSnapshotCountCritical);
        validateRange(
                "snapshotAgeDays", resolvedSnapshotAgeWarningDays, resolvedSnapshotAgeCriticalDays);
        validateRange(
                "deleteFileCount", resolvedDeleteFileCountWarning, resolvedDeleteFileCountCritical);
        validateRange(
                "deleteFileRatio", resolvedDeleteFileRatioWarning, resolvedDeleteFileRatioCritical);
        validateRange("manifestCount", resolvedManifestCountWarning, resolvedManifestCountCritical);
        validateRange(
                "manifestSizeBytes",
                resolvedManifestSizeWarningBytes,
                resolvedManifestSizeCriticalBytes);
        validateRange(
                "partitionCount", resolvedPartitionCountWarning, resolvedPartitionCountCritical);
        validateRange("partitionSkew", resolvedPartitionSkewWarning, resolvedPartitionSkewCritical);
        validateRange(
                "staleMetadataDays",
                resolvedStaleMetadataWarningDays,
                resolvedStaleMetadataCriticalDays);

        return new HealthThresholds(
                resolvedSmallFileSizeBytes,
                resolvedSmallFilePercentWarning,
                resolvedSmallFilePercentCritical,
                resolvedLargeFileSizeBytes,
                resolvedLargeFilePercentWarning,
                resolvedLargeFilePercentCritical,
                resolvedFileCountWarning,
                resolvedFileCountCritical,
                resolvedSnapshotCountWarning,
                resolvedSnapshotCountCritical,
                resolvedSnapshotAgeWarningDays,
                resolvedSnapshotAgeCriticalDays,
                resolvedDeleteFileCountWarning,
                resolvedDeleteFileCountCritical,
                resolvedDeleteFileRatioWarning,
                resolvedDeleteFileRatioCritical,
                resolvedManifestCountWarning,
                resolvedManifestCountCritical,
                resolvedManifestSizeWarningBytes,
                resolvedManifestSizeCriticalBytes,
                resolvedPartitionCountWarning,
                resolvedPartitionCountCritical,
                resolvedPartitionSkewWarning,
                resolvedPartitionSkewCritical,
                resolvedStaleMetadataWarningDays,
                resolvedStaleMetadataCriticalDays);
    }

    private static void validateRange(String name, long warningValue, long criticalValue) {
        if (warningValue >= criticalValue) {
            throw new IllegalArgumentException(
                    name
                            + " thresholds must satisfy warning < critical (warning="
                            + warningValue
                            + ", critical="
                            + criticalValue
                            + ")");
        }
    }

    private static void validateRange(String name, double warningValue, double criticalValue) {
        if (warningValue >= criticalValue) {
            throw new IllegalArgumentException(
                    name
                            + " thresholds must satisfy warning < critical (warning="
                            + warningValue
                            + ", critical="
                            + criticalValue
                            + ")");
        }
    }

    /**
     * Create a DTO from domain HealthThresholds.
     *
     * @param thresholds the domain thresholds
     * @return DTO representation
     */
    public static HealthThresholdsDto from(HealthThresholds thresholds) {
        if (thresholds == null) {
            return null;
        }
        return new HealthThresholdsDto(
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
