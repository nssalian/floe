package com.floe.core.health;

/**
 * Configurable thresholds for determining health issues.
 *
 * @param smallFileSizeBytes size below which file is "small"
 * @param smallFilePercentWarning warning threshold for small file %
 * @param smallFilePercentCritical critical threshold for small file %
 * @param largeFileSizeBytes size above which file is "large"
 * @param largeFilePercentWarning warning threshold for large file %
 * @param fileCountWarning warning threshold for total files
 * @param fileCountCritical critical threshold for total files
 * @param snapshotCountWarning warning threshold for snapshots
 * @param snapshotCountCritical critical threshold for snapshots
 * @param snapshotAgeWarningDays warning threshold for snapshot age
 * @param snapshotAgeCriticalDays critical threshold for snapshot age
 * @param deleteFileCountWarning warning threshold for delete files
 * @param deleteFileCountCritical critical threshold for delete files
 * @param manifestCountWarning warning threshold for manifests
 * @param manifestCountCritical critical threshold for manifests
 */
public record HealthThresholds(
        // Small file thresholds
        long smallFileSizeBytes,
        double smallFilePercentWarning,
        double smallFilePercentCritical,

        // Large file thresholds
        long largeFileSizeBytes,
        double largeFilePercentWarning,

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

        // Manifest thresholds
        int manifestCountWarning,
        int manifestCountCritical) {
    /** Default thresholds based on common Iceberg best practices. */
    public static HealthThresholds defaults() {
        return new HealthThresholds(
                100 * 1024 * 1024, // 100 MB small file threshold
                20.0, // 20% small files = warning
                50.0, // 50% small files = critical
                1024 * 1024 * 1024L, // 1 GB large file threshold
                20.0, // 20% large files = warning
                10_000, // 10k files = warning
                50_000, // 50k files = critical
                100, // 100 snapshots = warning
                500, // 500 snapshots = critical
                7, // 7 days old = warning
                30, // 30 days old = critical
                100, // 100 delete files = warning
                500, // 500 delete files = critical
                100, // 100 manifests = warning
                500 // 500 manifests = critical
                );
    }

    /** Strict thresholds for high-performance tables. */
    public static HealthThresholds strict() {
        return new HealthThresholds(
                128 * 1024 * 1024, // 128 MB small file threshold
                10.0, // 10% small files = warning
                25.0, // 25% small files = critical
                512 * 1024 * 1024L, // 512 MB large file threshold
                10.0, // 10% large files = warning
                5_000, // 5k files = warning
                20_000, // 20k files = critical
                50, // 50 snapshots = warning
                200, // 200 snapshots = critical
                3, // 3 days old = warning
                14, // 14 days old = critical
                50, // 50 delete files = warning
                200, // 200 delete files = critical
                50, // 50 manifests = warning
                200 // 200 manifests = critical
                );
    }

    /** Relaxed thresholds for archival or low-priority tables. */
    public static HealthThresholds relaxed() {
        return new HealthThresholds(
                64 * 1024 * 1024, // 64 MB small file threshold
                40.0, // 40% small files = warning
                70.0, // 70% small files = critical
                2 * 1024 * 1024 * 1024L, // 2 GB large file threshold
                30.0, // 30% large files = warning
                50_000, // 50k files = warning
                200_000, // 200k files = critical
                500, // 500 snapshots = warning
                2000, // 2000 snapshots = critical
                30, // 30 days old = warning
                90, // 90 days old = critical
                500, // 500 delete files = warning
                2000, // 2000 delete files = critical
                500, // 500 manifests = warning
                2000 // 2000 manifests = critical
                );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private long smallFileSizeBytes = 100 * 1024 * 1024;
        private double smallFilePercentWarning = 20.0;
        private double smallFilePercentCritical = 50.0;
        private long largeFileSizeBytes = 1024 * 1024 * 1024L;
        private double largeFilePercentWarning = 20.0;
        private int fileCountWarning = 10_000;
        private int fileCountCritical = 50_000;
        private int snapshotCountWarning = 100;
        private int snapshotCountCritical = 500;
        private int snapshotAgeWarningDays = 7;
        private int snapshotAgeCriticalDays = 30;
        private int deleteFileCountWarning = 100;
        private int deleteFileCountCritical = 500;
        private int manifestCountWarning = 100;
        private int manifestCountCritical = 500;

        public Builder smallFileSizeBytes(long smallFileSizeBytes) {
            this.smallFileSizeBytes = smallFileSizeBytes;
            return this;
        }

        public Builder smallFilePercentWarning(double smallFilePercentWarning) {
            this.smallFilePercentWarning = smallFilePercentWarning;
            return this;
        }

        public Builder smallFilePercentCritical(double smallFilePercentCritical) {
            this.smallFilePercentCritical = smallFilePercentCritical;
            return this;
        }

        public Builder largeFileSizeBytes(long largeFileSizeBytes) {
            this.largeFileSizeBytes = largeFileSizeBytes;
            return this;
        }

        public Builder largeFilePercentWarning(double largeFilePercentWarning) {
            this.largeFilePercentWarning = largeFilePercentWarning;
            return this;
        }

        public Builder fileCountWarning(int fileCountWarning) {
            this.fileCountWarning = fileCountWarning;
            return this;
        }

        public Builder fileCountCritical(int fileCountCritical) {
            this.fileCountCritical = fileCountCritical;
            return this;
        }

        public Builder snapshotCountWarning(int snapshotCountWarning) {
            this.snapshotCountWarning = snapshotCountWarning;
            return this;
        }

        public Builder snapshotCountCritical(int snapshotCountCritical) {
            this.snapshotCountCritical = snapshotCountCritical;
            return this;
        }

        public Builder snapshotAgeWarningDays(int snapshotAgeWarningDays) {
            this.snapshotAgeWarningDays = snapshotAgeWarningDays;
            return this;
        }

        public Builder snapshotAgeCriticalDays(int snapshotAgeCriticalDays) {
            this.snapshotAgeCriticalDays = snapshotAgeCriticalDays;
            return this;
        }

        public Builder deleteFileCountWarning(int deleteFileCountWarning) {
            this.deleteFileCountWarning = deleteFileCountWarning;
            return this;
        }

        public Builder deleteFileCountCritical(int deleteFileCountCritical) {
            this.deleteFileCountCritical = deleteFileCountCritical;
            return this;
        }

        public Builder manifestCountWarning(int manifestCountWarning) {
            this.manifestCountWarning = manifestCountWarning;
            return this;
        }

        public Builder manifestCountCritical(int manifestCountCritical) {
            this.manifestCountCritical = manifestCountCritical;
            return this;
        }

        public HealthThresholds build() {
            return new HealthThresholds(
                    smallFileSizeBytes,
                    smallFilePercentWarning,
                    smallFilePercentCritical,
                    largeFileSizeBytes,
                    largeFilePercentWarning,
                    fileCountWarning,
                    fileCountCritical,
                    snapshotCountWarning,
                    snapshotCountCritical,
                    snapshotAgeWarningDays,
                    snapshotAgeCriticalDays,
                    deleteFileCountWarning,
                    deleteFileCountCritical,
                    manifestCountWarning,
                    manifestCountCritical);
        }
    }
}
