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
 * @param deleteFileRatioWarning warning threshold for delete file ratio (deleteFiles/dataFiles)
 * @param deleteFileRatioCritical critical threshold for delete file ratio
 * @param manifestCountWarning warning threshold for manifests
 * @param manifestCountCritical critical threshold for manifests
 * @param manifestSizeWarningBytes warning threshold for total manifest size
 * @param manifestSizeCriticalBytes critical threshold for total manifest size
 * @param partitionCountWarning warning threshold for partition count
 * @param partitionCountCritical critical threshold for partition count
 * @param partitionSkewWarning warning threshold for partition skew (max/avg files per partition)
 * @param partitionSkewCritical critical threshold for partition skew
 * @param staleMetadataWarningDays warning threshold for days since last write
 * @param staleMetadataCriticalDays critical threshold for days since last write
 */
public record HealthThresholds(
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
    /** Default thresholds based on common Iceberg best practices. */
    public static HealthThresholds defaults() {
        return new HealthThresholds(
                100 * 1024 * 1024, // 100 MB small file threshold
                20.0, // 20% small files = warning
                50.0, // 50% small files = critical
                1024 * 1024 * 1024L, // 1 GB large file threshold
                20.0, // 20% large files = warning
                50.0, // 50% large files = critical
                10_000, // 10k files = warning
                50_000, // 50k files = critical
                100, // 100 snapshots = warning
                500, // 500 snapshots = critical
                7, // 7 days old = warning
                30, // 30 days old = critical
                100, // 100 delete files = warning
                500, // 500 delete files = critical
                0.10, // 10% delete file ratio = warning
                0.25, // 25% delete file ratio = critical
                100, // 100 manifests = warning
                500, // 500 manifests = critical
                100 * 1024 * 1024L, // 100 MB manifest size = warning
                500 * 1024 * 1024L, // 500 MB manifest size = critical
                5_000, // 5k partitions = warning
                10_000, // 10k partitions = critical
                3.0, // 3x partition skew = warning
                10.0, // 10x partition skew = critical
                7, // 7 days stale = warning
                30 // 30 days stale = critical
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
                25.0, // 25% large files = critical
                5_000, // 5k files = warning
                20_000, // 20k files = critical
                50, // 50 snapshots = warning
                200, // 200 snapshots = critical
                3, // 3 days old = warning
                14, // 14 days old = critical
                50, // 50 delete files = warning
                200, // 200 delete files = critical
                0.05, // 5% delete file ratio = warning
                0.15, // 15% delete file ratio = critical
                50, // 50 manifests = warning
                200, // 200 manifests = critical
                50 * 1024 * 1024L, // 50 MB manifest size = warning
                200 * 1024 * 1024L, // 200 MB manifest size = critical
                2_000, // 2k partitions = warning
                5_000, // 5k partitions = critical
                2.0, // 2x partition skew = warning
                5.0, // 5x partition skew = critical
                3, // 3 days stale = warning
                14 // 14 days stale = critical
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
                70.0, // 70% large files = critical
                50_000, // 50k files = warning
                200_000, // 200k files = critical
                500, // 500 snapshots = warning
                2000, // 2000 snapshots = critical
                30, // 30 days old = warning
                90, // 90 days old = critical
                500, // 500 delete files = warning
                2000, // 2000 delete files = critical
                0.20, // 20% delete file ratio = warning
                0.50, // 50% delete file ratio = critical
                500, // 500 manifests = warning
                2000, // 2000 manifests = critical
                200 * 1024 * 1024L, // 200 MB manifest size = warning
                1024 * 1024 * 1024L, // 1 GB manifest size = critical
                10_000, // 10k partitions = warning
                50_000, // 50k partitions = critical
                5.0, // 5x partition skew = warning
                20.0, // 20x partition skew = critical
                30, // 30 days stale = warning
                90 // 90 days stale = critical
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
        private double largeFilePercentCritical = 50.0;
        private int fileCountWarning = 10_000;
        private int fileCountCritical = 50_000;
        private int snapshotCountWarning = 100;
        private int snapshotCountCritical = 500;
        private int snapshotAgeWarningDays = 7;
        private int snapshotAgeCriticalDays = 30;
        private int deleteFileCountWarning = 100;
        private int deleteFileCountCritical = 500;
        private double deleteFileRatioWarning = 0.10;
        private double deleteFileRatioCritical = 0.25;
        private int manifestCountWarning = 100;
        private int manifestCountCritical = 500;
        private long manifestSizeWarningBytes = 100 * 1024 * 1024L;
        private long manifestSizeCriticalBytes = 500 * 1024 * 1024L;
        private int partitionCountWarning = 5_000;
        private int partitionCountCritical = 10_000;
        private double partitionSkewWarning = 3.0;
        private double partitionSkewCritical = 10.0;
        private int staleMetadataWarningDays = 7;
        private int staleMetadataCriticalDays = 30;

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

        public Builder largeFilePercentCritical(double largeFilePercentCritical) {
            this.largeFilePercentCritical = largeFilePercentCritical;
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

        public Builder deleteFileRatioWarning(double deleteFileRatioWarning) {
            this.deleteFileRatioWarning = deleteFileRatioWarning;
            return this;
        }

        public Builder deleteFileRatioCritical(double deleteFileRatioCritical) {
            this.deleteFileRatioCritical = deleteFileRatioCritical;
            return this;
        }

        public Builder manifestSizeWarningBytes(long manifestSizeWarningBytes) {
            this.manifestSizeWarningBytes = manifestSizeWarningBytes;
            return this;
        }

        public Builder manifestSizeCriticalBytes(long manifestSizeCriticalBytes) {
            this.manifestSizeCriticalBytes = manifestSizeCriticalBytes;
            return this;
        }

        public Builder partitionCountWarning(int partitionCountWarning) {
            this.partitionCountWarning = partitionCountWarning;
            return this;
        }

        public Builder partitionCountCritical(int partitionCountCritical) {
            this.partitionCountCritical = partitionCountCritical;
            return this;
        }

        public Builder partitionSkewWarning(double partitionSkewWarning) {
            this.partitionSkewWarning = partitionSkewWarning;
            return this;
        }

        public Builder partitionSkewCritical(double partitionSkewCritical) {
            this.partitionSkewCritical = partitionSkewCritical;
            return this;
        }

        public Builder staleMetadataWarningDays(int staleMetadataWarningDays) {
            this.staleMetadataWarningDays = staleMetadataWarningDays;
            return this;
        }

        public Builder staleMetadataCriticalDays(int staleMetadataCriticalDays) {
            this.staleMetadataCriticalDays = staleMetadataCriticalDays;
            return this;
        }

        public HealthThresholds build() {
            return new HealthThresholds(
                    smallFileSizeBytes,
                    smallFilePercentWarning,
                    smallFilePercentCritical,
                    largeFileSizeBytes,
                    largeFilePercentWarning,
                    largeFilePercentCritical,
                    fileCountWarning,
                    fileCountCritical,
                    snapshotCountWarning,
                    snapshotCountCritical,
                    snapshotAgeWarningDays,
                    snapshotAgeCriticalDays,
                    deleteFileCountWarning,
                    deleteFileCountCritical,
                    deleteFileRatioWarning,
                    deleteFileRatioCritical,
                    manifestCountWarning,
                    manifestCountCritical,
                    manifestSizeWarningBytes,
                    manifestSizeCriticalBytes,
                    partitionCountWarning,
                    partitionCountCritical,
                    partitionSkewWarning,
                    partitionSkewCritical,
                    staleMetadataWarningDays,
                    staleMetadataCriticalDays);
        }
    }
}
