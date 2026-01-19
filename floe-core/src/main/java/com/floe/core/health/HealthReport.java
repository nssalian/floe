package com.floe.core.health;

import com.floe.core.catalog.TableIdentifier;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Health assessment report for an Iceberg table. */
public record HealthReport(
        TableIdentifier tableIdentifier,
        Instant assessedAt,

        // Snapshot metrics
        int snapshotCount,
        Optional<Instant> oldestSnapshotTimestamp,
        Optional<Instant> newestSnapshotTimestamp,

        // Data file metrics
        int dataFileCount,
        long totalDataSizeBytes,
        long minFileSizeBytes,
        long maxFileSizeBytes,
        double avgFileSizeBytes,
        int smallFileCount,
        int largeFileCount,

        // Manifest metrics
        int manifestCount,
        long totalManifestSizeBytes,

        // Delete file metrics
        int deleteFileCount,
        int positionDeleteFileCount,
        int equalityDeleteFileCount,

        // Partitioning
        int partitionCount,

        // Issues Found
        List<HealthIssue> issues) {
    public static final long DEFAULT_SMALL_FILE_THRESHOLD_BYTES = 10 * 1024 * 1024; // 10 MB
    public static final long DEFAULT_LARGE_FILE_THRESHOLD_BYTES = 1024 * 1024 * 1024; // 1 GB

    /** Default constructor to ensure issues is never null. */
    public HealthReport {
        if (issues == null) {
            issues = List.of();
        }
    }

    public boolean hasIssues() {
        return !issues.isEmpty();
    }

    public boolean hasIssuesOfSeverity(HealthIssue.Severity severity) {
        return issues.stream().anyMatch(issue -> issue.severity() == severity);
    }

    public List<HealthIssue> getIssuesOfType(HealthIssue.Type type) {
        return issues.stream().filter(issue -> issue.type() == type).toList();
    }

    public Optional<Duration> oldestSnapshotAge() {
        return oldestSnapshotTimestamp.map(ts -> Duration.between(ts, assessedAt));
    }

    public double avgFileSizeMb() {
        return avgFileSizeBytes / (1024.0 * 1024.0);
    }

    public double totalDataSizeGb() {
        return totalDataSizeBytes / (1024.0 * 1024.0 * 1024.0);
    }

    public boolean isEmpty() {
        return dataFileCount == 0;
    }

    public double smallFilePercentage() {
        if (dataFileCount == 0) return 0.0;
        return (smallFileCount * 100.0) / dataFileCount;
    }

    public boolean needsCompaction(double smallFileThresholdPercent) {
        return smallFilePercentage() > smallFileThresholdPercent;
    }

    /** Builder for HealthReport */
    public static Builder builder(TableIdentifier tableIdentifier) {
        return new Builder(tableIdentifier);
    }

    public static Builder builder() {
        throw new UnsupportedOperationException(
                "TableIdentifier is required to build HealthReport");
    }

    public static class Builder {

        private final TableIdentifier tableIdentifier;
        private Instant assessedAt = Instant.now();
        private int snapshotCount = 0;
        private Optional<Instant> oldestSnapshotTimestamp = Optional.empty();
        private Optional<Instant> newestSnapshotTimestamp = Optional.empty();
        private int dataFileCount = 0;
        private long totalDataSizeBytes = 0;
        private long minFileSizeBytes = 0;
        private long maxFileSizeBytes = 0;
        private double avgFileSizeBytes = 0;
        private int smallFileCount = 0;
        private int largeFileCount = 0;
        private int manifestCount = 0;
        private long totalManifestSizeBytes = 0;
        private int deleteFileCount = 0;
        private int positionDeleteFileCount = 0;
        private int equalityDeleteFileCount = 0;
        private int partitionCount = 0;
        private List<HealthIssue> issues = List.of();

        private Builder(TableIdentifier tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
        }

        public Builder assessedAt(Instant assessedAt) {
            this.assessedAt = assessedAt;
            return this;
        }

        public Builder snapshotCount(int snapshotCount) {
            this.snapshotCount = snapshotCount;
            return this;
        }

        public Builder oldestSnapshotTimestamp(Instant oldestSnapshotTimestamp) {
            this.oldestSnapshotTimestamp = Optional.ofNullable(oldestSnapshotTimestamp);
            return this;
        }

        public Builder newestSnapshotTimestamp(Instant newestSnapshotTimestamp) {
            this.newestSnapshotTimestamp = Optional.ofNullable(newestSnapshotTimestamp);
            return this;
        }

        public Builder dataFileCount(int dataFileCount) {
            this.dataFileCount = dataFileCount;
            return this;
        }

        public Builder totalDataSizeBytes(long totalDataSizeBytes) {
            this.totalDataSizeBytes = totalDataSizeBytes;
            return this;
        }

        public Builder minFileSizeBytes(long minFileSizeBytes) {
            this.minFileSizeBytes = minFileSizeBytes;
            return this;
        }

        public Builder maxFileSizeBytes(long maxFileSizeBytes) {
            this.maxFileSizeBytes = maxFileSizeBytes;
            return this;
        }

        public Builder avgFileSizeBytes(double avgFileSizeBytes) {
            this.avgFileSizeBytes = avgFileSizeBytes;
            return this;
        }

        public Builder smallFileCount(int smallFileCount) {
            this.smallFileCount = smallFileCount;
            return this;
        }

        public Builder largeFileCount(int largeFileCount) {
            this.largeFileCount = largeFileCount;
            return this;
        }

        public Builder manifestCount(int manifestCount) {
            this.manifestCount = manifestCount;
            return this;
        }

        public Builder totalManifestSizeBytes(long totalManifestSizeBytes) {
            this.totalManifestSizeBytes = totalManifestSizeBytes;
            return this;
        }

        public Builder deleteFileCount(int deleteFileCount) {
            this.deleteFileCount = deleteFileCount;
            return this;
        }

        public Builder positionDeleteFileCount(int positionDeleteFileCount) {
            this.positionDeleteFileCount = positionDeleteFileCount;
            return this;
        }

        public Builder equalityDeleteFileCount(int equalityDeleteFileCount) {
            this.equalityDeleteFileCount = equalityDeleteFileCount;
            return this;
        }

        public Builder partitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        public Builder issues(List<HealthIssue> issues) {
            this.issues = issues;
            return this;
        }

        public HealthReport build() {
            return new HealthReport(
                    tableIdentifier,
                    assessedAt,
                    snapshotCount,
                    oldestSnapshotTimestamp,
                    newestSnapshotTimestamp,
                    dataFileCount,
                    totalDataSizeBytes,
                    minFileSizeBytes,
                    maxFileSizeBytes,
                    avgFileSizeBytes,
                    smallFileCount,
                    largeFileCount,
                    manifestCount,
                    totalManifestSizeBytes,
                    deleteFileCount,
                    positionDeleteFileCount,
                    equalityDeleteFileCount,
                    partitionCount,
                    issues);
        }
    }
}
