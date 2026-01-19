package com.floe.core.catalog;

import java.time.Instant;
import java.util.Map;

/**
 * Metadata about an Iceberg table
 *
 * @param identifier table identifier
 * @param location table storage location
 * @param snapshotCount number of snapshots
 * @param currentSnapshotId current snapshot ID
 * @param currentSnapshotTimestamp timestamp of current snapshot
 * @param oldestSnapshotTimestamp timestamp of oldest snapshot
 * @param dataFileCount number of data files
 * @param totalDataFileSizeBytes total size of data files in bytes
 * @param deleteFileCount number of delete files
 * @param positionDeleteFileCount number of position delete files
 * @param equalityDeleteFileCount number of equality delete files
 * @param totalRecordCount total record count
 * @param manifestCount number of manifests
 * @param totalManifestSizeBytes total manifest size in bytes
 * @param formatVersion Iceberg format version
 * @param partitionSpec partition specification
 * @param sortOrder sort order
 * @param properties table properties
 * @param lastModified last modification timestamp
 */
public record TableMetadata(
        TableIdentifier identifier,
        String location,
        int snapshotCount,
        long currentSnapshotId,
        Instant currentSnapshotTimestamp,
        Instant oldestSnapshotTimestamp,
        int dataFileCount,
        long totalDataFileSizeBytes,
        int deleteFileCount,
        int positionDeleteFileCount,
        int equalityDeleteFileCount,
        long totalRecordCount,
        int manifestCount,
        long totalManifestSizeBytes,
        int formatVersion,
        String partitionSpec,
        String sortOrder,
        Map<String, String> properties,
        Instant lastModified) {
    public double averageDataFileSizeMb() {
        if (dataFileCount == 0) return 0.0;
        return ((totalDataFileSizeBytes / (double) dataFileCount) / (1024 * 1024));
    }

    public double totalDataSizeGb() {
        return totalDataFileSizeBytes / (1024.0 * 1024.0 * 1024.0);
    }

    public boolean isEmpty() {
        return dataFileCount == 0;
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    public boolean hasDeleteFiles() {
        return deleteFileCount > 0;
    }

    public boolean isFormatV2() {
        return formatVersion >= 2;
    }

    public boolean isPartitioned() {
        return (partitionSpec != null
                && !partitionSpec.isBlank()
                && !partitionSpec.equals("[]")
                && !partitionSpec.equals("unpartitioned"));
    }

    public boolean isSorted() {
        return (sortOrder != null && !sortOrder.isBlank() && !sortOrder.equals("unsorted"));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private TableIdentifier identifier;
        private String location;
        private int snapshotCount;
        private long currentSnapshotId;
        private Instant currentSnapshotTimestamp;
        private Instant oldestSnapshotTimestamp;
        private int dataFileCount;
        private long totalDataFileSizeBytes;
        private int deleteFileCount;
        private int positionDeleteFileCount;
        private int equalityDeleteFileCount;
        private long totalRecordCount;
        private int manifestCount;
        private long totalManifestSizeBytes;
        private int formatVersion = 2;
        private String partitionSpec;
        private String sortOrder;
        private Map<String, String> properties = Map.of();
        private Instant lastModified;

        public Builder identifier(TableIdentifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder location(String location) {
            this.location = location;
            return this;
        }

        public Builder snapshotCount(int snapshotCount) {
            this.snapshotCount = snapshotCount;
            return this;
        }

        public Builder currentSnapshotId(long currentSnapshotId) {
            this.currentSnapshotId = currentSnapshotId;
            return this;
        }

        public Builder currentSnapshotTimestamp(Instant currentSnapshotTimestamp) {
            this.currentSnapshotTimestamp = currentSnapshotTimestamp;
            return this;
        }

        public Builder oldestSnapshotTimestamp(Instant oldestSnapshotTimestamp) {
            this.oldestSnapshotTimestamp = oldestSnapshotTimestamp;
            return this;
        }

        public Builder dataFileCount(int dataFileCount) {
            this.dataFileCount = dataFileCount;
            return this;
        }

        public Builder totalDataFileSizeBytes(long totalDataFileSizeBytes) {
            this.totalDataFileSizeBytes = totalDataFileSizeBytes;
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

        public Builder totalRecordCount(long totalRecordCount) {
            this.totalRecordCount = totalRecordCount;
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

        public Builder formatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
            return this;
        }

        public Builder partitionSpec(String partitionSpec) {
            this.partitionSpec = partitionSpec;
            return this;
        }

        public Builder sortOrder(String sortOrder) {
            this.sortOrder = sortOrder;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder lastModified(Instant lastModified) {
            this.lastModified = lastModified;
            return this;
        }

        public TableMetadata build() {
            return new TableMetadata(
                    identifier,
                    location,
                    snapshotCount,
                    currentSnapshotId,
                    currentSnapshotTimestamp,
                    oldestSnapshotTimestamp,
                    dataFileCount,
                    totalDataFileSizeBytes,
                    deleteFileCount,
                    positionDeleteFileCount,
                    equalityDeleteFileCount,
                    totalRecordCount,
                    manifestCount,
                    totalManifestSizeBytes,
                    formatVersion,
                    partitionSpec,
                    sortOrder,
                    properties,
                    lastModified);
        }
    }
}
