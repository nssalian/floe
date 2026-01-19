package com.floe.core.policy;

import java.util.List;

/** Configuration for rewrite manifests operation. */
public record RewriteManifestsConfig(
        Integer specId,
        String stagingLocation,
        List<String> sortBy,
        ManifestFilterConfig rewriteIf) {
    /**
     * Filter configuration for selecting which manifests to rewrite. All fields are optional and
     * map directly to Iceberg ManifestFile schema fields.
     */
    public record ManifestFilterConfig(
            // PATH - manifest file path
            String path,
            // LENGTH - manifest file size in bytes
            Long length,
            // SPEC_ID - partition spec ID
            Integer specId,
            // MANIFEST_CONTENT - DATA or DELETES
            String content,
            // SEQUENCE_NUMBER - sequence number of commit that added the manifest
            Long sequenceNumber,
            // MIN_SEQUENCE_NUMBER - lowest sequence number of any live file in manifest
            Long minSequenceNumber,
            // SNAPSHOT_ID - snapshot that added the manifest
            Long snapshotId,
            // ADDED_FILES_COUNT
            Integer addedFilesCount,
            // EXISTING_FILES_COUNT
            Integer existingFilesCount,
            // DELETED_FILES_COUNT
            Integer deletedFilesCount,
            // ADDED_ROWS_COUNT
            Long addedRowsCount,
            // EXISTING_ROWS_COUNT
            Long existingRowsCount,
            // DELETED_ROWS_COUNT
            Long deletedRowsCount,
            // FIRST_ROW_ID
            Long firstRowId,
            // KEY_METADATA - as hex string
            String keyMetadata,
            // PARTITION_SUMMARIES
            List<PartitionFieldSummaryConfig> partitionSummaries) {
        /** Summary of a partition field in a manifest. */
        public record PartitionFieldSummaryConfig(
                Boolean containsNull, Boolean containsNan, String lowerBound, String upperBound) {
            public static PartitionFieldSummaryConfig of(boolean containsNull) {
                return new PartitionFieldSummaryConfig(containsNull, null, null, null);
            }
        }

        public static ManifestFilterConfig empty() {
            return new ManifestFilterConfig(
                    null, null, null, null, null, null, null, null, null, null, null, null, null,
                    null, null, null);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private String path;
            private Long length;
            private Integer specId;
            private String content;
            private Long sequenceNumber;
            private Long minSequenceNumber;
            private Long snapshotId;
            private Integer addedFilesCount;
            private Integer existingFilesCount;
            private Integer deletedFilesCount;
            private Long addedRowsCount;
            private Long existingRowsCount;
            private Long deletedRowsCount;
            private Long firstRowId;
            private String keyMetadata;
            private List<PartitionFieldSummaryConfig> partitionSummaries;

            public Builder path(String path) {
                this.path = path;
                return this;
            }

            public Builder length(Long length) {
                this.length = length;
                return this;
            }

            public Builder specId(Integer specId) {
                this.specId = specId;
                return this;
            }

            public Builder content(String content) {
                this.content = content;
                return this;
            }

            public Builder sequenceNumber(Long sequenceNumber) {
                this.sequenceNumber = sequenceNumber;
                return this;
            }

            public Builder minSequenceNumber(Long minSequenceNumber) {
                this.minSequenceNumber = minSequenceNumber;
                return this;
            }

            public Builder snapshotId(Long snapshotId) {
                this.snapshotId = snapshotId;
                return this;
            }

            public Builder addedFilesCount(Integer count) {
                this.addedFilesCount = count;
                return this;
            }

            public Builder existingFilesCount(Integer count) {
                this.existingFilesCount = count;
                return this;
            }

            public Builder deletedFilesCount(Integer count) {
                this.deletedFilesCount = count;
                return this;
            }

            public Builder addedRowsCount(Long count) {
                this.addedRowsCount = count;
                return this;
            }

            public Builder existingRowsCount(Long count) {
                this.existingRowsCount = count;
                return this;
            }

            public Builder deletedRowsCount(Long count) {
                this.deletedRowsCount = count;
                return this;
            }

            public Builder firstRowId(Long firstRowId) {
                this.firstRowId = firstRowId;
                return this;
            }

            public Builder keyMetadata(String keyMetadata) {
                this.keyMetadata = keyMetadata;
                return this;
            }

            public Builder partitionSummaries(List<PartitionFieldSummaryConfig> summaries) {
                this.partitionSummaries = summaries != null ? List.copyOf(summaries) : null;
                return this;
            }

            public ManifestFilterConfig build() {
                return new ManifestFilterConfig(
                        path,
                        length,
                        specId,
                        content,
                        sequenceNumber,
                        minSequenceNumber,
                        snapshotId,
                        addedFilesCount,
                        existingFilesCount,
                        deletedFilesCount,
                        addedRowsCount,
                        existingRowsCount,
                        deletedRowsCount,
                        firstRowId,
                        keyMetadata,
                        partitionSummaries);
            }
        }
    }

    public static RewriteManifestsConfig defaults() {
        return new RewriteManifestsConfig(null, null, null, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Integer specId;
        private String stagingLocation;
        private List<String> sortBy;
        private ManifestFilterConfig rewriteIf;

        public Builder specId(Integer specId) {
            this.specId = specId;
            return this;
        }

        public Builder stagingLocation(String stagingLocation) {
            this.stagingLocation = stagingLocation;
            return this;
        }

        public Builder sortBy(List<String> sortBy) {
            this.sortBy = sortBy != null ? List.copyOf(sortBy) : null;
            return this;
        }

        public Builder rewriteIf(ManifestFilterConfig rewriteIf) {
            this.rewriteIf = rewriteIf;
            return this;
        }

        public RewriteManifestsConfig build() {
            return new RewriteManifestsConfig(specId, stagingLocation, sortBy, rewriteIf);
        }
    }
}
