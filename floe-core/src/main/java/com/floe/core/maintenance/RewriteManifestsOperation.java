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

package com.floe.core.maintenance;

import java.util.List;
import java.util.Optional;

/**
 * Configuration for Iceberg manifest rewriting.
 *
 * <p>Maps to Apache Iceberg's {@code RewriteManifests} action. Rewrites manifest files to optimize
 * metadata structure, improving query planning performance. Manifests can be consolidated or
 * reorganized based on partition field ordering.
 *
 * @param specId partition spec ID to rewrite manifests for (defaults to table's current spec)
 * @param stagingLocation staging location for writing new manifests (defaults to table metadata
 *     location)
 * @param sortBy list of partition fields to sort manifests by
 * @param rewriteIf predicate filter to select which manifests to rewrite (maps all 16 ManifestFile
 *     schema fields)
 * @see <a href="https://iceberg.apache.org/docs/latest/maintenance/#rewrite-manifests">Iceberg
 *     RewriteManifests</a>
 */
public record RewriteManifestsOperation(
        Optional<Integer> specId,
        Optional<String> stagingLocation,
        Optional<List<String>> sortBy,
        Optional<ManifestFilter> rewriteIf)
        implements MaintenanceOperation {
    /**
     * Filter criteria for selecting which manifests to rewrite.
     *
     * <p>All fields are optional and map directly to Iceberg's ManifestFile schema (16 fields).
     * Multiple criteria are combined with AND logic. At runtime, this filter is converted to a
     * {@code Predicate<ManifestFile>} for the {@code rewriteIf()} method.
     *
     * @param path manifest file path
     * @param length manifest file size in bytes
     * @param specId partition spec ID
     * @param content manifest content type (DATA or DELETES)
     * @param sequenceNumber sequence number of commit that added the manifest
     * @param minSequenceNumber lowest sequence number of any live file in manifest
     * @param snapshotId snapshot ID that added the manifest
     * @param addedFilesCount number of files added by the manifest
     * @param existingFilesCount number of existing files in the manifest
     * @param deletedFilesCount number of files deleted by the manifest
     * @param addedRowsCount total rows added across all added files
     * @param existingRowsCount total rows in existing files
     * @param deletedRowsCount total rows deleted
     * @param firstRowId starting row ID for new rows in added data files
     * @param keyMetadata encryption key metadata (hex encoded)
     * @param partitionSummaries summaries of partition field bounds
     */
    public record ManifestFilter(
            // PATH - manifest file path
            Optional<String> path,
            // LENGTH - manifest file size in bytes
            Optional<Long> length,
            // SPEC_ID - partition spec ID
            Optional<Integer> specId,
            // MANIFEST_CONTENT - DATA or DELETES
            Optional<ManifestContent> content,
            // SEQUENCE_NUMBER - sequence number of commit that added the manifest
            Optional<Long> sequenceNumber,
            // MIN_SEQUENCE_NUMBER - lowest sequence number of any live file in manifest
            Optional<Long> minSequenceNumber,
            // SNAPSHOT_ID - snapshot that added the manifest
            Optional<Long> snapshotId,
            // ADDED_FILES_COUNT
            Optional<Integer> addedFilesCount,
            // EXISTING_FILES_COUNT
            Optional<Integer> existingFilesCount,
            // DELETED_FILES_COUNT
            Optional<Integer> deletedFilesCount,
            // ADDED_ROWS_COUNT
            Optional<Long> addedRowsCount,
            // EXISTING_ROWS_COUNT
            Optional<Long> existingRowsCount,
            // DELETED_ROWS_COUNT
            Optional<Long> deletedRowsCount,
            // FIRST_ROW_ID - starting row ID for new rows in ADDED data files
            Optional<Long> firstRowId,
            // KEY_METADATA - encryption key metadata (as hex string for JSON compatibility)
            Optional<String> keyMetadata,
            // PARTITION_SUMMARIES - list of partition field summaries (as JSON array)
            Optional<List<PartitionFieldSummary>> partitionSummaries) {
        /** Summary of a partition field in a manifest. Maps to Iceberg's PartitionFieldSummary. */
        public record PartitionFieldSummary(
                boolean containsNull,
                Optional<Boolean> containsNan,
                Optional<String> lowerBound,
                Optional<String> upperBound) {
            public static PartitionFieldSummary of(boolean containsNull) {
                return new PartitionFieldSummary(
                        containsNull, Optional.empty(), Optional.empty(), Optional.empty());
            }

            public static Builder builder() {
                return new Builder();
            }

            public static class Builder {

                private boolean containsNull = false;
                private Optional<Boolean> containsNan = Optional.empty();
                private Optional<String> lowerBound = Optional.empty();
                private Optional<String> upperBound = Optional.empty();

                public Builder containsNull(boolean containsNull) {
                    this.containsNull = containsNull;
                    return this;
                }

                public Builder containsNan(boolean containsNan) {
                    this.containsNan = Optional.of(containsNan);
                    return this;
                }

                public Builder lowerBound(String lowerBound) {
                    this.lowerBound = Optional.ofNullable(lowerBound);
                    return this;
                }

                public Builder upperBound(String upperBound) {
                    this.upperBound = Optional.ofNullable(upperBound);
                    return this;
                }

                public PartitionFieldSummary build() {
                    return new PartitionFieldSummary(
                            containsNull, containsNan, lowerBound, upperBound);
                }
            }
        }

        public static ManifestFilter empty() {
            return new ManifestFilter(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        public static Builder builder() {
            return new Builder();
        }

        /** Returns true if no filter criteria are set. */
        public boolean isEmpty() {
            return (path.isEmpty()
                    && length.isEmpty()
                    && specId.isEmpty()
                    && content.isEmpty()
                    && sequenceNumber.isEmpty()
                    && minSequenceNumber.isEmpty()
                    && snapshotId.isEmpty()
                    && addedFilesCount.isEmpty()
                    && existingFilesCount.isEmpty()
                    && deletedFilesCount.isEmpty()
                    && addedRowsCount.isEmpty()
                    && existingRowsCount.isEmpty()
                    && deletedRowsCount.isEmpty()
                    && firstRowId.isEmpty()
                    && keyMetadata.isEmpty()
                    && partitionSummaries.isEmpty());
        }

        public static class Builder {

            private Optional<String> path = Optional.empty();
            private Optional<Long> length = Optional.empty();
            private Optional<Integer> specId = Optional.empty();
            private Optional<ManifestContent> content = Optional.empty();
            private Optional<Long> sequenceNumber = Optional.empty();
            private Optional<Long> minSequenceNumber = Optional.empty();
            private Optional<Long> snapshotId = Optional.empty();
            private Optional<Integer> addedFilesCount = Optional.empty();
            private Optional<Integer> existingFilesCount = Optional.empty();
            private Optional<Integer> deletedFilesCount = Optional.empty();
            private Optional<Long> addedRowsCount = Optional.empty();
            private Optional<Long> existingRowsCount = Optional.empty();
            private Optional<Long> deletedRowsCount = Optional.empty();
            private Optional<Long> firstRowId = Optional.empty();
            private Optional<String> keyMetadata = Optional.empty();
            private Optional<List<PartitionFieldSummary>> partitionSummaries = Optional.empty();

            public Builder path(String path) {
                this.path = Optional.ofNullable(path);
                return this;
            }

            public Builder length(long length) {
                this.length = Optional.of(length);
                return this;
            }

            public Builder specId(int specId) {
                this.specId = Optional.of(specId);
                return this;
            }

            public Builder content(ManifestContent content) {
                this.content = Optional.ofNullable(content);
                return this;
            }

            public Builder sequenceNumber(long sequenceNumber) {
                this.sequenceNumber = Optional.of(sequenceNumber);
                return this;
            }

            public Builder minSequenceNumber(long minSequenceNumber) {
                this.minSequenceNumber = Optional.of(minSequenceNumber);
                return this;
            }

            public Builder snapshotId(long snapshotId) {
                this.snapshotId = Optional.of(snapshotId);
                return this;
            }

            public Builder addedFilesCount(int count) {
                this.addedFilesCount = Optional.of(count);
                return this;
            }

            public Builder existingFilesCount(int count) {
                this.existingFilesCount = Optional.of(count);
                return this;
            }

            public Builder deletedFilesCount(int count) {
                this.deletedFilesCount = Optional.of(count);
                return this;
            }

            public Builder addedRowsCount(long count) {
                this.addedRowsCount = Optional.of(count);
                return this;
            }

            public Builder existingRowsCount(long count) {
                this.existingRowsCount = Optional.of(count);
                return this;
            }

            public Builder deletedRowsCount(long count) {
                this.deletedRowsCount = Optional.of(count);
                return this;
            }

            public Builder firstRowId(long firstRowId) {
                this.firstRowId = Optional.of(firstRowId);
                return this;
            }

            public Builder keyMetadata(String keyMetadata) {
                this.keyMetadata = Optional.ofNullable(keyMetadata);
                return this;
            }

            public Builder partitionSummaries(List<PartitionFieldSummary> summaries) {
                this.partitionSummaries =
                        summaries != null ? Optional.of(List.copyOf(summaries)) : Optional.empty();
                return this;
            }

            public ManifestFilter build() {
                return new ManifestFilter(
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

    /** Manifest content type - mirrors Iceberg's ManifestContent enum. */
    public enum ManifestContent {
        DATA,
        DELETES,
    }

    @Override
    public Type getType() {
        return Type.REWRITE_MANIFESTS;
    }

    @Override
    public String describe() {
        StringBuilder desc = new StringBuilder("Rewrite manifests");
        specId.ifPresent(id -> desc.append(String.format(" with spec ID %d", id)));
        stagingLocation.ifPresent(
                location -> desc.append(String.format(", staging location %s", location)));
        sortBy.ifPresent(fields -> desc.append(String.format(", sort by %s", fields)));
        rewriteIf.ifPresent(
                filter -> {
                    if (!filter.isEmpty()) {
                        desc.append(", with rewriteIf predicate");
                    }
                });
        return desc.toString();
    }

    public static RewriteManifestsOperation defaults() {
        return new RewriteManifestsOperation(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Optional<Integer> specId = Optional.empty();
        private Optional<String> stagingLocation = Optional.empty();
        private Optional<List<String>> sortBy = Optional.empty();
        private Optional<ManifestFilter> rewriteIf = Optional.empty();

        public Builder specId(int specId) {
            this.specId = Optional.of(specId);
            return this;
        }

        public Builder stagingLocation(String location) {
            if (location == null) {
                this.stagingLocation = Optional.empty();
                return this;
            }
            this.stagingLocation = Optional.of(location);
            return this;
        }

        public Builder sortBy(List<String> fields) {
            if (fields == null || fields.isEmpty()) {
                this.sortBy = Optional.empty();
                return this;
            }
            this.sortBy = Optional.of(List.copyOf(fields));
            return this;
        }

        public Builder rewriteIf(ManifestFilter filter) {
            this.rewriteIf = Optional.ofNullable(filter);
            return this;
        }

        public RewriteManifestsOperation build() {
            return new RewriteManifestsOperation(specId, stagingLocation, sortBy, rewriteIf);
        }
    }
}
