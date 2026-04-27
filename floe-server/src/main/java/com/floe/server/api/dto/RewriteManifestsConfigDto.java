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

package com.floe.server.api.dto;

import com.floe.core.policy.RewriteManifestsConfig;
import com.floe.core.policy.RewriteManifestsConfig.ManifestFilterConfig;
import com.floe.core.policy.RewriteManifestsConfig.ManifestFilterConfig.PartitionFieldSummaryConfig;
import java.util.List;

/** DTO for RewriteManifests configuration. */
public record RewriteManifestsConfigDto(
        Integer specId, String stagingLocation, List<String> sortBy, ManifestFilterDto rewriteIf) {
    /**
     * DTO for manifest filter criteria. All fields are optional and map directly to Iceberg
     * ManifestFile schema fields.
     */
    public record ManifestFilterDto(
            // PATH
            String path,
            // LENGTH
            Long length,
            // SPEC_ID
            Integer specId,
            // MANIFEST_CONTENT
            String content,
            // SEQUENCE_NUMBER
            Long sequenceNumber,
            // MIN_SEQUENCE_NUMBER
            Long minSequenceNumber,
            // SNAPSHOT_ID
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
            // KEY_METADATA
            String keyMetadata,
            // PARTITION_SUMMARIES
            List<PartitionFieldSummaryDto> partitionSummaries) {
        /** DTO for partition field summary. */
        public record PartitionFieldSummaryDto(
                Boolean containsNull, Boolean containsNan, String lowerBound, String upperBound) {
            public PartitionFieldSummaryConfig toConfig() {
                return new PartitionFieldSummaryConfig(
                        containsNull, containsNan, lowerBound, upperBound);
            }
        }

        public ManifestFilterConfig toConfig() {
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
                    partitionSummaries != null
                            ? partitionSummaries.stream()
                                    .map(PartitionFieldSummaryDto::toConfig)
                                    .toList()
                            : null);
        }
    }

    public RewriteManifestsConfig toConfig() {
        return new RewriteManifestsConfig(
                specId, stagingLocation, sortBy, rewriteIf != null ? rewriteIf.toConfig() : null);
    }
}
