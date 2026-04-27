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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Configuration for data file compaction. */
public record RewriteDataFilesConfig(
        String strategy,
        List<String> sortOrder,
        List<String> zOrderColumns,
        Long targetFileSizeBytes,
        Long maxFileGroupSizeBytes,
        Integer maxConcurrentFileGroupRewrites,
        Boolean partialProgressEnabled,
        Integer partialProgressMaxCommits,
        Integer partialProgressMaxFailedCommits,
        String filter,
        String rewriteJobOrder,
        Boolean useStartingSequenceNumber,
        Boolean removeDanglingDeletes,
        Integer outputSpecId) {
    public static RewriteDataFilesConfig defaults() {
        return new RewriteDataFilesConfig(
                "BINPACK", null, null, null, null, null, null, null, null, null, null, null, null,
                null);
    }

    public Map<String, String> toOptionsMap() {
        Map<String, String> options = new HashMap<>();

        if (targetFileSizeBytes != null) {
            options.put("target-file-size-bytes", targetFileSizeBytes.toString());
        }
        if (maxFileGroupSizeBytes != null) {
            options.put("max-file-group-size-bytes", maxFileGroupSizeBytes.toString());
        }
        if (maxConcurrentFileGroupRewrites != null) {
            options.put(
                    "max-concurrent-file-group-rewrites",
                    maxConcurrentFileGroupRewrites.toString());
        }
        if (partialProgressEnabled != null) {
            options.put("partial-progress.enabled", partialProgressEnabled.toString());
        }
        if (partialProgressMaxCommits != null) {
            options.put("partial-progress.max-commits", partialProgressMaxCommits.toString());
        }
        if (partialProgressMaxFailedCommits != null) {
            options.put(
                    "partial-progress.max-failed-commits",
                    partialProgressMaxFailedCommits.toString());
        }
        if (rewriteJobOrder != null) {
            options.put("rewrite-job-order", rewriteJobOrder);
        }
        if (useStartingSequenceNumber != null) {
            options.put("use-starting-sequence-number", useStartingSequenceNumber.toString());
        }
        if (removeDanglingDeletes != null) {
            options.put("remove-dangling-deletes", removeDanglingDeletes.toString());
        }
        if (outputSpecId != null) {
            options.put("output-spec-id", outputSpecId.toString());
        }

        return options;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String strategy = "BINPACK";
        private List<String> sortOrder;
        private List<String> zOrderColumns;
        private Long targetFileSizeBytes;
        private Long maxFileGroupSizeBytes;
        private Integer maxConcurrentFileGroupRewrites;
        private Boolean partialProgressEnabled;
        private Integer partialProgressMaxCommits;
        private Integer partialProgressMaxFailedCommits;
        private String filter;
        private String rewriteJobOrder;
        private Boolean useStartingSequenceNumber;
        private Boolean removeDanglingDeletes;
        private Integer outputSpecId;

        /**
         * Set the rewrite strategy (BINPACK, SORT, or ZORDER).
         *
         * @param strategy the compaction strategy to use
         * @return this builder
         */
        public Builder strategy(String strategy) {
            this.strategy = strategy;
            return this;
        }

        /**
         * Set the sort order for SORT strategy.
         *
         * @param sortOrder list of column names to sort by
         * @return this builder
         */
        public Builder sortOrder(List<String> sortOrder) {
            this.sortOrder = sortOrder;
            return this;
        }

        /**
         * Set the Z-order columns for ZORDER strategy.
         *
         * @param zOrderColumns list of column names for Z-ordering
         * @return this builder
         */
        public Builder zOrderColumns(List<String> zOrderColumns) {
            this.zOrderColumns = zOrderColumns;
            return this;
        }

        /**
         * Set the target file size for rewritten files.
         *
         * @param v target size in bytes (e.g., 134217728 for 128 MB)
         * @return this builder
         */
        public Builder targetFileSizeBytes(Long v) {
            this.targetFileSizeBytes = v;
            return this;
        }

        /**
         * Set the maximum size of a file group to rewrite together.
         *
         * @param v max size in bytes (default: 100 GB)
         * @return this builder
         */
        public Builder maxFileGroupSizeBytes(Long v) {
            this.maxFileGroupSizeBytes = v;
            return this;
        }

        /**
         * Set the parallelism level for rewriting file groups.
         *
         * @param v number of file groups to rewrite concurrently (default: 5)
         * @return this builder
         */
        public Builder maxConcurrentFileGroupRewrites(Integer v) {
            this.maxConcurrentFileGroupRewrites = v;
            return this;
        }

        /**
         * Enable partial progress commits after each file group.
         *
         * @param v true to enable partial commits (default: false)
         * @return this builder
         */
        public Builder partialProgressEnabled(Boolean v) {
            this.partialProgressEnabled = v;
            return this;
        }

        /**
         * Set the maximum number of partial progress commits.
         *
         * @param v max commits (default: 10)
         * @return this builder
         */
        public Builder partialProgressMaxCommits(Integer v) {
            this.partialProgressMaxCommits = v;
            return this;
        }

        /**
         * Set the maximum failed commits before aborting.
         *
         * @param v max failed commits before abort
         * @return this builder
         */
        public Builder partialProgressMaxFailedCommits(Integer v) {
            this.partialProgressMaxFailedCommits = v;
            return this;
        }

        /**
         * Set an Iceberg expression filter for which files to rewrite.
         *
         * @param v expression filter string (e.g., "partition = 'abc'")
         * @return this builder
         */
        public Builder filter(String v) {
            this.filter = v;
            return this;
        }

        /**
         * Set the job execution order (NONE, BYTES_ASC, BYTES_DESC, FILES_ASC, FILES_DESC).
         *
         * @param v rewrite job order strategy
         * @return this builder
         */
        public Builder rewriteJobOrder(String v) {
            this.rewriteJobOrder = v;
            return this;
        }

        /**
         * Set whether to use starting sequence numbers for commit validation.
         *
         * @param v true to use starting sequence numbers (default: true)
         * @return this builder
         */
        public Builder useStartingSequenceNumber(Boolean v) {
            this.useStartingSequenceNumber = v;
            return this;
        }

        /**
         * Set whether to remove dangling delete files during rewrite.
         *
         * @param v true to remove dangling deletes (default: false)
         * @return this builder
         */
        public Builder removeDanglingDeletes(Boolean v) {
            this.removeDanglingDeletes = v;
            return this;
        }

        /**
         * Set the output partition spec ID for rewritten files.
         *
         * @param v partition spec ID, or null to use current spec
         * @return this builder
         */
        public Builder outputSpecId(Integer v) {
            this.outputSpecId = v;
            return this;
        }

        public RewriteDataFilesConfig build() {
            return new RewriteDataFilesConfig(
                    strategy,
                    sortOrder,
                    zOrderColumns,
                    targetFileSizeBytes,
                    maxFileGroupSizeBytes,
                    maxConcurrentFileGroupRewrites,
                    partialProgressEnabled,
                    partialProgressMaxCommits,
                    partialProgressMaxFailedCommits,
                    filter,
                    rewriteJobOrder,
                    useStartingSequenceNumber,
                    removeDanglingDeletes,
                    outputSpecId);
        }
    }
}
