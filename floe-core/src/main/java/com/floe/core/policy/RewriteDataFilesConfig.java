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

        public Builder strategy(String strategy) {
            this.strategy = strategy;
            return this;
        }

        public Builder sortOrder(List<String> sortOrder) {
            this.sortOrder = sortOrder;
            return this;
        }

        public Builder zOrderColumns(List<String> zOrderColumns) {
            this.zOrderColumns = zOrderColumns;
            return this;
        }

        public Builder targetFileSizeBytes(Long v) {
            this.targetFileSizeBytes = v;
            return this;
        }

        public Builder maxFileGroupSizeBytes(Long v) {
            this.maxFileGroupSizeBytes = v;
            return this;
        }

        public Builder maxConcurrentFileGroupRewrites(Integer v) {
            this.maxConcurrentFileGroupRewrites = v;
            return this;
        }

        public Builder partialProgressEnabled(Boolean v) {
            this.partialProgressEnabled = v;
            return this;
        }

        public Builder partialProgressMaxCommits(Integer v) {
            this.partialProgressMaxCommits = v;
            return this;
        }

        public Builder partialProgressMaxFailedCommits(Integer v) {
            this.partialProgressMaxFailedCommits = v;
            return this;
        }

        public Builder filter(String v) {
            this.filter = v;
            return this;
        }

        public Builder rewriteJobOrder(String v) {
            this.rewriteJobOrder = v;
            return this;
        }

        public Builder useStartingSequenceNumber(Boolean v) {
            this.useStartingSequenceNumber = v;
            return this;
        }

        public Builder removeDanglingDeletes(Boolean v) {
            this.removeDanglingDeletes = v;
            return this;
        }

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
