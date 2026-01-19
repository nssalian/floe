package com.floe.core.maintenance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Configuration for Iceberg data file compaction (rewriting).
 *
 * <p>Maps to Apache Iceberg's {@code RewriteDataFiles} action. Combines small data files into
 * larger files to improve query performance and reduce metadata overhead.
 *
 * @param strategy compaction strategy: BINPACK (default), SORT, or ZORDER
 * @param sortOrder column names to sort by when using SORT strategy
 * @param zOrderColumns column names for Z-ordering when using ZORDER strategy
 * @param targetFileSizeBytes target size for output files (default: 512MB)
 * @param maxFileGroupSizeBytes maximum size of file groups to rewrite together (default: 100GB)
 * @param maxConcurrentFileGroupRewrites max concurrent file group rewrites (default: 5)
 * @param partialProgressEnabled enable partial progress commits during compaction
 * @param partialProgressMaxCommits max number of commits for partial progress (default: 10)
 * @param partialProgressMaxFailedCommits max failed commits before aborting
 * @param filter Iceberg expression (JSON) to filter which files to rewrite
 * @param rewriteJobOrder order to process file groups: NONE, BYTES_ASC/DESC, FILES_ASC/DESC
 * @param useStartingSequenceNumber use starting sequence number for new files (default: true)
 * @param removeDanglingDeletes remove delete files with no matching data files
 * @param outputSpecId partition spec ID for output files
 * @see <a href="https://iceberg.apache.org/docs/latest/maintenance/#compact-data-files">Iceberg
 *     RewriteDataFiles</a>
 */
public record RewriteDataFilesOperation(
        RewriteStrategy strategy,
        Optional<List<String>> sortOrder,
        Optional<List<String>> zOrderColumns,
        Optional<Long> targetFileSizeBytes,
        Optional<Long> maxFileGroupSizeBytes,
        int maxConcurrentFileGroupRewrites,
        Boolean partialProgressEnabled,
        int partialProgressMaxCommits,
        Optional<Integer> partialProgressMaxFailedCommits,
        Optional<String> filter,
        RewriteJobOrder rewriteJobOrder,
        Boolean useStartingSequenceNumber,
        Boolean removeDanglingDeletes,
        Optional<Integer> outputSpecId)
        implements MaintenanceOperation {
    public enum RewriteStrategy {
        BINPACK,
        SORT,
        ZORDER,
    }

    public enum RewriteJobOrder {
        BYTES_ASC,
        BYTES_DESC,
        FILES_ASC,
        FILES_DESC,
        NONE,
    }

    public static final long DEFAULT_TARGET_FILE_SIZE = 512 * 1024 * 1024L;

    public RewriteDataFilesOperation {
        if (maxConcurrentFileGroupRewrites < 1) {
            throw new IllegalArgumentException("maxConcurrentFileGroupRewrites must be at least 1");
        }
        if (partialProgressMaxCommits < 1) {
            throw new IllegalArgumentException("partialProgressMaxCommits must be at least 1");
        }
    }

    public static RewriteDataFilesOperation defaults() {
        return new RewriteDataFilesOperation(
                RewriteStrategy.BINPACK,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                5,
                false,
                10,
                Optional.empty(),
                Optional.empty(),
                RewriteJobOrder.NONE,
                true,
                false,
                Optional.empty());
    }

    @Override
    public Type getType() {
        return Type.REWRITE_DATA_FILES;
    }

    @Override
    public String describe() {
        StringBuilder desc =
                new StringBuilder(
                        String.format("Rewrite data files using %s strategy", strategy.name()));
        targetFileSizeBytes.ifPresent(
                v -> desc.append(String.format(" with target file size %d bytes", v)));
        desc.append(
                String.format(
                        ", max concurrent file group rewrites %d", maxConcurrentFileGroupRewrites));
        if (partialProgressEnabled) {
            desc.append(", partial progress enabled");
        }
        if (rewriteJobOrder != RewriteJobOrder.NONE) {
            desc.append(String.format(", rewrite job order %s", rewriteJobOrder.name()));
        }
        desc.append(
                useStartingSequenceNumber
                        ? ", use starting sequence number"
                        : ", do not use starting sequence number");
        if (removeDanglingDeletes) {
            desc.append(", remove dangling deletes");
        }
        outputSpecId.ifPresent(v -> desc.append(String.format(", output spec ID %d", v)));
        return desc.toString();
    }

    public Map<String, String> toOptionsMap() {
        Map<String, String> options = new HashMap<>();

        targetFileSizeBytes.ifPresent(v -> options.put("target-file-size-bytes", v.toString()));
        maxFileGroupSizeBytes.ifPresent(
                v -> options.put("max-file-group-size-bytes", v.toString()));
        options.put(
                "max-concurrent-file-group-rewrites",
                String.valueOf(maxConcurrentFileGroupRewrites));
        options.put("partial-progress.enabled", String.valueOf(partialProgressEnabled));
        options.put("partial-progress.max-commits", String.valueOf(partialProgressMaxCommits));
        partialProgressMaxFailedCommits.ifPresent(
                v -> options.put("partial-progress.max-failed-commits", v.toString()));
        if (rewriteJobOrder != RewriteJobOrder.NONE) {
            options.put(
                    "rewrite-job-order",
                    rewriteJobOrder.name().toLowerCase(java.util.Locale.ROOT).replace('_', '-'));
        }
        options.put("use-starting-sequence-number", String.valueOf(useStartingSequenceNumber));
        if (removeDanglingDeletes) {
            options.put("remove-dangling-deletes", "true");
        }
        outputSpecId.ifPresent(v -> options.put("output-spec-id", v.toString()));

        return options;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private RewriteStrategy strategy = RewriteStrategy.BINPACK;
        private Optional<List<String>> sortOrder = Optional.empty();
        private Optional<List<String>> zOrderColumns = Optional.empty();
        private Optional<Long> targetFileSizeBytes = Optional.of(DEFAULT_TARGET_FILE_SIZE);
        private Optional<Long> maxFileGroupSizeBytes = Optional.empty();
        private int maxConcurrentFileGroupRewrites = 5;
        private Boolean partialProgressEnabled = false;
        private int partialProgressMaxCommits = 10;
        private Optional<Integer> partialProgressMaxFailedCommits = Optional.empty();
        private Optional<String> filter = Optional.empty();
        private RewriteJobOrder rewriteJobOrder = RewriteJobOrder.NONE;
        private Boolean useStartingSequenceNumber = true;
        private Boolean removeDanglingDeletes = false;
        private Optional<Integer> outputSpecId = Optional.empty();

        public Builder strategy(RewriteStrategy strategy) {
            this.strategy = strategy;
            return this;
        }

        public Builder sortOrder(List<String> sortOrder) {
            this.sortOrder = Optional.of(sortOrder);
            return this;
        }

        public Builder zOrderColumns(List<String> cols) {
            this.zOrderColumns = Optional.of(cols);
            return this;
        }

        public Builder targetFileSizeBytes(long bytes) {
            this.targetFileSizeBytes = Optional.of(bytes);
            return this;
        }

        public Builder maxFileGroupSizeBytes(long bytes) {
            this.maxFileGroupSizeBytes = Optional.of(bytes);
            return this;
        }

        public Builder maxConcurrentFileGroupRewrites(int count) {
            this.maxConcurrentFileGroupRewrites = count;
            return this;
        }

        public Builder partialProgressEnabled(Boolean enabled) {
            this.partialProgressEnabled = enabled;
            return this;
        }

        public Builder partialProgressMaxCommits(int commits) {
            this.partialProgressMaxCommits = commits;
            return this;
        }

        public Builder partialProgressMaxFailedCommits(int commits) {
            this.partialProgressMaxFailedCommits = Optional.of(commits);
            return this;
        }

        public Builder filter(String filter) {
            this.filter = Optional.of(filter);
            return this;
        }

        public Builder rewriteJobOrder(RewriteJobOrder order) {
            this.rewriteJobOrder = order;
            return this;
        }

        public Builder useStartingSequenceNumber(Boolean use) {
            this.useStartingSequenceNumber = use;
            return this;
        }

        public Builder removeDanglingDeletes(Boolean remove) {
            this.removeDanglingDeletes = remove;
            return this;
        }

        public Builder outputSpecId(int specId) {
            this.outputSpecId = Optional.of(specId);
            return this;
        }

        public RewriteDataFilesOperation build() {
            return new RewriteDataFilesOperation(
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
