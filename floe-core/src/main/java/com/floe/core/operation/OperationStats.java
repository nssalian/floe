package com.floe.core.operation;

import java.time.Duration;
import java.time.Instant;

/**
 * Aggregated statistics for maintenance operations within a time window.
 *
 * @param totalOperations total operation count
 * @param successCount successful operations
 * @param failedCount failed operations
 * @param partialFailureCount partial failures
 * @param runningCount currently running
 * @param noPolicyCount no matching policy
 * @param noOperationsCount no operations configured
 * @param windowStart stats window start
 * @param windowEnd stats window end
 * @param zeroChangeRuns runs with no changes detected
 * @param consecutiveFailures consecutive failed runs
 * @param consecutiveZeroChangeRuns consecutive runs with no changes
 * @param averageBytesRewritten average bytes rewritten per run
 * @param averageFilesRewritten average files rewritten per run
 * @param lastRunAt timestamp of most recent run
 */
public record OperationStats(
        long totalOperations,
        long successCount,
        long failedCount,
        long partialFailureCount,
        long runningCount,
        long noPolicyCount,
        long noOperationsCount,
        Instant windowStart,
        Instant windowEnd,
        long zeroChangeRuns,
        long consecutiveFailures,
        long consecutiveZeroChangeRuns,
        double averageBytesRewritten,
        double averageFilesRewritten,
        Instant lastRunAt) {
    /** Count of operations with any failures (FAILED + PARTIAL_FAILURE). */
    public long withFailuresCount() {
        return failedCount + partialFailureCount;
    }

    /** Count of completed operations (all terminal states except running). */
    public long completedCount() {
        return totalOperations - runningCount;
    }

    /** Success rate as a percentage (0-100). */
    public double successRate() {
        long completed = completedCount();
        if (completed == 0) {
            return 0.0;
        }
        return (successCount * 100.0) / completed;
    }

    /** Failure rate as a percentage (0-100). */
    public double failureRate() {
        long completed = completedCount();
        if (completed == 0) {
            return 0.0;
        }
        long failures = failedCount + partialFailureCount;
        return (failures * 100.0) / completed;
    }

    /** Average changes per run (bytes preferred, falls back to files). */
    public double averageChanges() {
        if (averageBytesRewritten > 0) {
            return averageBytesRewritten;
        }
        return averageFilesRewritten;
    }

    /** Duration of the stats window. */
    public Duration windowDuration() {
        return Duration.between(windowStart, windowEnd);
    }

    /** Create empty stats for a window. */
    public static OperationStats empty(Instant windowStart, Instant windowEnd) {
        return new OperationStats(0, 0, 0, 0, 0, 0, 0, windowStart, windowEnd, 0, 0, 0, 0, 0, null);
    }

    /** Builder for constructing OperationStats. */
    public static Builder builder() {
        return new Builder();
    }

    /** Build stats from recent operation records. */
    public static OperationStats fromRecords(java.util.List<OperationRecord> records, int lastN) {
        if (records == null || records.isEmpty() || lastN <= 0) {
            Instant now = Instant.now();
            return empty(now, now);
        }

        java.util.List<OperationRecord> sorted =
                records.stream()
                        .sorted(
                                java.util.Comparator.comparing(OperationRecord::startedAt)
                                        .reversed())
                        .limit(lastN)
                        .toList();

        Instant windowEnd = sorted.get(0).startedAt();
        Instant windowStart = sorted.get(sorted.size() - 1).startedAt();

        long success = 0;
        long failed = 0;
        long partial = 0;
        long running = 0;
        long noPolicy = 0;
        long noOps = 0;
        long zeroChangeRuns = 0;

        long bytesSum = 0;
        long filesSum = 0;

        long consecutiveFailures = 0;
        long consecutiveZeroChanges = 0;
        boolean stillCountingFailures = true;
        boolean stillCountingZeroChanges = true;

        for (OperationRecord record : sorted) {
            switch (record.status()) {
                case SUCCESS -> success++;
                case FAILED -> failed++;
                case PARTIAL_FAILURE -> partial++;
                case RUNNING, PENDING -> running++;
                case NO_POLICY -> noPolicy++;
                case NO_OPERATIONS -> noOps++;
            }

            java.util.Map<String, Object> metrics =
                    record.normalizedMetrics() != null
                            ? record.normalizedMetrics()
                            : record.results() != null
                                    ? record.results().aggregatedMetrics()
                                    : java.util.Map.of();

            long bytes = getLong(metrics, NormalizedMetrics.BYTES_REWRITTEN);
            long files = getLong(metrics, NormalizedMetrics.FILES_REWRITTEN);
            long manifests = getLong(metrics, NormalizedMetrics.MANIFESTS_REWRITTEN);
            long snapshots = getLong(metrics, NormalizedMetrics.SNAPSHOTS_EXPIRED);
            long deleteFiles = getLong(metrics, NormalizedMetrics.DELETE_FILES_REMOVED);
            long orphanFiles = getLong(metrics, NormalizedMetrics.ORPHAN_FILES_REMOVED);

            boolean isZeroChange =
                    bytes == 0
                            && files == 0
                            && manifests == 0
                            && snapshots == 0
                            && deleteFiles == 0
                            && orphanFiles == 0;

            if (isZeroChange) {
                zeroChangeRuns++;
            }

            if (stillCountingFailures) {
                if (record.status() == OperationStatus.FAILED
                        || record.status() == OperationStatus.PARTIAL_FAILURE) {
                    consecutiveFailures++;
                } else {
                    stillCountingFailures = false;
                }
            }

            if (stillCountingZeroChanges) {
                if (isZeroChange) {
                    consecutiveZeroChanges++;
                } else {
                    stillCountingZeroChanges = false;
                }
            }

            bytesSum += bytes;
            filesSum += files;
        }

        double avgBytes = sorted.isEmpty() ? 0 : (bytesSum / (double) sorted.size());
        double avgFiles = sorted.isEmpty() ? 0 : (filesSum / (double) sorted.size());

        return new OperationStats(
                sorted.size(),
                success,
                failed,
                partial,
                running,
                noPolicy,
                noOps,
                windowStart,
                windowEnd,
                zeroChangeRuns,
                consecutiveFailures,
                consecutiveZeroChanges,
                avgBytes,
                avgFiles,
                windowEnd);
    }

    public static class Builder {

        private long totalOperations;
        private long successCount;
        private long failedCount;
        private long partialFailureCount;
        private long runningCount;
        private long noPolicyCount;
        private long noOperationsCount;
        private Instant windowStart;
        private Instant windowEnd;
        private long zeroChangeRuns;
        private long consecutiveFailures;
        private long consecutiveZeroChangeRuns;
        private double averageBytesRewritten;
        private double averageFilesRewritten;
        private Instant lastRunAt;

        public Builder totalOperations(long count) {
            this.totalOperations = count;
            return this;
        }

        public Builder successCount(long count) {
            this.successCount = count;
            return this;
        }

        public Builder failedCount(long count) {
            this.failedCount = count;
            return this;
        }

        public Builder partialFailureCount(long count) {
            this.partialFailureCount = count;
            return this;
        }

        public Builder runningCount(long count) {
            this.runningCount = count;
            return this;
        }

        public Builder noPolicyCount(long count) {
            this.noPolicyCount = count;
            return this;
        }

        public Builder noOperationsCount(long count) {
            this.noOperationsCount = count;
            return this;
        }

        public Builder windowStart(Instant start) {
            this.windowStart = start;
            return this;
        }

        public Builder windowEnd(Instant end) {
            this.windowEnd = end;
            return this;
        }

        public Builder zeroChangeRuns(long zeroChangeRuns) {
            this.zeroChangeRuns = zeroChangeRuns;
            return this;
        }

        public Builder consecutiveFailures(long consecutiveFailures) {
            this.consecutiveFailures = consecutiveFailures;
            return this;
        }

        public Builder consecutiveZeroChangeRuns(long consecutiveZeroChangeRuns) {
            this.consecutiveZeroChangeRuns = consecutiveZeroChangeRuns;
            return this;
        }

        public Builder averageBytesRewritten(double averageBytesRewritten) {
            this.averageBytesRewritten = averageBytesRewritten;
            return this;
        }

        public Builder averageFilesRewritten(double averageFilesRewritten) {
            this.averageFilesRewritten = averageFilesRewritten;
            return this;
        }

        public Builder lastRunAt(Instant lastRunAt) {
            this.lastRunAt = lastRunAt;
            return this;
        }

        public OperationStats build() {
            return new OperationStats(
                    totalOperations,
                    successCount,
                    failedCount,
                    partialFailureCount,
                    runningCount,
                    noPolicyCount,
                    noOperationsCount,
                    windowStart,
                    windowEnd,
                    zeroChangeRuns,
                    consecutiveFailures,
                    consecutiveZeroChangeRuns,
                    averageBytesRewritten,
                    averageFilesRewritten,
                    lastRunAt);
        }
    }

    /**
     * Extract a long value from a metrics map.
     *
     * @param metrics the metrics map
     * @param key the key to look up
     * @return the long value, or 0 if key not found or value is not a number
     */
    private static long getLong(java.util.Map<String, Object> metrics, String key) {
        Object value = metrics.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        return 0L;
    }
}
