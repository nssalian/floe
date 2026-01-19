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
        Instant windowEnd) {
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

    /** Duration of the stats window. */
    public Duration windowDuration() {
        return Duration.between(windowStart, windowEnd);
    }

    /** Create empty stats for a window. */
    public static OperationStats empty(Instant windowStart, Instant windowEnd) {
        return new OperationStats(0, 0, 0, 0, 0, 0, 0, windowStart, windowEnd);
    }

    /** Builder for constructing OperationStats. */
    public static Builder builder() {
        return new Builder();
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
                    windowEnd);
        }
    }
}
