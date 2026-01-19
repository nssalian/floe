package com.floe.server.api;

import com.floe.core.operation.OperationStats;
import java.time.Duration;

/** Response for operation statistics. */
public record StatsResponse(
        long totalOperations,
        long successCount,
        long failedCount,
        long partialFailureCount,
        long runningCount,
        long noPolicyCount,
        long noOperationsCount,
        long withFailuresCount,
        long completedCount,
        double successRate,
        String windowStart,
        String windowEnd,
        String windowDuration) {
    public static StatsResponse from(OperationStats stats) {
        return new StatsResponse(
                stats.totalOperations(),
                stats.successCount(),
                stats.failedCount(),
                stats.partialFailureCount(),
                stats.runningCount(),
                stats.noPolicyCount(),
                stats.noOperationsCount(),
                stats.withFailuresCount(),
                stats.completedCount(),
                stats.successRate(),
                stats.windowStart().toString(),
                stats.windowEnd().toString(),
                formatDuration(stats.windowDuration()));
    }

    private static String formatDuration(Duration duration) {
        long hours = duration.toHours();
        if (hours >= 24 && hours % 24 == 0) {
            return (hours / 24) + "d";
        }
        return hours + "h";
    }
}
