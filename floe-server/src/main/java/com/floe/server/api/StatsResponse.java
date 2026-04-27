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
