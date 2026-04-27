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

import com.floe.core.policy.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/** Response DTO for maintenance policy details. */
public record PolicyResponse(
        String id,
        String name,
        String description,
        String tablePattern,
        boolean enabled,
        int priority,
        Map<String, String> tags,
        Instant createdAt,
        Instant updatedAt,
        OperationSummary rewriteDataFiles,
        OperationSummary expireSnapshots,
        OperationSummary orphanCleanup,
        OperationSummary rewriteManifests,
        HealthThresholdsDto healthThresholds,
        TriggerConditionsDto triggerConditions) {
    /** Create from domain MaintenancePolicy. */
    public static PolicyResponse from(MaintenancePolicy policy) {
        return new PolicyResponse(
                policy.id(),
                policy.name(),
                policy.description(),
                policy.tablePattern().toString(),
                policy.enabled(),
                policy.priority(),
                policy.tags(),
                policy.createdAt(),
                policy.updatedAt(),
                toSummary(OperationType.REWRITE_DATA_FILES, policy),
                toSummary(OperationType.EXPIRE_SNAPSHOTS, policy),
                toSummary(OperationType.ORPHAN_CLEANUP, policy),
                toSummary(OperationType.REWRITE_MANIFESTS, policy),
                HealthThresholdsDto.from(policy.healthThresholds()),
                TriggerConditionsDto.from(policy.triggerConditions()));
    }

    private static OperationSummary toSummary(OperationType type, MaintenancePolicy policy) {
        boolean configured =
                switch (type) {
                    case REWRITE_DATA_FILES -> policy.rewriteDataFiles() != null;
                    case EXPIRE_SNAPSHOTS -> policy.expireSnapshots() != null;
                    case ORPHAN_CLEANUP -> policy.orphanCleanup() != null;
                    case REWRITE_MANIFESTS -> policy.rewriteManifests() != null;
                };

        if (!configured) {
            return null;
        }
        boolean enabled = policy.enabled();
        ScheduleConfig schedule = policy.getSchedule(type);
        String scheduleStr =
                schedule != null && schedule.intervalInDays() != null
                        ? formatDuration(schedule.intervalInDays())
                        : (schedule != null && schedule.cronExpression() != null
                                ? schedule.cronExpression()
                                : null);
        return new OperationSummary(enabled, scheduleStr);
    }

    /**
     * Format a Duration as an ISO-8601 period string (P1D, P7D, etc.) for whole days, or fall back
     * to Duration format (PT24H, PT6H) for sub-day intervals.
     */
    private static String formatDuration(Duration duration) {
        long hours = duration.toHours();
        if (hours > 0 && hours % 24 == 0) {
            long days = hours / 24;
            return "P" + days + "D";
        }
        return duration.toString();
    }

    /** Summary of an operation's configuration. */
    public record OperationSummary(boolean enabled, String schedule) {}
}
