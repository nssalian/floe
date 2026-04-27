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
import com.floe.server.api.dto.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Request DTO for creating a new maintenance policy. */
public record CreatePolicyRequest(
        @NotBlank(message = "Policy name is required") String name,
        String description,
        @NotBlank(message = "Table pattern is required") String tablePattern,
        int priority,
        Map<String, String> tags,

        // Operation configs
        RewriteDataFilesConfigDto rewriteDataFiles,
        @Valid ScheduleConfigDto rewriteDataFilesSchedule,
        ExpireSnapshotsConfigDto expireSnapshots,
        @Valid ScheduleConfigDto expireSnapshotsSchedule,
        OrphanCleanupConfigDto orphanCleanup,
        @Valid ScheduleConfigDto orphanCleanupSchedule,
        RewriteManifestsConfigDto rewriteManifests,
        @Valid ScheduleConfigDto rewriteManifestsSchedule,

        // Health thresholds (optional - uses defaults if not specified)
        HealthThresholdsDto healthThresholds,

        // Trigger conditions (optional - null means always trigger when schedule is due)
        TriggerConditionsDto triggerConditions) {
    /** Convert to domain MaintenancePolicy. */
    public MaintenancePolicy toPolicy() {
        Instant now = Instant.now();
        return new MaintenancePolicy(
                UUID.randomUUID().toString(),
                name,
                description,
                TablePattern.parse(tablePattern),
                true, // enabled by default
                rewriteDataFiles != null ? rewriteDataFiles.toConfig() : null,
                rewriteDataFilesSchedule != null ? rewriteDataFilesSchedule.toConfig() : null,
                expireSnapshots != null ? expireSnapshots.toConfig() : null,
                expireSnapshotsSchedule != null ? expireSnapshotsSchedule.toConfig() : null,
                orphanCleanup != null ? orphanCleanup.toConfig() : null,
                orphanCleanupSchedule != null ? orphanCleanupSchedule.toConfig() : null,
                rewriteManifests != null ? rewriteManifests.toConfig() : null,
                rewriteManifestsSchedule != null ? rewriteManifestsSchedule.toConfig() : null,
                priority,
                healthThresholds != null ? healthThresholds.toThresholds() : null,
                triggerConditions != null ? triggerConditions.toConditions() : null,
                tags != null ? tags : Map.of(),
                now,
                now);
    }
}
