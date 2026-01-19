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
        @Valid ScheduleConfigDto rewriteManifestsSchedule) {
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
                tags != null ? tags : Map.of(),
                now,
                now);
    }
}
