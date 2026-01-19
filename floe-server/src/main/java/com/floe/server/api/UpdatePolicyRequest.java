package com.floe.server.api;

import com.floe.core.policy.*;
import com.floe.server.api.dto.*;
import java.time.Instant;
import java.util.Map;

/** Request DTO for updating a maintenance policy. All fields are optional. */
public record UpdatePolicyRequest(
        String name,
        String description,
        String tablePattern,
        Integer priority,
        Boolean enabled,
        Map<String, String> tags,

        // Operation configs - null means no change, explicit object means replace
        RewriteDataFilesConfigDto rewriteDataFiles,
        ScheduleConfigDto rewriteDataFilesSchedule,
        ExpireSnapshotsConfigDto expireSnapshots,
        ScheduleConfigDto expireSnapshotsSchedule,
        OrphanCleanupConfigDto orphanCleanup,
        ScheduleConfigDto orphanCleanupSchedule,
        RewriteManifestsConfigDto rewriteManifests,
        ScheduleConfigDto rewriteManifestsSchedule) {
    /** Apply updates to existing policy, preserving unchanged fields. */
    public MaintenancePolicy applyTo(MaintenancePolicy existing) {
        return new MaintenancePolicy(
                existing.id(),
                name != null ? name : existing.name(),
                description != null ? description : existing.description(),
                tablePattern != null ? TablePattern.parse(tablePattern) : existing.tablePattern(),
                enabled != null ? enabled : existing.enabled(),
                rewriteDataFiles != null
                        ? rewriteDataFiles.toConfig()
                        : existing.rewriteDataFiles(),
                rewriteDataFilesSchedule != null
                        ? rewriteDataFilesSchedule.toConfig()
                        : existing.rewriteDataFilesSchedule(),
                expireSnapshots != null ? expireSnapshots.toConfig() : existing.expireSnapshots(),
                expireSnapshotsSchedule != null
                        ? expireSnapshotsSchedule.toConfig()
                        : existing.expireSnapshotsSchedule(),
                orphanCleanup != null ? orphanCleanup.toConfig() : existing.orphanCleanup(),
                orphanCleanupSchedule != null
                        ? orphanCleanupSchedule.toConfig()
                        : existing.orphanCleanupSchedule(),
                rewriteManifests != null
                        ? rewriteManifests.toConfig()
                        : existing.rewriteManifests(),
                rewriteManifestsSchedule != null
                        ? rewriteManifestsSchedule.toConfig()
                        : existing.rewriteManifestsSchedule(),
                priority != null ? priority : existing.priority(),
                tags != null ? tags : existing.tags(),
                existing.createdAt(),
                Instant.now() // updatedAt
                );
    }
}
