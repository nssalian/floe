package com.floe.core.policy;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a maintenance policy for Iceberg tables.
 *
 * @param id unique identifier
 * @param name policy name
 * @param description policy description
 * @param tablePattern pattern to match tables
 * @param enabled whether policy is active
 * @param rewriteDataFiles compaction config
 * @param rewriteDataFilesSchedule compaction schedule
 * @param expireSnapshots snapshot expiration config
 * @param expireSnapshotsSchedule snapshot expiration schedule
 * @param orphanCleanup orphan cleanup config
 * @param orphanCleanupSchedule orphan cleanup schedule
 * @param rewriteManifests manifest rewrite config
 * @param rewriteManifestsSchedule manifest rewrite schedule
 * @param priority policy priority (higher wins)
 * @param tags metadata tags
 * @param createdAt creation timestamp
 * @param updatedAt last update timestamp
 */
public record MaintenancePolicy(
        String id,
        String name,
        String description,
        TablePattern tablePattern,
        Boolean enabled,
        RewriteDataFilesConfig rewriteDataFiles,
        ScheduleConfig rewriteDataFilesSchedule,
        ExpireSnapshotsConfig expireSnapshots,
        ScheduleConfig expireSnapshotsSchedule,
        OrphanCleanupConfig orphanCleanup,
        ScheduleConfig orphanCleanupSchedule,
        RewriteManifestsConfig rewriteManifests,
        ScheduleConfig rewriteManifestsSchedule,
        int priority,
        Map<String, String> tags,
        Instant createdAt,
        Instant updatedAt) {
    /**
     * Calculate effective priority including pattern specificity. Used for policy resolution when
     * multiple policies match.
     */
    public int effectivePriority() {
        return priority * 1000 + tablePattern.specificity();
    }

    /** Get name or default if not set. */
    public String getNameOrDefault() {
        return (name == null || name.isBlank()) ? "Policy-" + id : name;
    }

    /** Check if the policy is active (enabled). */
    public boolean isActive() {
        return enabled;
    }

    /** Check if this policy has any operations configured. */
    public boolean hasAnyOperations() {
        return (rewriteDataFiles != null
                || expireSnapshots != null
                || orphanCleanup != null
                || rewriteManifests != null);
    }

    /** Check if a specific operation is enabled. */
    public boolean isOperationEnabled(OperationType operation) {
        return switch (operation) {
            case REWRITE_DATA_FILES ->
                    rewriteDataFiles != null
                            && rewriteDataFilesSchedule != null
                            && Boolean.TRUE.equals(rewriteDataFilesSchedule.enabled());
            case EXPIRE_SNAPSHOTS ->
                    expireSnapshots != null
                            && expireSnapshotsSchedule != null
                            && Boolean.TRUE.equals(expireSnapshotsSchedule.enabled());
            case ORPHAN_CLEANUP ->
                    orphanCleanup != null
                            && orphanCleanupSchedule != null
                            && Boolean.TRUE.equals(orphanCleanupSchedule.enabled());
            case REWRITE_MANIFESTS ->
                    rewriteManifests != null
                            && rewriteManifestsSchedule != null
                            && Boolean.TRUE.equals(rewriteManifestsSchedule.enabled());
        };
    }

    /**
     * Check if operation config exists (for manual triggers). Unlike isOperationEnabled(), this
     * doesn't require a schedule.
     */
    public boolean hasOperationConfig(OperationType operation) {
        return switch (operation) {
            case REWRITE_DATA_FILES -> rewriteDataFiles != null;
            case EXPIRE_SNAPSHOTS -> expireSnapshots != null;
            case ORPHAN_CLEANUP -> orphanCleanup != null;
            case REWRITE_MANIFESTS -> rewriteManifests != null;
        };
    }

    /** Get schedule for a specific operation. */
    public ScheduleConfig getSchedule(OperationType operation) {
        return switch (operation) {
            case REWRITE_DATA_FILES -> rewriteDataFilesSchedule;
            case EXPIRE_SNAPSHOTS -> expireSnapshotsSchedule;
            case ORPHAN_CLEANUP -> orphanCleanupSchedule;
            case REWRITE_MANIFESTS -> rewriteManifestsSchedule;
        };
    }

    /** Default policy for all tables */
    public static MaintenancePolicy defaultPolicy() {
        return new MaintenancePolicy(
                "default",
                "Default Policy",
                "Applies to all tables with no specific operations configured.",
                TablePattern.matchAll(),
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                0,
                Map.of(),
                Instant.EPOCH,
                Instant.EPOCH);
    }

    /** Builder for MaintenancePolicy. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for MaintenancePolicy. */
    public static class Builder {

        private String id = UUID.randomUUID().toString();
        private String name;
        private String description;
        private TablePattern tablePattern = TablePattern.matchAll();
        private Boolean enabled = true;
        private RewriteDataFilesConfig rewriteDataFiles;
        private ScheduleConfig rewriteDataFilesSchedule;
        private ExpireSnapshotsConfig expireSnapshots;
        private ScheduleConfig expireSnapshotsSchedule;
        private OrphanCleanupConfig orphanCleanup;
        private ScheduleConfig orphanCleanupSchedule;
        private RewriteManifestsConfig rewriteManifests;
        private ScheduleConfig rewriteManifestsSchedule;
        private int priority;
        private Map<String, String> tags;
        private Instant createdAt;
        private Instant updatedAt;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder tablePattern(TablePattern tablePattern) {
            this.tablePattern = tablePattern;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder tags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder enabled(Boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder updatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder rewriteDataFiles(RewriteDataFilesConfig rewriteDataFiles) {
            this.rewriteDataFiles = rewriteDataFiles;
            return this;
        }

        public Builder rewriteDataFilesSchedule(ScheduleConfig rewriteDataFilesSchedule) {
            this.rewriteDataFilesSchedule = rewriteDataFilesSchedule;
            return this;
        }

        public Builder expireSnapshots(ExpireSnapshotsConfig expireSnapshots) {
            this.expireSnapshots = expireSnapshots;
            return this;
        }

        public Builder expireSnapshotsSchedule(ScheduleConfig expireSnapshotsSchedule) {
            this.expireSnapshotsSchedule = expireSnapshotsSchedule;
            return this;
        }

        public Builder orphanCleanup(OrphanCleanupConfig orphanCleanup) {
            this.orphanCleanup = orphanCleanup;
            return this;
        }

        public Builder orphanCleanupSchedule(ScheduleConfig orphanCleanupSchedule) {
            this.orphanCleanupSchedule = orphanCleanupSchedule;
            return this;
        }

        public Builder rewriteManifests(RewriteManifestsConfig rewriteManifests) {
            this.rewriteManifests = rewriteManifests;
            return this;
        }

        public Builder rewriteManifestsSchedule(ScheduleConfig rewriteManifestsSchedule) {
            this.rewriteManifestsSchedule = rewriteManifestsSchedule;
            return this;
        }

        public MaintenancePolicy build() {
            return new MaintenancePolicy(
                    id,
                    name,
                    description,
                    tablePattern,
                    enabled,
                    rewriteDataFiles,
                    rewriteDataFilesSchedule,
                    expireSnapshots,
                    expireSnapshotsSchedule,
                    orphanCleanup,
                    orphanCleanupSchedule,
                    rewriteManifests,
                    rewriteManifestsSchedule,
                    priority,
                    tags,
                    createdAt,
                    updatedAt);
        }
    }
}
