package com.floe.core.maintenance;

/**
 * Sealed interface representing Iceberg table maintenance operations.
 *
 * <p>Each operation type has specific configuration options
 */
public sealed interface MaintenanceOperation
        permits RewriteDataFilesOperation,
                ExpireSnapshotsOperation,
                RewriteManifestsOperation,
                OrphanCleanupOperation {

    /** The type of maintenance operation. */
    enum Type {
        REWRITE_DATA_FILES("Rewrite data files for optimal size"),
        EXPIRE_SNAPSHOTS("Remove old snapshots"),
        REWRITE_MANIFESTS("Optimize manifest files"),
        ORPHAN_CLEANUP("Remove orphaned data files");

        private final String description;

        Type(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /** Get the operation type. */
    Type getType();

    /** Description of what this operation will do. */
    String describe();
}
