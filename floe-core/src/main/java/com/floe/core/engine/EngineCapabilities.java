package com.floe.core.engine;

import com.floe.core.maintenance.MaintenanceOperation;
import java.util.Set;

/**
 * Capabilities of an execution engine.
 *
 * @param supportedOperations operations this engine can execute
 * @param supportsAsync jobs run independently
 * @param supportsPartitionFiltering can target specific partitions
 * @param supportsIncrementalProcessing can track processing state
 * @param maxConcurrentMaintenanceOperations max parallel operations
 */
public record EngineCapabilities(
        Set<MaintenanceOperation.Type> supportedOperations,
        boolean supportsAsync,
        boolean supportsPartitionFiltering,
        boolean supportsIncrementalProcessing,
        int maxConcurrentMaintenanceOperations) {
    /** Create default capabilities with no special features. */
    public static EngineCapabilities defaultCapabilities() {
        return new EngineCapabilities(Set.of(), false, false, false, 10);
    }

    /** Check if a maintenance operation type is supported. */
    public boolean isOperationSupported(MaintenanceOperation.Type type) {
        return supportedOperations.contains(type);
    }

    /** Spark via Livy */
    public static EngineCapabilities spark() {
        return new EngineCapabilities(
                Set.of(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        MaintenanceOperation.Type.REWRITE_MANIFESTS,
                        MaintenanceOperation.Type.ORPHAN_CLEANUP),
                true, // async - jobs run independently
                true, // partition filtering - rewrite specific partitions
                true, // incremental - track last processed state
                100 // concurrent operations
                );
    }

    /** Trino: Full support for all Iceberg maintenance operations. */
    public static EngineCapabilities trino() {
        return new EngineCapabilities(
                Set.of(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES, // optimize
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        MaintenanceOperation.Type.ORPHAN_CLEANUP, // remove_orphan_files
                        MaintenanceOperation.Type.REWRITE_MANIFESTS), // optimize_manifests
                false,
                false,
                false,
                10);
    }

    /** Create custom capabilities. */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Set<MaintenanceOperation.Type> supportedOperations = Set.of();
        private boolean supportsAsync = false;
        private boolean supportsPartitionFiltering = false;
        private boolean supportsIncrementalProcessing = false;
        private int maxConcurrentOperations = 10;

        public Builder supportedOperations(Set<MaintenanceOperation.Type> ops) {
            this.supportedOperations = ops;
            return this;
        }

        public Builder supportsAsync(boolean value) {
            this.supportsAsync = value;
            return this;
        }

        public Builder supportsPartitionFiltering(boolean value) {
            this.supportsPartitionFiltering = value;
            return this;
        }

        public Builder supportsIncrementalProcessing(boolean value) {
            this.supportsIncrementalProcessing = value;
            return this;
        }

        public Builder maxConcurrentOperations(int value) {
            this.maxConcurrentOperations = value;
            return this;
        }

        public EngineCapabilities build() {
            return new EngineCapabilities(
                    supportedOperations,
                    supportsAsync,
                    supportsPartitionFiltering,
                    supportsIncrementalProcessing,
                    maxConcurrentOperations);
        }
    }
}
