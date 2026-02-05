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
