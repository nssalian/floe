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

import java.time.Duration;
import java.util.Optional;

/**
 * Configuration for Iceberg snapshot expiration.
 *
 * <p>Maps to Apache Iceberg's {@code ExpireSnapshots} action. Removes old snapshots and their
 * associated data files that are no longer needed. This helps reclaim storage space and maintain
 * table performance.
 *
 * @param retainLast minimum number of ancestor snapshots to retain (must be at least 1)
 * @param maxSnapshotAge expire snapshots older than this duration (maps to {@code expireOlderThan})
 * @param cleanExpiredMetadata when true, also removes unused partition specs and schemas
 * @param expireSnapshotId expire a specific snapshot by ID (Spark only, not supported in Trino)
 * @see <a href="https://iceberg.apache.org/docs/latest/maintenance/#expire-snapshots">Iceberg
 *     ExpireSnapshots</a>
 */
public record ExpireSnapshotsOperation(
        int retainLast,
        Optional<Duration> maxSnapshotAge,
        boolean cleanExpiredMetadata,
        Optional<Long> expireSnapshotId)
        implements MaintenanceOperation {
    public ExpireSnapshotsOperation {
        if (retainLast < 1) {
            throw new IllegalArgumentException("retainLast must be at least 1");
        }
    }

    @Override
    public Type getType() {
        return Type.EXPIRE_SNAPSHOTS;
    }

    @Override
    public String describe() {
        StringBuilder desc =
                new StringBuilder(String.format("Expire snapshots, retain last %d", retainLast));
        maxSnapshotAge.ifPresent(age -> desc.append(String.format(", max age %s", age)));
        if (cleanExpiredMetadata) {
            desc.append(", clean expired metadata");
        }
        expireSnapshotId.ifPresent(id -> desc.append(String.format(", expire snapshot ID %d", id)));
        return desc.toString();
    }

    public static ExpireSnapshotsOperation defaults() {
        return new ExpireSnapshotsOperation(
                5, Optional.of(Duration.ofDays(7)), false, Optional.empty());
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for ExpireSnapshotsOperation. */
    public static class Builder {

        private int retainLast = 5;
        private Optional<Duration> maxSnapshotAge = Optional.of(Duration.ofDays(7));
        private boolean cleanExpiredMetadata = false;
        private Optional<Long> expireSnapshotId = Optional.empty();

        public Builder retainLast(int retainLast) {
            if (retainLast < 1) {
                throw new IllegalArgumentException("retainLast must be at least 1");
            }
            this.retainLast = retainLast;
            return this;
        }

        public Builder maxSnapshotAge(Duration maxSnapshotAge) {
            this.maxSnapshotAge = Optional.ofNullable(maxSnapshotAge);
            return this;
        }

        public Builder cleanExpiredMetadata(boolean cleanExpiredMetadata) {
            this.cleanExpiredMetadata = cleanExpiredMetadata;
            return this;
        }

        public Builder expireSnapshotId(long snapshotId) {
            this.expireSnapshotId = Optional.of(snapshotId);
            return this;
        }

        public ExpireSnapshotsOperation build() {
            return new ExpireSnapshotsOperation(
                    retainLast, maxSnapshotAge, cleanExpiredMetadata, expireSnapshotId);
        }
    }
}
