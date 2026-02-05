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

package com.floe.core.policy;

import java.time.Duration;

/**
 * Policy configuration for Iceberg ExpireSnapshots action.
 *
 * @see com.floe.core.maintenance.ExpireSnapshotsOperation
 */
public record ExpireSnapshotsConfig(
        Integer retainLast,
        Duration maxSnapshotAge,
        Boolean cleanExpiredMetadata,
        Long expireSnapshotId) {
    public static ExpireSnapshotsConfig defaults() {
        return new ExpireSnapshotsConfig(5, Duration.ofDays(7), false, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Integer retainLast = 5;
        private Duration maxSnapshotAge = Duration.ofDays(7);
        private Boolean cleanExpiredMetadata = false;
        private Long expireSnapshotId = null;

        public Builder retainLast(Integer retainLast) {
            this.retainLast = retainLast;
            return this;
        }

        public Builder maxSnapshotAge(Duration maxSnapshotAge) {
            this.maxSnapshotAge = maxSnapshotAge;
            return this;
        }

        public Builder cleanExpiredMetadata(Boolean cleanExpiredMetadata) {
            this.cleanExpiredMetadata = cleanExpiredMetadata;
            return this;
        }

        public Builder expireSnapshotId(Long expireSnapshotId) {
            this.expireSnapshotId = expireSnapshotId;
            return this;
        }

        public ExpireSnapshotsConfig build() {
            return new ExpireSnapshotsConfig(
                    retainLast, maxSnapshotAge, cleanExpiredMetadata, expireSnapshotId);
        }
    }
}
