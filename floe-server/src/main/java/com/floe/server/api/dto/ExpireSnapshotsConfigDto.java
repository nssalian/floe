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

package com.floe.server.api.dto;

import com.floe.core.policy.ExpireSnapshotsConfig;
import java.time.Duration;

/** DTO for ExpireSnapshots configuration. */
public record ExpireSnapshotsConfigDto(
        Integer retainLast,
        String maxSnapshotAge, // ISO-8601 duration, e.g., "P7D"
        Boolean cleanExpiredMetadata,
        Long expireSnapshotId) {
    public ExpireSnapshotsConfig toConfig() {
        return ExpireSnapshotsConfig.builder()
                .retainLast(retainLast != null ? retainLast : 5)
                .maxSnapshotAge(
                        maxSnapshotAge != null
                                ? Duration.parse(maxSnapshotAge)
                                : Duration.ofDays(7))
                .cleanExpiredMetadata(cleanExpiredMetadata != null ? cleanExpiredMetadata : false)
                .expireSnapshotId(expireSnapshotId)
                .build();
    }
}
