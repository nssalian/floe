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
