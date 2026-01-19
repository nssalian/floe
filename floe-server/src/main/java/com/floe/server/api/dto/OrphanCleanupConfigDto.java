package com.floe.server.api.dto;

import com.floe.core.policy.OrphanCleanupConfig;
import java.time.Duration;
import java.util.Map;

/** DTO for OrphanCleanup configuration. */
public record OrphanCleanupConfigDto(
        Integer retentionPeriodInDays,
        String location,
        String prefixMismatchMode,
        Map<String, String> equalSchemes,
        Map<String, String> equalAuthorities) {
    public OrphanCleanupConfig toConfig() {
        return new OrphanCleanupConfig(
                retentionPeriodInDays != null
                        ? Duration.ofDays(retentionPeriodInDays)
                        : Duration.ofDays(3),
                location,
                prefixMismatchMode != null ? prefixMismatchMode : "ERROR",
                equalSchemes != null ? equalSchemes : Map.of(),
                equalAuthorities != null ? equalAuthorities : Map.of());
    }
}
