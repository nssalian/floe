package com.floe.core.policy;

import java.time.Duration;
import java.util.Map;

/** Configuration for orphan file cleanup. */
public record OrphanCleanupConfig(
        Duration retentionPeriodInDays,
        String location,
        String prefixMismatchMode,
        Map<String, String> equalSchemes,
        Map<String, String> equalAuthorities) {
    /** Default configuration: 3 days retention. */
    public static OrphanCleanupConfig defaults() {
        return new OrphanCleanupConfig(Duration.ofDays(3), null, "ERROR", Map.of(), Map.of());
    }

    /** Conservative configuration: 7 days retention. */
    public static OrphanCleanupConfig conservative() {
        return new OrphanCleanupConfig(Duration.ofDays(7), null, "ERROR", Map.of(), Map.of());
    }

    /** Calculate the cutoff timestamp for orphan file deletion. */
    public long calculateCutoffTimestamp() {
        return System.currentTimeMillis() - retentionPeriodInDays.toMillis();
    }

    /** Builder for OrphanCleanupConfig */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Duration retentionPeriodInDays;
        private String location;
        private String prefixMismatchMode = "ERROR";
        private Map<String, String> equalSchemes = Map.of();
        private Map<String, String> equalAuthorities = Map.of();

        public Builder retentionPeriodInDays(Duration retentionPeriodInDays) {
            this.retentionPeriodInDays = retentionPeriodInDays;
            return this;
        }

        public Builder location(String location) {
            this.location = location;
            return this;
        }

        public Builder prefixMismatchMode(String mode) {
            this.prefixMismatchMode = mode;
            return this;
        }

        public Builder equalSchemes(Map<String, String> schemes) {
            this.equalSchemes = schemes != null ? Map.copyOf(schemes) : Map.of();
            return this;
        }

        public Builder equalAuthorities(Map<String, String> authorities) {
            this.equalAuthorities = authorities != null ? Map.copyOf(authorities) : Map.of();
            return this;
        }

        public OrphanCleanupConfig build() {
            return new OrphanCleanupConfig(
                    retentionPeriodInDays,
                    location,
                    prefixMismatchMode,
                    equalSchemes,
                    equalAuthorities);
        }
    }
}
