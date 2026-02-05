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
import java.util.Map;
import java.util.Optional;

/**
 * Configuration for Iceberg orphan file cleanup.
 *
 * <p>Maps to Apache Iceberg's {@code DeleteOrphanFiles} action. Removes files in table locations
 * that are not referenced by any table metadata. Orphan files can accumulate from failed writes or
 * from operations that were interrupted.
 *
 * @param olderThan only delete files older than this duration (default: 3 days)
 * @param location specific location to scan for orphans (defaults to table root)
 * @param prefixMismatchMode how to handle files with mismatched prefixes: ERROR, IGNORE, DELETE
 * @param equalSchemes map of equivalent URI schemes (e.g., s3a -> s3)
 * @param equalAuthorities map of equivalent URI authorities (e.g., bucket.s3.amazonaws.com ->
 *     bucket)
 * @see <a href="https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files">Iceberg
 *     DeleteOrphanFiles</a>
 */
public record OrphanCleanupOperation(
        Duration olderThan,
        Optional<String> location,
        PrefixMismatchMode prefixMismatchMode,
        Map<String, String> equalSchemes,
        Map<String, String> equalAuthorities)
        implements MaintenanceOperation {
    public enum PrefixMismatchMode {
        ERROR,
        IGNORE,
        DELETE,
    }

    public OrphanCleanupOperation {
        if (olderThan.isNegative() || olderThan.isZero()) {
            throw new IllegalArgumentException("olderThan must be positive");
        }
        equalSchemes = Map.copyOf(equalSchemes);
        equalAuthorities = Map.copyOf(equalAuthorities);
    }

    @Override
    public Type getType() {
        return Type.ORPHAN_CLEANUP;
    }

    @Override
    public String describe() {
        String locDesc = location.map(loc -> " at location " + loc).orElse("");
        return String.format(
                "Orphan cleanup older than %s%s with prefix mismatch mode %s",
                olderThan, locDesc, prefixMismatchMode.name());
    }

    public static OrphanCleanupOperation defaults() {
        return new OrphanCleanupOperation(
                Duration.ofDays(3), Optional.empty(), PrefixMismatchMode.ERROR, Map.of(), Map.of());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Duration olderThan = Duration.ofDays(3);
        private Optional<String> location = Optional.empty();
        private PrefixMismatchMode prefixMismatchMode = PrefixMismatchMode.ERROR;
        private Map<String, String> equalSchemes = Map.of();
        private Map<String, String> equalAuthorities = Map.of();

        public Builder olderThan(Duration olderThan) {
            this.olderThan = olderThan;
            return this;
        }

        public Builder location(String location) {
            this.location = Optional.of(location);
            return this;
        }

        public Builder prefixMismatchMode(PrefixMismatchMode mode) {
            this.prefixMismatchMode = mode;
            return this;
        }

        public Builder equalSchemes(Map<String, String> schemes) {
            this.equalSchemes = schemes;
            return this;
        }

        public Builder equalScheme(String scheme1, String scheme2) {
            Map<String, String> newSchemes = new java.util.HashMap<>(this.equalSchemes);
            newSchemes.put(scheme1, scheme2);
            this.equalSchemes = newSchemes;
            return this;
        }

        public Builder equalAuthorities(Map<String, String> authorities) {
            this.equalAuthorities = authorities;
            return this;
        }

        public OrphanCleanupOperation build() {
            return new OrphanCleanupOperation(
                    olderThan, location, prefixMismatchMode, equalSchemes, equalAuthorities);
        }
    }
}
