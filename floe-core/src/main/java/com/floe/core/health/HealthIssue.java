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

package com.floe.core.health;

/**
 * Represents a health issue found during table assessment.
 *
 * @param type issue type
 * @param severity issue severity
 * @param message human-readable description
 */
public record HealthIssue(Type type, Severity severity, String message) {
    public enum Type {
        // Data file issues
        TOO_MANY_SMALL_FILES,
        TOO_MANY_LARGE_FILES,
        HIGH_FILE_COUNT,

        // Snapshot issues
        TOO_MANY_SNAPSHOTS,
        OLD_SNAPSHOTS,

        // Delete file issues
        TOO_MANY_DELETE_FILES,
        HIGH_DELETE_FILE_RATIO,

        // Manifest issues
        TOO_MANY_MANIFESTS,
        LARGE_MANIFEST_LIST,

        // Partition issues
        TOO_MANY_PARTITIONS,
        PARTITION_SKEW,

        // General
        TABLE_EMPTY,
        STALE_METADATA,
    }

    public enum Severity {
        INFO, // Informational, no action needed
        WARNING, // Should address soon
        CRITICAL, // Needs immediate attention
    }

    public static HealthIssue info(Type type, String message) {
        return new HealthIssue(type, Severity.INFO, message);
    }

    public static HealthIssue warning(Type type, String message) {
        return new HealthIssue(type, Severity.WARNING, message);
    }

    public static HealthIssue critical(Type type, String message) {
        return new HealthIssue(type, Severity.CRITICAL, message);
    }

    public boolean isCritical() {
        return severity == Severity.CRITICAL;
    }

    public boolean isWarningOrWorse() {
        return severity == Severity.WARNING || severity == Severity.CRITICAL;
    }
}
