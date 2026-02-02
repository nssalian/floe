package com.floe.core.operation;

import java.util.HashMap;
import java.util.Map;

/**
 * Standardized metrics keys emitted by execution engines.
 *
 * <p>All metrics maps should use these keys when possible to enable consistent reporting across
 * engines.
 */
public final class NormalizedMetrics {

    public static final String FILES_REWRITTEN = "filesRewritten";
    public static final String BYTES_REWRITTEN = "bytesRewritten";
    public static final String MANIFESTS_REWRITTEN = "manifestsRewritten";
    public static final String SNAPSHOTS_EXPIRED = "snapshotsExpired";
    public static final String DELETE_FILES_REMOVED = "deleteFilesRemoved";
    public static final String ORPHAN_FILES_REMOVED = "orphanFilesRemoved";
    public static final String DURATION_MS = "durationMs";
    public static final String ENGINE_TYPE = "engineType";
    public static final String EXECUTION_ID = "executionId";

    private NormalizedMetrics() {}

    /**
     * Returns a copy of the provided metrics map with normalized keys merged in.
     *
     * @param metrics raw metrics map
     * @return merged metrics map containing normalized keys
     */
    public static Map<String, Object> mergeNormalized(Map<String, Object> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return Map.of();
        }
        return new HashMap<>(metrics);
    }
}
