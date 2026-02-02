package com.floe.core.operation;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class NormalizedMetricsTest {

    @Test
    void keysAllDefined() {
        assertNotNull(NormalizedMetrics.FILES_REWRITTEN);
        assertNotNull(NormalizedMetrics.BYTES_REWRITTEN);
        assertNotNull(NormalizedMetrics.MANIFESTS_REWRITTEN);
        assertNotNull(NormalizedMetrics.SNAPSHOTS_EXPIRED);
        assertNotNull(NormalizedMetrics.DELETE_FILES_REMOVED);
        assertNotNull(NormalizedMetrics.ORPHAN_FILES_REMOVED);
        assertNotNull(NormalizedMetrics.DURATION_MS);
        assertNotNull(NormalizedMetrics.ENGINE_TYPE);
        assertNotNull(NormalizedMetrics.EXECUTION_ID);
    }
}
