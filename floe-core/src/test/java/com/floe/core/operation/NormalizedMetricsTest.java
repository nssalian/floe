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
