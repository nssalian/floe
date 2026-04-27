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

package com.floe.spark.job;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SparkJobMetricsEmissionTest {

    @Test
    void emitMetricsJsonFormat() throws Exception {
        String json =
                MaintenanceJob.formatMetricsJson(
                        Map.of(
                                "filesRewritten",
                                1,
                                "bytesRewritten",
                                2,
                                "manifestsRewritten",
                                3,
                                "snapshotsExpired",
                                4,
                                "deleteFilesRemoved",
                                5,
                                "orphanFilesRemoved",
                                6,
                                "durationMs",
                                7,
                                "engineType",
                                "spark",
                                "executionId",
                                "exec"));

        assertTrue(json.contains("\"metricsType\""));
        assertTrue(json.contains("\"metrics\""));
        assertTrue(json.contains("\"filesRewritten\""));
    }
}
