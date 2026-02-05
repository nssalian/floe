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

package com.floe.engine.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SparkExecutionEngineMetricsTest {

    @Test
    void parseMetricsFromJsonLog() {
        SparkExecutionEngine engine =
                new SparkExecutionEngine(
                        SparkEngineConfig.builder().maintenanceJobJar("dummy.jar").build());
        String logs =
                "INFO start\n"
                        + "{\"metricsType\":\"floe\",\"metrics\":{\"filesRewritten\":2,\"bytesRewritten\":100}}\n"
                        + "INFO done\n";

        Map<String, Object> metrics = engine.extractMetricsFromLogs(logs);

        assertEquals(2, ((Number) metrics.get("filesRewritten")).intValue());
        assertEquals(100, ((Number) metrics.get("bytesRewritten")).intValue());
    }

    @Test
    void parseMetricsMalformedJsonHandled() {
        SparkExecutionEngine engine =
                new SparkExecutionEngine(
                        SparkEngineConfig.builder().maintenanceJobJar("dummy.jar").build());
        Map<String, Object> metrics = engine.extractMetricsFromLogs("not-json");

        assertTrue(metrics.isEmpty());
    }
}
