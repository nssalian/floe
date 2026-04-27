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

package com.floe.engine.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TrinoExecutionEngineMetricsTest {

    @Test
    void normalizeMetricsFromResultSet() {
        TrinoExecutionEngine engine =
                new TrinoExecutionEngine(
                        TrinoEngineConfig.builder()
                                .jdbcUrl("jdbc:trino://localhost:8080")
                                .username("user")
                                .catalog("demo")
                                .build());

        Map<String, Object> metrics =
                engine.normalizeMetrics(
                        Map.of("rewritten_data_files_count", 5, "rewritten_data_bytes", 2048));

        assertEquals(5, ((Number) metrics.get("filesRewritten")).intValue());
        assertEquals(2048, ((Number) metrics.get("bytesRewritten")).intValue());
        assertTrue(metrics.containsKey("rewritten_data_files_count"));
    }
}
