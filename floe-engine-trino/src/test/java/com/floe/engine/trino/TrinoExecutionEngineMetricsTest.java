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
