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
