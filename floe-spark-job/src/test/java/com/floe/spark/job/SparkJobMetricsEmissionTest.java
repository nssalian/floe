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
