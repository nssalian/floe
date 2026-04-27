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

package com.floe.core.orchestrator;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthReport;
import com.floe.core.policy.OperationType;
import com.floe.core.policy.TriggerConditions;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TriggerEvaluatorTest {

    private TriggerEvaluator evaluator;
    private Instant now;
    private TableIdentifier tableId;

    @BeforeEach
    void setUp() {
        evaluator = new TriggerEvaluator();
        now = Instant.now();
        tableId = TableIdentifier.of("catalog", "db", "table");
    }

    @Test
    void nullConditionsAlwaysTriggers() {
        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(
                        null, createHealthReport(), null, OperationType.REWRITE_DATA_FILES, now);

        assertTrue(result.shouldTrigger());
        assertFalse(result.forcedByCriticalPipeline());
        assertTrue(result.metConditions().contains("No conditions configured"));
    }

    @Test
    void noConditionsConfiguredAlwaysTriggers() {
        TriggerConditions conditions = TriggerConditions.alwaysTrigger();

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(
                        conditions,
                        createHealthReport(),
                        null,
                        OperationType.REWRITE_DATA_FILES,
                        now);

        assertTrue(result.shouldTrigger());
        assertFalse(conditions.hasConditions());
    }

    @Test
    void smallFilePercentageConditionMet() {
        TriggerConditions conditions =
                TriggerConditions.builder().smallFilePercentageAbove(20.0).build();

        HealthReport health = createHealthReportWithSmallFiles(30.0, 30);

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(conditions, health, null, OperationType.REWRITE_DATA_FILES, now);

        assertTrue(result.shouldTrigger());
        assertEquals(1, result.metConditions().size());
        assertTrue(result.metConditions().get(0).contains("Small file %"));
    }

    @Test
    void smallFilePercentageConditionNotMet() {
        TriggerConditions conditions =
                TriggerConditions.builder().smallFilePercentageAbove(30.0).build();

        HealthReport health = createHealthReportWithSmallFiles(20.0, 20);

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(conditions, health, null, OperationType.REWRITE_DATA_FILES, now);

        assertFalse(result.shouldTrigger());
        assertEquals(1, result.unmetConditions().size());
    }

    @Test
    void deleteFileCountConditionMet() {
        TriggerConditions conditions = TriggerConditions.builder().deleteFileCountAbove(50).build();

        HealthReport health = createHealthReportWithDeleteFiles(100, 0.15);

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(conditions, health, null, OperationType.REWRITE_DATA_FILES, now);

        assertTrue(result.shouldTrigger());
        assertTrue(result.metConditions().get(0).contains("Delete file count"));
    }

    @Test
    void snapshotCountConditionMet() {
        TriggerConditions conditions = TriggerConditions.builder().snapshotCountAbove(50).build();

        HealthReport health = createHealthReportWithSnapshots(100, 10L);

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(conditions, health, null, OperationType.EXPIRE_SNAPSHOTS, now);

        assertTrue(result.shouldTrigger());
        assertTrue(result.metConditions().get(0).contains("Snapshot count"));
    }

    @Test
    void partitionSkewConditionMet() {
        TriggerConditions conditions = TriggerConditions.builder().partitionSkewAbove(2.0).build();

        HealthReport health =
                HealthReport.builder(tableId)
                        .dataFileCount(100)
                        .partitionCount(5)
                        .partitionSkew(3.5)
                        .build();

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(conditions, health, null, OperationType.REWRITE_DATA_FILES, now);

        assertTrue(result.shouldTrigger());
        assertTrue(result.metConditions().get(0).contains("Partition skew"));
    }

    @Test
    void multipleConditionsOrLogic() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(50.0) // Not met
                        .deleteFileCountAbove(50) // Met
                        .snapshotCountAbove(200) // Not met
                        .build();

        HealthReport health = createHealthReport();

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(conditions, health, null, OperationType.REWRITE_DATA_FILES, now);

        assertTrue(result.shouldTrigger());
        assertEquals(1, result.metConditions().size());
        assertEquals(2, result.unmetConditions().size());
    }

    @Test
    void minIntervalBlocksTrigger() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(10.0)
                        .minIntervalMinutes(60)
                        .build();

        HealthReport health = createHealthReportWithSmallFiles(30.0, 30);
        Instant lastOpTime = now.minus(Duration.ofMinutes(30)); // Only 30 minutes ago

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(
                        conditions, health, lastOpTime, OperationType.REWRITE_DATA_FILES, now);

        assertFalse(result.shouldTrigger());
        assertNotNull(result.nextEligibleTime());
        assertTrue(result.unmetConditions().get(0).contains("Min interval"));
    }

    @Test
    void minIntervalElapsedAllowsTrigger() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(10.0)
                        .minIntervalMinutes(60)
                        .build();

        HealthReport health = createHealthReportWithSmallFiles(30.0, 30);
        Instant lastOpTime = now.minus(Duration.ofMinutes(90)); // 90 minutes ago

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(
                        conditions, health, lastOpTime, OperationType.REWRITE_DATA_FILES, now);

        assertTrue(result.shouldTrigger());
        assertNull(result.nextEligibleTime());
    }

    @Test
    void perOperationMinIntervalOverridesGlobal() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(10.0)
                        .minIntervalMinutes(60)
                        .perOperationMinIntervalMinutes(
                                Map.of(OperationType.REWRITE_DATA_FILES, 30))
                        .build();

        HealthReport health = createHealthReportWithSmallFiles(30.0, 30);
        Instant lastOpTime = now.minus(Duration.ofMinutes(45)); // 45 minutes ago

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(
                        conditions, health, lastOpTime, OperationType.REWRITE_DATA_FILES, now);

        // Should trigger because per-operation interval is 30 minutes, and 45 > 30
        assertTrue(result.shouldTrigger());
    }

    @Test
    void criticalPipelineForcesTriggerWhenDeadlineExceeded() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(50.0) // Not met by health report
                        .minIntervalMinutes(60)
                        .criticalPipeline(true)
                        .criticalPipelineMaxDelayMinutes(120)
                        .build();

        HealthReport health = createHealthReportWithSmallFiles(10.0, 10); // Below threshold
        Instant lastOpTime = now.minus(Duration.ofMinutes(150)); // 150 minutes ago > 120 max delay

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(
                        conditions, health, lastOpTime, OperationType.REWRITE_DATA_FILES, now);

        assertTrue(result.shouldTrigger());
        assertTrue(result.forcedByCriticalPipeline());
        assertTrue(result.metConditions().get(0).contains("Critical pipeline deadline"));
    }

    @Test
    void criticalPipelineDoesNotForceWhenDeadlineNotExceeded() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(50.0) // Not met
                        .criticalPipeline(true)
                        .criticalPipelineMaxDelayMinutes(120)
                        .build();

        HealthReport health = createHealthReportWithSmallFiles(10.0, 10); // Below threshold
        Instant lastOpTime = now.minus(Duration.ofMinutes(60)); // Only 60 minutes ago

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(
                        conditions, health, lastOpTime, OperationType.REWRITE_DATA_FILES, now);

        assertFalse(result.shouldTrigger());
        assertFalse(result.forcedByCriticalPipeline());
    }

    @Test
    void noHealthDataWithConditionsDoesNotTrigger() {
        TriggerConditions conditions =
                TriggerConditions.builder().smallFilePercentageAbove(20.0).build();

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(conditions, null, null, OperationType.REWRITE_DATA_FILES, now);

        assertFalse(result.shouldTrigger());
        assertTrue(result.unmetConditions().get(0).contains("No health data"));
    }

    @Test
    void noHealthDataWithNoConditionsTriggers() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .minIntervalMinutes(60) // Only time gate, no threshold conditions
                        .build();

        TriggerEvaluator.EvaluationResult result =
                evaluator.evaluate(conditions, null, null, OperationType.REWRITE_DATA_FILES, now);

        assertTrue(result.shouldTrigger());
    }

    @Test
    void evaluationResultAlwaysTriggerFactory() {
        TriggerEvaluator.EvaluationResult result =
                TriggerEvaluator.EvaluationResult.alwaysTrigger();

        assertTrue(result.shouldTrigger());
        assertFalse(result.forcedByCriticalPipeline());
        assertTrue(result.metConditions().contains("No conditions configured"));
        assertTrue(result.unmetConditions().isEmpty());
        assertNull(result.nextEligibleTime());
    }

    @Test
    void evaluationResultBlockedFactory() {
        Instant nextEligible = now.plus(Duration.ofMinutes(30));
        TriggerEvaluator.EvaluationResult result =
                TriggerEvaluator.EvaluationResult.blocked(
                        "Min interval: 30 minutes remaining", nextEligible);

        assertFalse(result.shouldTrigger());
        assertFalse(result.forcedByCriticalPipeline());
        assertTrue(result.metConditions().isEmpty());
        assertEquals(1, result.unmetConditions().size());
        assertEquals(nextEligible, result.nextEligibleTime());
    }

    // Helper methods to create test health reports using the builder

    private HealthReport createHealthReport() {
        return HealthReport.builder(tableId)
                .assessedAt(now)
                .dataFileCount(100)
                .smallFileCount(25)
                .largeFileCount(5)
                .totalDataSizeBytes(1024 * 1024 * 1024L)
                .snapshotCount(50)
                .oldestSnapshotAgeDays(7L)
                .deleteFileCount(100)
                .deleteFileRatio(0.10)
                .manifestCount(10)
                .totalManifestSizeBytes(50 * 1024 * 1024L)
                .partitionCount(100)
                .build();
    }

    private HealthReport createHealthReportWithSmallFiles(double percentage, int count) {
        // To get the desired percentage, we need: count / dataFileCount * 100 = percentage
        // So dataFileCount = count * 100 / percentage
        int dataFileCount = (int) ((count * 100.0) / percentage);
        return HealthReport.builder(tableId)
                .assessedAt(now)
                .dataFileCount(dataFileCount)
                .smallFileCount(count)
                .largeFileCount(5)
                .totalDataSizeBytes(1024 * 1024 * 1024L)
                .snapshotCount(50)
                .oldestSnapshotAgeDays(7L)
                .deleteFileCount(50)
                .deleteFileRatio(0.05)
                .manifestCount(10)
                .totalManifestSizeBytes(50 * 1024 * 1024L)
                .partitionCount(100)
                .build();
    }

    private HealthReport createHealthReportWithDeleteFiles(int count, double ratio) {
        return HealthReport.builder(tableId)
                .assessedAt(now)
                .dataFileCount(100)
                .smallFileCount(20)
                .largeFileCount(5)
                .totalDataSizeBytes(1024 * 1024 * 1024L)
                .snapshotCount(50)
                .oldestSnapshotAgeDays(7L)
                .deleteFileCount(count)
                .deleteFileRatio(ratio)
                .manifestCount(10)
                .totalManifestSizeBytes(50 * 1024 * 1024L)
                .partitionCount(100)
                .build();
    }

    private HealthReport createHealthReportWithSnapshots(int count, Long ageDays) {
        return HealthReport.builder(tableId)
                .assessedAt(now)
                .dataFileCount(100)
                .smallFileCount(20)
                .largeFileCount(5)
                .totalDataSizeBytes(1024 * 1024 * 1024L)
                .snapshotCount(count)
                .oldestSnapshotAgeDays(ageDays)
                .deleteFileCount(50)
                .deleteFileRatio(0.05)
                .manifestCount(10)
                .totalManifestSizeBytes(50 * 1024 * 1024L)
                .partitionCount(100)
                .build();
    }
}
