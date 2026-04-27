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

package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TriggerConditionsTest {

    @Test
    void defaultsHasReasonableValues() {
        TriggerConditions defaults = TriggerConditions.defaults();

        assertEquals(20.0, defaults.smallFilePercentageAbove());
        assertEquals(100L, defaults.smallFileCountAbove());
        assertEquals(50, defaults.deleteFileCountAbove());
        assertEquals(0.10, defaults.deleteFileRatioAbove());
        assertEquals(50, defaults.snapshotCountAbove());
        assertEquals(7, defaults.snapshotAgeAboveDays());
        assertEquals(100 * 1024 * 1024L, defaults.manifestSizeAboveBytes());
        assertEquals(60, defaults.minIntervalMinutes());
        assertFalse(defaults.criticalPipeline());
    }

    @Test
    void alwaysTriggerHasNoConditions() {
        TriggerConditions alwaysTrigger = TriggerConditions.alwaysTrigger();

        assertNull(alwaysTrigger.smallFilePercentageAbove());
        assertNull(alwaysTrigger.smallFileCountAbove());
        assertNull(alwaysTrigger.deleteFileCountAbove());
        assertNull(alwaysTrigger.deleteFileRatioAbove());
        assertNull(alwaysTrigger.snapshotCountAbove());
        assertNull(alwaysTrigger.snapshotAgeAboveDays());
        assertNull(alwaysTrigger.manifestSizeAboveBytes());
        assertNull(alwaysTrigger.minIntervalMinutes());
        assertFalse(alwaysTrigger.hasConditions());
    }

    @Test
    void conservativeHasHigherThresholds() {
        TriggerConditions conservative = TriggerConditions.conservative();
        TriggerConditions defaults = TriggerConditions.defaults();

        assertTrue(conservative.smallFilePercentageAbove() > defaults.smallFilePercentageAbove());
        assertTrue(conservative.smallFileCountAbove() > defaults.smallFileCountAbove());
        assertTrue(conservative.deleteFileCountAbove() > defaults.deleteFileCountAbove());
        assertTrue(conservative.deleteFileRatioAbove() > defaults.deleteFileRatioAbove());
        assertTrue(conservative.snapshotCountAbove() > defaults.snapshotCountAbove());
        assertTrue(conservative.snapshotAgeAboveDays() > defaults.snapshotAgeAboveDays());
        assertTrue(conservative.minIntervalMinutes() > defaults.minIntervalMinutes());
    }

    @Test
    void hasConditionsReturnsTrueWhenAnyConditionSet() {
        TriggerConditions withSmallFile =
                TriggerConditions.builder().smallFilePercentageAbove(10.0).build();
        assertTrue(withSmallFile.hasConditions());

        TriggerConditions withDeleteFile =
                TriggerConditions.builder().deleteFileCountAbove(100).build();
        assertTrue(withDeleteFile.hasConditions());

        TriggerConditions withSnapshot = TriggerConditions.builder().snapshotCountAbove(50).build();
        assertTrue(withSnapshot.hasConditions());
    }

    @Test
    void hasConditionsReturnsFalseWhenNoThresholdSet() {
        TriggerConditions onlyTimeGates =
                TriggerConditions.builder()
                        .minIntervalMinutes(60)
                        .criticalPipeline(true)
                        .criticalPipelineMaxDelayMinutes(120)
                        .build();
        assertFalse(onlyTimeGates.hasConditions());
    }

    @Test
    void isCriticalPipelineReturnsTrueWhenEnabled() {
        TriggerConditions critical =
                TriggerConditions.builder()
                        .criticalPipeline(true)
                        .criticalPipelineMaxDelayMinutes(60)
                        .build();
        assertTrue(critical.isCriticalPipeline());

        TriggerConditions notCritical = TriggerConditions.builder().criticalPipeline(false).build();
        assertFalse(notCritical.isCriticalPipeline());
    }

    @Test
    void getMinIntervalMinutesReturnsPerOperationIfSet() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .minIntervalMinutes(60)
                        .perOperationMinIntervalMinutes(
                                Map.of(
                                        OperationType.REWRITE_DATA_FILES,
                                        30,
                                        OperationType.EXPIRE_SNAPSHOTS,
                                        120))
                        .build();

        assertEquals(30, conditions.getMinIntervalMinutes(OperationType.REWRITE_DATA_FILES));
        assertEquals(120, conditions.getMinIntervalMinutes(OperationType.EXPIRE_SNAPSHOTS));
        assertEquals(60, conditions.getMinIntervalMinutes(OperationType.ORPHAN_CLEANUP));
        assertEquals(60, conditions.getMinIntervalMinutes(OperationType.REWRITE_MANIFESTS));
    }

    @Test
    void getMinIntervalMinutesReturnsGlobalWhenNoPerOperationSet() {
        TriggerConditions conditions = TriggerConditions.builder().minIntervalMinutes(90).build();

        assertEquals(90, conditions.getMinIntervalMinutes(OperationType.REWRITE_DATA_FILES));
        assertEquals(90, conditions.getMinIntervalMinutes(OperationType.EXPIRE_SNAPSHOTS));
    }

    @Test
    void getMinIntervalMinutesReturnsNullWhenNothingSet() {
        TriggerConditions conditions = TriggerConditions.builder().build();

        assertNull(conditions.getMinIntervalMinutes(OperationType.REWRITE_DATA_FILES));
    }

    @Test
    void builderCreatesValidConditions() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(25.0)
                        .smallFileCountAbove(200L)
                        .totalFileSizeAboveBytes(10L * 1024 * 1024 * 1024)
                        .deleteFileCountAbove(100)
                        .deleteFileRatioAbove(0.15)
                        .snapshotCountAbove(75)
                        .snapshotAgeAboveDays(14)
                        .partitionCountAbove(1000)
                        .partitionSkewAbove(5.0)
                        .manifestSizeAboveBytes(200 * 1024 * 1024L)
                        .minIntervalMinutes(45)
                        .criticalPipeline(true)
                        .criticalPipelineMaxDelayMinutes(180)
                        .build();

        assertEquals(25.0, conditions.smallFilePercentageAbove());
        assertEquals(200L, conditions.smallFileCountAbove());
        assertEquals(10L * 1024 * 1024 * 1024, conditions.totalFileSizeAboveBytes());
        assertEquals(100, conditions.deleteFileCountAbove());
        assertEquals(0.15, conditions.deleteFileRatioAbove());
        assertEquals(75, conditions.snapshotCountAbove());
        assertEquals(14, conditions.snapshotAgeAboveDays());
        assertEquals(1000, conditions.partitionCountAbove());
        assertEquals(5.0, conditions.partitionSkewAbove());
        assertEquals(200 * 1024 * 1024L, conditions.manifestSizeAboveBytes());
        assertEquals(45, conditions.minIntervalMinutes());
        assertTrue(conditions.criticalPipeline());
        assertEquals(180, conditions.criticalPipelineMaxDelayMinutes());
        assertEquals(TriggerConditions.TriggerLogic.OR, conditions.triggerLogic());
    }

    @Test
    void compactConstructorHandlesNullPerOperationMap() {
        TriggerConditions conditions =
                new TriggerConditions(
                        null, null, null, null, null, null, null, null, null, null, 60, null, false,
                        null, null);

        assertNotNull(conditions.perOperationMinIntervalMinutes());
        assertTrue(conditions.perOperationMinIntervalMinutes().isEmpty());
        assertEquals(TriggerConditions.TriggerLogic.OR, conditions.triggerLogic());
    }

    @Test
    void compactConstructorHandlesNullCriticalPipeline() {
        TriggerConditions conditions =
                new TriggerConditions(
                        null, null, null, null, null, null, null, null, null, null, null, Map.of(),
                        null, null, null);

        assertFalse(conditions.criticalPipeline());
        assertEquals(TriggerConditions.TriggerLogic.OR, conditions.triggerLogic());
    }

    @Test
    void recordSupportsEquality() {
        TriggerConditions c1 =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(20.0)
                        .minIntervalMinutes(60)
                        .build();
        TriggerConditions c2 =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(20.0)
                        .minIntervalMinutes(60)
                        .build();

        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());
    }

    @Test
    void differentConditionsAreNotEqual() {
        TriggerConditions c1 = TriggerConditions.builder().smallFilePercentageAbove(20.0).build();
        TriggerConditions c2 = TriggerConditions.builder().smallFilePercentageAbove(30.0).build();

        assertNotEquals(c1, c2);
    }
}
