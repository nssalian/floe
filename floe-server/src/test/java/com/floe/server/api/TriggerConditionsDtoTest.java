package com.floe.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.policy.OperationType;
import com.floe.core.policy.TriggerConditions;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TriggerConditionsDtoTest {

    @Test
    void toConditionsConvertsAllFields() {
        TriggerConditionsDto dto =
                new TriggerConditionsDto(
                        25.0, // smallFilePercentageAbove
                        200L, // smallFileCountAbove
                        10L * 1024 * 1024 * 1024, // totalFileSizeAboveBytes
                        100, // deleteFileCountAbove
                        0.15, // deleteFileRatioAbove
                        75, // snapshotCountAbove
                        14, // snapshotAgeAboveDays
                        1000, // partitionCountAbove
                        5.0, // partitionSkewAbove
                        200 * 1024 * 1024L, // manifestSizeAboveBytes
                        45, // minIntervalMinutes
                        Map.of("REWRITE_DATA_FILES", 30, "EXPIRE_SNAPSHOTS", 60),
                        true, // criticalPipeline
                        180, // criticalPipelineMaxDelayMinutes
                        "AND" // triggerLogic
                        );

        TriggerConditions conditions = dto.toConditions();

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
        assertEquals(TriggerConditions.TriggerLogic.AND, conditions.triggerLogic());

        // Check per-operation intervals were converted
        assertEquals(30, conditions.getMinIntervalMinutes(OperationType.REWRITE_DATA_FILES));
        assertEquals(60, conditions.getMinIntervalMinutes(OperationType.EXPIRE_SNAPSHOTS));
    }

    @Test
    void toConditionsHandlesNullValues() {
        TriggerConditionsDto dto =
                new TriggerConditionsDto(
                        null, null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null);

        TriggerConditions conditions = dto.toConditions();

        assertNull(conditions.smallFilePercentageAbove());
        assertNull(conditions.minIntervalMinutes());
        assertFalse(conditions.criticalPipeline());
        assertTrue(conditions.perOperationMinIntervalMinutes().isEmpty());
        assertEquals(TriggerConditions.TriggerLogic.OR, conditions.triggerLogic());
    }

    @Test
    void toConditionsConvertsOperationTypesCaseInsensitive() {
        TriggerConditionsDto dto =
                new TriggerConditionsDto(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        60,
                        Map.of("rewrite_data_files", 30, "EXPIRE_SNAPSHOTS", 45),
                        false,
                        null,
                        null);

        TriggerConditions conditions = dto.toConditions();

        assertEquals(30, conditions.getMinIntervalMinutes(OperationType.REWRITE_DATA_FILES));
        assertEquals(45, conditions.getMinIntervalMinutes(OperationType.EXPIRE_SNAPSHOTS));
    }

    @Test
    void toConditionsSkipsInvalidOperationTypes() {
        TriggerConditionsDto dto =
                new TriggerConditionsDto(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        60,
                        Map.of("INVALID_OP", 30, "REWRITE_DATA_FILES", 45),
                        false,
                        null,
                        null);

        TriggerConditions conditions = dto.toConditions();

        // Only valid operation type should be present
        assertEquals(1, conditions.perOperationMinIntervalMinutes().size());
        assertEquals(45, conditions.getMinIntervalMinutes(OperationType.REWRITE_DATA_FILES));
    }

    @Test
    void fromConvertsConditionsToDto() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(20.0)
                        .smallFileCountAbove(100L)
                        .deleteFileCountAbove(50)
                        .deleteFileRatioAbove(0.10)
                        .snapshotCountAbove(50)
                        .snapshotAgeAboveDays(7)
                        .manifestSizeAboveBytes(100 * 1024 * 1024L)
                        .minIntervalMinutes(60)
                        .perOperationMinIntervalMinutes(
                                Map.of(OperationType.REWRITE_DATA_FILES, 30))
                        .criticalPipeline(true)
                        .criticalPipelineMaxDelayMinutes(120)
                        .build();

        TriggerConditionsDto dto = TriggerConditionsDto.from(conditions);

        assertEquals(20.0, dto.smallFilePercentageAbove());
        assertEquals(100L, dto.smallFileCountAbove());
        assertEquals(50, dto.deleteFileCountAbove());
        assertEquals(0.10, dto.deleteFileRatioAbove());
        assertEquals(50, dto.snapshotCountAbove());
        assertEquals(7, dto.snapshotAgeAboveDays());
        assertEquals(100 * 1024 * 1024L, dto.manifestSizeAboveBytes());
        assertEquals(60, dto.minIntervalMinutes());
        assertTrue(dto.criticalPipeline());
        assertEquals(120, dto.criticalPipelineMaxDelayMinutes());
        assertEquals(30, dto.perOperationMinIntervalMinutes().get("REWRITE_DATA_FILES"));
        assertEquals("OR", dto.triggerLogic());
    }

    @Test
    void fromReturnsNullForNullInput() {
        TriggerConditionsDto dto = TriggerConditionsDto.from(null);

        assertNull(dto);
    }

    @Test
    void roundTripConversion() {
        TriggerConditionsDto originalDto =
                new TriggerConditionsDto(
                        25.0,
                        200L,
                        null,
                        100,
                        0.15,
                        75,
                        14,
                        null,
                        null,
                        200 * 1024 * 1024L,
                        45,
                        Map.of("REWRITE_DATA_FILES", 30),
                        true,
                        180,
                        "OR");

        TriggerConditions conditions = originalDto.toConditions();
        TriggerConditionsDto convertedDto = TriggerConditionsDto.from(conditions);

        assertEquals(
                originalDto.smallFilePercentageAbove(), convertedDto.smallFilePercentageAbove());
        assertEquals(originalDto.smallFileCountAbove(), convertedDto.smallFileCountAbove());
        assertEquals(originalDto.deleteFileCountAbove(), convertedDto.deleteFileCountAbove());
        assertEquals(originalDto.deleteFileRatioAbove(), convertedDto.deleteFileRatioAbove());
        assertEquals(originalDto.snapshotCountAbove(), convertedDto.snapshotCountAbove());
        assertEquals(originalDto.snapshotAgeAboveDays(), convertedDto.snapshotAgeAboveDays());
        assertEquals(originalDto.manifestSizeAboveBytes(), convertedDto.manifestSizeAboveBytes());
        assertEquals(originalDto.minIntervalMinutes(), convertedDto.minIntervalMinutes());
        assertEquals(originalDto.criticalPipeline(), convertedDto.criticalPipeline());
        assertEquals(
                originalDto.criticalPipelineMaxDelayMinutes(),
                convertedDto.criticalPipelineMaxDelayMinutes());
        assertEquals(originalDto.triggerLogic(), convertedDto.triggerLogic());
    }

    @Test
    void recordSupportsEquality() {
        TriggerConditionsDto dto1 =
                new TriggerConditionsDto(
                        20.0, null, null, null, null, null, null, null, null, null, 60, null, false,
                        null, "OR");
        TriggerConditionsDto dto2 =
                new TriggerConditionsDto(
                        20.0, null, null, null, null, null, null, null, null, null, 60, null, false,
                        null, "OR");

        assertEquals(dto1, dto2);
        assertEquals(dto1.hashCode(), dto2.hashCode());
    }
}
