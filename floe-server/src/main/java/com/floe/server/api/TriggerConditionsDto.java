package com.floe.server.api;

import com.floe.core.policy.OperationType;
import com.floe.core.policy.TriggerConditions;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DTO for trigger conditions in policy API requests.
 *
 * <p>All fields are optional - unset fields will be null (no condition).
 */
public record TriggerConditionsDto(
        // Data file conditions
        Double smallFilePercentageAbove,
        Long smallFileCountAbove,
        Long totalFileSizeAboveBytes,

        // Delete file conditions
        Integer deleteFileCountAbove,
        Double deleteFileRatioAbove,

        // Snapshot conditions
        Integer snapshotCountAbove,
        Integer snapshotAgeAboveDays,

        // Partition conditions
        Integer partitionCountAbove,
        Double partitionSkewAbove,

        // Manifest conditions
        Long manifestSizeAboveBytes,

        // Time-based gates
        Integer minIntervalMinutes,
        Map<String, Integer> perOperationMinIntervalMinutes,

        // Critical pipeline settings
        Boolean criticalPipeline,
        Integer criticalPipelineMaxDelayMinutes,
        String triggerLogic) {
    private static final Logger LOG = LoggerFactory.getLogger(TriggerConditionsDto.class);

    /**
     * Convert to domain TriggerConditions.
     *
     * @return TriggerConditions with values from this DTO
     */
    public TriggerConditions toConditions() {
        return new TriggerConditions(
                smallFilePercentageAbove,
                smallFileCountAbove,
                totalFileSizeAboveBytes,
                deleteFileCountAbove,
                deleteFileRatioAbove,
                snapshotCountAbove,
                snapshotAgeAboveDays,
                partitionCountAbove,
                partitionSkewAbove,
                manifestSizeAboveBytes,
                minIntervalMinutes,
                convertPerOperationIntervals(perOperationMinIntervalMinutes),
                criticalPipeline,
                criticalPipelineMaxDelayMinutes,
                parseTriggerLogic(triggerLogic));
    }

    private static TriggerConditions.TriggerLogic parseTriggerLogic(String logic) {
        if (logic == null || logic.isBlank()) {
            return TriggerConditions.TriggerLogic.OR;
        }
        try {
            return TriggerConditions.TriggerLogic.valueOf(logic.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            LOG.warn("Invalid triggerLogic '{}', defaulting to OR", logic);
            return TriggerConditions.TriggerLogic.OR;
        }
    }

    private static Map<OperationType, Integer> convertPerOperationIntervals(
            Map<String, Integer> intervals) {
        if (intervals == null || intervals.isEmpty()) {
            return Map.of();
        }
        var result = new java.util.HashMap<OperationType, Integer>();
        for (var entry : intervals.entrySet()) {
            try {
                OperationType opType =
                        OperationType.valueOf(entry.getKey().toUpperCase(Locale.ROOT));
                result.put(opType, entry.getValue());
            } catch (IllegalArgumentException e) {
                LOG.warn(
                        "Ignoring invalid operation type '{}' in perOperationMinIntervalMinutes",
                        entry.getKey());
            }
        }
        return Map.copyOf(result);
    }

    /**
     * Create a DTO from domain TriggerConditions.
     *
     * @param conditions the domain conditions
     * @return DTO representation
     */
    public static TriggerConditionsDto from(TriggerConditions conditions) {
        if (conditions == null) {
            return null;
        }
        return new TriggerConditionsDto(
                conditions.smallFilePercentageAbove(),
                conditions.smallFileCountAbove(),
                conditions.totalFileSizeAboveBytes(),
                conditions.deleteFileCountAbove(),
                conditions.deleteFileRatioAbove(),
                conditions.snapshotCountAbove(),
                conditions.snapshotAgeAboveDays(),
                conditions.partitionCountAbove(),
                conditions.partitionSkewAbove(),
                conditions.manifestSizeAboveBytes(),
                conditions.minIntervalMinutes(),
                convertFromOperationIntervals(conditions.perOperationMinIntervalMinutes()),
                conditions.criticalPipeline(),
                conditions.criticalPipelineMaxDelayMinutes(),
                conditions.triggerLogic() != null
                        ? conditions.triggerLogic().name()
                        : TriggerConditions.TriggerLogic.OR.name());
    }

    private static Map<String, Integer> convertFromOperationIntervals(
            Map<OperationType, Integer> intervals) {
        if (intervals == null || intervals.isEmpty()) {
            return Map.of();
        }
        var result = new java.util.HashMap<String, Integer>();
        for (var entry : intervals.entrySet()) {
            result.put(entry.getKey().name(), entry.getValue());
        }
        return Map.copyOf(result);
    }
}
