package com.floe.server.api.dto;

import java.time.Instant;
import java.util.Map;

public record OperationResultDto(
        String operationType,
        String status,
        Instant startTime,
        Instant endTime,
        Long durationMs,
        Map<String, Object> metrics,
        String errorMessage) {
    public static OperationResultDto from(com.floe.core.orchestrator.OperationResult result) {
        Long durationMs = null;
        if (result.startTime() != null && result.endTime() != null) {
            durationMs = result.endTime().toEpochMilli() - result.startTime().toEpochMilli();
        }
        return new OperationResultDto(
                result.operationType().name(),
                result.status().name(),
                result.startTime(),
                result.endTime(),
                durationMs,
                result.metrics(),
                result.errorMessage());
    }
}
