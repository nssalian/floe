package com.floe.server.api;

import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.server.api.dto.OperationResultDto;
import java.time.Instant;
import java.util.List;

/**
 * Response from policy-driven maintenance trigger.
 *
 * @param orchestrationId unique orchestration ID
 * @param status overall status
 * @param table target table
 * @param policyName policy that was applied
 * @param startTime when maintenance started
 * @param endTime when maintenance ended
 * @param durationMs duration in milliseconds
 * @param operationResults individual operation results
 * @param message human-readable status message
 */
public record TriggerResponse(
        String orchestrationId,
        String status,
        String table,
        String policyName,
        Instant startTime,
        Instant endTime,
        Long durationMs,
        List<OperationResultDto> operationResults,
        String message) {
    public static TriggerResponse from(OrchestratorResult result) {
        String message =
                switch (result.status()) {
                    case SUCCESS -> "All operations completed successfully";
                    case PARTIAL_FAILURE -> "Some operations failed";
                    case FAILED -> "Maintenance failed";
                    case NO_POLICY -> "No matching policy found for table";
                    case NO_OPERATIONS -> "Policy has no enabled operations";
                    case RUNNING -> "Maintenance is running";
                };

        List<OperationResultDto> operationResults =
                result.operationResults().stream().map(OperationResultDto::from).toList();

        String fullTableName =
                result.table().getCatalog()
                        + "."
                        + result.table().getNamespace()
                        + "."
                        + result.table().getTableName();
        if (result.startTime() != null && result.endTime() != null) {
            Long durationMs = result.endTime().toEpochMilli() - result.startTime().toEpochMilli();
            return new TriggerResponse(
                    result.orchestrationId(),
                    result.status().name(),
                    fullTableName,
                    result.policyName(),
                    result.startTime(),
                    result.endTime(),
                    durationMs,
                    operationResults,
                    message);
        } else {
            return new TriggerResponse(
                    result.orchestrationId(),
                    result.status().name(),
                    fullTableName,
                    result.policyName(),
                    result.startTime(),
                    result.endTime(),
                    null,
                    operationResults,
                    message);
        }
    }
}
