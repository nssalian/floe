package com.floe.server.api;

import com.floe.core.operation.OperationResults;

/**
 * Response for a single operation.
 *
 * @param operationType type of operation
 * @param status operation status
 * @param durationMs duration in milliseconds
 * @param errorMessage error message if failed
 */
public record SingleOperationResponse(
        String operationType, String status, long durationMs, String errorMessage) {
    public static SingleOperationResponse from(OperationResults.SingleOperationResult result) {
        return new SingleOperationResponse(
                result.operationType().name(),
                result.status(),
                result.durationMs(),
                result.errorMessage());
    }
}
