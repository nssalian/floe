package com.floe.server.api;

import com.floe.core.operation.OperationResults;
import java.util.List;

/**
 * Response for operation results.
 *
 * @param operations list of individual operations
 * @param successCount successful operations count
 * @param failedCount failed operations count
 * @param skippedCount skipped operations count
 * @param totalDurationMs total duration in milliseconds
 */
public record OperationResultsResponse(
        List<SingleOperationResponse> operations,
        int successCount,
        int failedCount,
        int skippedCount,
        long totalDurationMs) {
    public static OperationResultsResponse from(OperationResults results) {
        List<SingleOperationResponse> ops =
                results.operations().stream().map(SingleOperationResponse::from).toList();

        return new OperationResultsResponse(
                ops,
                (int) results.successCount(),
                (int) results.failedCount(),
                (int) results.skippedCount(),
                results.totalDurationMs());
    }
}
