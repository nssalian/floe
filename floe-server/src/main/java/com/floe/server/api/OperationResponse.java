package com.floe.server.api;

import com.floe.core.operation.OperationRecord;

/** Response for operation details. */
public record OperationResponse(
        String id,
        String catalog,
        String namespace,
        String tableName,
        String qualifiedTableName,
        String policyName,
        String policyId,
        String status,
        String statusDescription,
        String startedAt,
        String completedAt,
        Long durationMs,
        String errorMessage,
        OperationResultsResponse results,
        String createdAt) {
    public static OperationResponse from(OperationRecord record) {
        return new OperationResponse(
                record.id().toString(),
                record.catalog(),
                record.namespace(),
                record.tableName(),
                record.qualifiedTableName(),
                record.policyName(),
                record.policyId() != null ? record.policyId().toString() : null,
                record.status().name(),
                record.status().getDescription(),
                record.startedAt().toString(),
                record.completedAt() != null ? record.completedAt().toString() : null,
                record.duration().toMillis(),
                record.errorMessage(),
                record.results() != null ? OperationResultsResponse.from(record.results()) : null,
                record.createdAt().toString());
    }
}
