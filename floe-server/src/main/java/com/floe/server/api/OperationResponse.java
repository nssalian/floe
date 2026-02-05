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
        String engineType,
        String executionId,
        String scheduleId,
        String policyVersion,
        String status,
        String statusDescription,
        String startedAt,
        String completedAt,
        Long durationMs,
        String errorMessage,
        OperationResultsResponse results,
        java.util.Map<String, Object> normalizedMetrics,
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
                record.engineType(),
                record.executionId(),
                record.scheduleId(),
                record.policyVersion(),
                record.status().name(),
                record.status().getDescription(),
                record.startedAt().toString(),
                record.completedAt() != null ? record.completedAt().toString() : null,
                record.duration().toMillis(),
                record.errorMessage(),
                record.results() != null ? OperationResultsResponse.from(record.results()) : null,
                record.normalizedMetrics(),
                record.createdAt().toString());
    }
}
