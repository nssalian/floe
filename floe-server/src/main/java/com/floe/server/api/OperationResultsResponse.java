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
