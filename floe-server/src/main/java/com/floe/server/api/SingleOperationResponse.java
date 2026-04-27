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

/**
 * Response for a single operation.
 *
 * @param operationType type of operation
 * @param status operation status
 * @param durationMs duration in milliseconds
 * @param metrics operation metrics
 * @param errorMessage error message if failed
 */
public record SingleOperationResponse(
        String operationType,
        String status,
        long durationMs,
        java.util.Map<String, Object> metrics,
        String errorMessage) {
    public static SingleOperationResponse from(OperationResults.SingleOperationResult result) {
        return new SingleOperationResponse(
                result.operationType().name(),
                result.status(),
                result.durationMs(),
                result.metrics(),
                result.errorMessage());
    }
}
