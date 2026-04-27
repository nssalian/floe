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

package com.floe.core.orchestrator;

import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Result of a single maintenance operation within an orchestration run.
 *
 * @param operationType type of operation
 * @param status execution status
 * @param startTime when operation started
 * @param endTime when operation ended
 * @param metrics operation metrics
 * @param errorMessage error message if failed
 */
public record OperationResult(
        MaintenanceOperation.Type operationType,
        ExecutionStatus status,
        Instant startTime,
        Instant endTime,
        Map<String, Object> metrics,
        String errorMessage) {
    public Duration duration() {
        if (endTime == null) {
            return Duration.between(startTime, Instant.now());
        }
        return Duration.between(startTime, endTime);
    }

    public boolean isSuccess() {
        return status == ExecutionStatus.SUCCEEDED;
    }

    public boolean isFailed() {
        return status == ExecutionStatus.FAILED;
    }

    public boolean isSkipped() {
        return status == ExecutionStatus.CANCELLED;
    }

    public static OperationResult skipped(MaintenanceOperation.Type type, String reason) {
        return new OperationResult(
                type, ExecutionStatus.CANCELLED, Instant.now(), Instant.now(), Map.of(), reason);
    }
}
