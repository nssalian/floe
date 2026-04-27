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

package com.floe.core.operation;

import com.floe.core.orchestrator.OrchestratorResult;

/** Status of a persisted maintenance operation run. */
public enum OperationStatus {
    /** Operation is queued but not yet started */
    PENDING("Waiting to start"),

    /** Operation is currently executing */
    RUNNING("Currently executing"),

    /** All operations completed successfully */
    SUCCESS("Completed successfully"),

    /** Some operations succeeded, some failed */
    PARTIAL_FAILURE("Partially completed"),

    /** All operations failed */
    FAILED("Failed"),

    /** No matching policy found for the table */
    NO_POLICY("No policy matched"),

    /** Policy matched but no operations were enabled */
    NO_OPERATIONS("No operations enabled");

    private final String description;

    OperationStatus(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    /** Check if this is a terminal state. */
    public boolean isTerminal() {
        return this == SUCCESS
                || this == PARTIAL_FAILURE
                || this == FAILED
                || this == NO_POLICY
                || this == NO_OPERATIONS;
    }

    /** Check if operation is still in progress. */
    public boolean isInProgress() {
        return this == PENDING || this == RUNNING;
    }

    /** Check if operation completed successfully. */
    public boolean isSuccess() {
        return this == SUCCESS;
    }

    /** Check if operation had any failures. */
    public boolean hasFailures() {
        return this == FAILED || this == PARTIAL_FAILURE;
    }

    /** Convert from OrchestratorResult.Status to OperationStatus. */
    public static OperationStatus fromOrchestratorStatus(OrchestratorResult.Status status) {
        return switch (status) {
            case SUCCESS -> SUCCESS;
            case PARTIAL_FAILURE -> PARTIAL_FAILURE;
            case FAILED -> FAILED;
            case NO_POLICY -> NO_POLICY;
            case NO_OPERATIONS -> NO_OPERATIONS;
            case RUNNING -> RUNNING;
        };
    }
}
