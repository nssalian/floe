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

package com.floe.core.engine;

/** Status of a maintenance operation execution. */
public enum ExecutionStatus {
    PENDING("Waiting to start"),
    RUNNING("Currently executing"),
    SUCCEEDED("Finished successfully"),
    FAILED("Finished with error"),
    CANCELLED("Cancelled before completion"),
    SKIPPED("Skipped - not supported"),
    UNKNOWN("Unknown status");

    private final String description;

    ExecutionStatus(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    /** Check if this is a terminal state. */
    public boolean isTerminal() {
        return (this == SUCCEEDED || this == FAILED || this == CANCELLED || this == SKIPPED);
    }

    /** Check if execution was skipped. */
    public boolean isSkipped() {
        return this == SKIPPED;
    }

    /** Check if execution is still in progress. */
    public boolean isInProgress() {
        return this == PENDING || this == RUNNING;
    }

    /** Check if execution has failed. */
    public boolean isFailed() {
        return this == FAILED;
    }

    /** Check if execution has been cancelled. */
    public boolean isCancelled() {
        return this == CANCELLED;
    }

    /** Check if execution has completed successfully. */
    public boolean isSucceeded() {
        return this == SUCCEEDED;
    }
}
