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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExecutionStatusTest {

    @Test
    @DisplayName("should have expected number of statuses")
    void shouldHaveExpectedNumberOfStatuses() {
        assertEquals(7, ExecutionStatus.values().length);
    }

    @Test
    @DisplayName("should have correct descriptions")
    void shouldHaveCorrectDescriptions() {
        assertEquals("Waiting to start", ExecutionStatus.PENDING.getDescription());
        assertEquals("Currently executing", ExecutionStatus.RUNNING.getDescription());
        assertEquals("Finished successfully", ExecutionStatus.SUCCEEDED.getDescription());
        assertEquals("Finished with error", ExecutionStatus.FAILED.getDescription());
        assertEquals("Cancelled before completion", ExecutionStatus.CANCELLED.getDescription());
        assertEquals("Skipped - not supported", ExecutionStatus.SKIPPED.getDescription());
        assertEquals("Unknown status", ExecutionStatus.UNKNOWN.getDescription());
    }

    @Test
    @DisplayName("should identify terminal states correctly")
    void isTerminalShouldReturnTrueForTerminalStates() {
        assertTrue(ExecutionStatus.SUCCEEDED.isTerminal());
        assertTrue(ExecutionStatus.FAILED.isTerminal());
        assertTrue(ExecutionStatus.CANCELLED.isTerminal());
        assertTrue(ExecutionStatus.SKIPPED.isTerminal());
    }

    @Test
    @DisplayName("should identify non-terminal states correctly")
    void isTerminalShouldReturnFalseForNonTerminalStates() {
        assertFalse(ExecutionStatus.PENDING.isTerminal());
        assertFalse(ExecutionStatus.RUNNING.isTerminal());
        assertFalse(ExecutionStatus.UNKNOWN.isTerminal());
    }

    @Test
    @DisplayName("should identify skipped status correctly")
    void isSkippedShouldOnlyReturnTrueForSkipped() {
        assertTrue(ExecutionStatus.SKIPPED.isSkipped());
        assertFalse(ExecutionStatus.PENDING.isSkipped());
        assertFalse(ExecutionStatus.RUNNING.isSkipped());
        assertFalse(ExecutionStatus.SUCCEEDED.isSkipped());
        assertFalse(ExecutionStatus.FAILED.isSkipped());
        assertFalse(ExecutionStatus.CANCELLED.isSkipped());
        assertFalse(ExecutionStatus.UNKNOWN.isSkipped());
    }

    @Test
    @DisplayName("should identify in-progress states correctly")
    void isInProgressShouldReturnTrueForActiveStates() {
        assertTrue(ExecutionStatus.PENDING.isInProgress());
        assertTrue(ExecutionStatus.RUNNING.isInProgress());
    }

    @Test
    @DisplayName("should identify inactive states correctly")
    void isInProgressShouldReturnFalseForInactiveStates() {
        assertFalse(ExecutionStatus.SUCCEEDED.isInProgress());
        assertFalse(ExecutionStatus.FAILED.isInProgress());
        assertFalse(ExecutionStatus.CANCELLED.isInProgress());
        assertFalse(ExecutionStatus.UNKNOWN.isInProgress());
    }

    @Test
    @DisplayName("should identify failed status correctly")
    void isFailedShouldOnlyReturnTrueForFailed() {
        assertTrue(ExecutionStatus.FAILED.isFailed());
        assertFalse(ExecutionStatus.PENDING.isFailed());
        assertFalse(ExecutionStatus.RUNNING.isFailed());
        assertFalse(ExecutionStatus.SUCCEEDED.isFailed());
        assertFalse(ExecutionStatus.CANCELLED.isFailed());
        assertFalse(ExecutionStatus.UNKNOWN.isFailed());
    }

    @Test
    @DisplayName("should identify cancelled status correctly")
    void isCancelledShouldOnlyReturnTrueForCancelled() {
        assertTrue(ExecutionStatus.CANCELLED.isCancelled());
        assertFalse(ExecutionStatus.PENDING.isCancelled());
        assertFalse(ExecutionStatus.RUNNING.isCancelled());
        assertFalse(ExecutionStatus.SUCCEEDED.isCancelled());
        assertFalse(ExecutionStatus.FAILED.isCancelled());
        assertFalse(ExecutionStatus.UNKNOWN.isCancelled());
    }

    @Test
    @DisplayName("should identify completed status correctly")
    void isCompletedShouldOnlyReturnTrueForCompleted() {
        assertTrue(ExecutionStatus.SUCCEEDED.isSucceeded());
        assertFalse(ExecutionStatus.PENDING.isSucceeded());
        assertFalse(ExecutionStatus.RUNNING.isSucceeded());
        assertFalse(ExecutionStatus.FAILED.isSucceeded());
        assertFalse(ExecutionStatus.CANCELLED.isSucceeded());
        assertFalse(ExecutionStatus.UNKNOWN.isSucceeded());
    }

    @Test
    @DisplayName("should convert from string correctly")
    void shouldConvertFromString() {
        assertEquals(ExecutionStatus.PENDING, ExecutionStatus.valueOf("PENDING"));
        assertEquals(ExecutionStatus.RUNNING, ExecutionStatus.valueOf("RUNNING"));
        assertEquals(ExecutionStatus.SUCCEEDED, ExecutionStatus.valueOf("SUCCEEDED"));
        assertEquals(ExecutionStatus.FAILED, ExecutionStatus.valueOf("FAILED"));
        assertEquals(ExecutionStatus.CANCELLED, ExecutionStatus.valueOf("CANCELLED"));
        assertEquals(ExecutionStatus.UNKNOWN, ExecutionStatus.valueOf("UNKNOWN"));
    }

    @Test
    @DisplayName("should throw exception for invalid value")
    void shouldThrowExceptionForInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> ExecutionStatus.valueOf("INVALID"));
    }
}
