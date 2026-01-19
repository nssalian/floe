package com.floe.core.operation;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.orchestrator.OrchestratorResult;
import org.junit.jupiter.api.Test;

class OperationStatusTest {

    // Description Tests

    @Test
    void pendingShouldHaveDescription() {
        assertEquals("Waiting to start", OperationStatus.PENDING.getDescription());
    }

    @Test
    void runningShouldHaveDescription() {
        assertEquals("Currently executing", OperationStatus.RUNNING.getDescription());
    }

    @Test
    void successShouldHaveDescription() {
        assertEquals("Completed successfully", OperationStatus.SUCCESS.getDescription());
    }

    @Test
    void partialFailureShouldHaveDescription() {
        assertEquals("Partially completed", OperationStatus.PARTIAL_FAILURE.getDescription());
    }

    @Test
    void failedShouldHaveDescription() {
        assertEquals("Failed", OperationStatus.FAILED.getDescription());
    }

    @Test
    void noPolicyShouldHaveDescription() {
        assertEquals("No policy matched", OperationStatus.NO_POLICY.getDescription());
    }

    @Test
    void noOperationsShouldHaveDescription() {
        assertEquals("No operations enabled", OperationStatus.NO_OPERATIONS.getDescription());
    }

    // isTerminal Tests

    @Test
    void pendingShouldNotBeTerminal() {
        assertFalse(OperationStatus.PENDING.isTerminal());
    }

    @Test
    void runningShouldNotBeTerminal() {
        assertFalse(OperationStatus.RUNNING.isTerminal());
    }

    @Test
    void successShouldBeTerminal() {
        assertTrue(OperationStatus.SUCCESS.isTerminal());
    }

    @Test
    void partialFailureShouldBeTerminal() {
        assertTrue(OperationStatus.PARTIAL_FAILURE.isTerminal());
    }

    @Test
    void failedShouldBeTerminal() {
        assertTrue(OperationStatus.FAILED.isTerminal());
    }

    @Test
    void noPolicyShouldBeTerminal() {
        assertTrue(OperationStatus.NO_POLICY.isTerminal());
    }

    @Test
    void noOperationsShouldBeTerminal() {
        assertTrue(OperationStatus.NO_OPERATIONS.isTerminal());
    }

    // isInProgress Tests

    @Test
    void pendingShouldBeInProgress() {
        assertTrue(OperationStatus.PENDING.isInProgress());
    }

    @Test
    void runningShouldBeInProgress() {
        assertTrue(OperationStatus.RUNNING.isInProgress());
    }

    @Test
    void successShouldNotBeInProgress() {
        assertFalse(OperationStatus.SUCCESS.isInProgress());
    }

    @Test
    void failedShouldNotBeInProgress() {
        assertFalse(OperationStatus.FAILED.isInProgress());
    }

    @Test
    void partialFailureShouldNotBeInProgress() {
        assertFalse(OperationStatus.PARTIAL_FAILURE.isInProgress());
    }

    // isSuccess Tests

    @Test
    void successStatusShouldBeSuccess() {
        assertTrue(OperationStatus.SUCCESS.isSuccess());
    }

    @Test
    void otherStatusesShouldNotBeSuccess() {
        assertFalse(OperationStatus.PENDING.isSuccess());
        assertFalse(OperationStatus.RUNNING.isSuccess());
        assertFalse(OperationStatus.FAILED.isSuccess());
        assertFalse(OperationStatus.PARTIAL_FAILURE.isSuccess());
        assertFalse(OperationStatus.NO_POLICY.isSuccess());
        assertFalse(OperationStatus.NO_OPERATIONS.isSuccess());
    }

    // hasFailures Tests

    @Test
    void failedShouldHaveFailures() {
        assertTrue(OperationStatus.FAILED.hasFailures());
    }

    @Test
    void partialFailureShouldHaveFailures() {
        assertTrue(OperationStatus.PARTIAL_FAILURE.hasFailures());
    }

    @Test
    void successShouldNotHaveFailures() {
        assertFalse(OperationStatus.SUCCESS.hasFailures());
    }

    @Test
    void pendingShouldNotHaveFailures() {
        assertFalse(OperationStatus.PENDING.hasFailures());
    }

    @Test
    void runningShouldNotHaveFailures() {
        assertFalse(OperationStatus.RUNNING.hasFailures());
    }

    @Test
    void noPolicyShouldNotHaveFailures() {
        assertFalse(OperationStatus.NO_POLICY.hasFailures());
    }

    // fromOrchestratorStatus Tests

    @Test
    void shouldMapSuccessFromOrchestrator() {
        assertEquals(
                OperationStatus.SUCCESS,
                OperationStatus.fromOrchestratorStatus(OrchestratorResult.Status.SUCCESS));
    }

    @Test
    void shouldMapPartialFailureFromOrchestrator() {
        assertEquals(
                OperationStatus.PARTIAL_FAILURE,
                OperationStatus.fromOrchestratorStatus(OrchestratorResult.Status.PARTIAL_FAILURE));
    }

    @Test
    void shouldMapFailedFromOrchestrator() {
        assertEquals(
                OperationStatus.FAILED,
                OperationStatus.fromOrchestratorStatus(OrchestratorResult.Status.FAILED));
    }

    @Test
    void shouldMapNoPolicyFromOrchestrator() {
        assertEquals(
                OperationStatus.NO_POLICY,
                OperationStatus.fromOrchestratorStatus(OrchestratorResult.Status.NO_POLICY));
    }

    @Test
    void shouldMapNoOperationsFromOrchestrator() {
        assertEquals(
                OperationStatus.NO_OPERATIONS,
                OperationStatus.fromOrchestratorStatus(OrchestratorResult.Status.NO_OPERATIONS));
    }

    @Test
    void shouldMapRunningFromOrchestrator() {
        assertEquals(
                OperationStatus.RUNNING,
                OperationStatus.fromOrchestratorStatus(OrchestratorResult.Status.RUNNING));
    }

    // Enum Tests

    @Test
    void shouldHaveSevenStatuses() {
        assertEquals(7, OperationStatus.values().length);
    }

    @Test
    void shouldParseFromString() {
        assertEquals(OperationStatus.PENDING, OperationStatus.valueOf("PENDING"));
        assertEquals(OperationStatus.RUNNING, OperationStatus.valueOf("RUNNING"));
        assertEquals(OperationStatus.SUCCESS, OperationStatus.valueOf("SUCCESS"));
        assertEquals(OperationStatus.PARTIAL_FAILURE, OperationStatus.valueOf("PARTIAL_FAILURE"));
        assertEquals(OperationStatus.FAILED, OperationStatus.valueOf("FAILED"));
        assertEquals(OperationStatus.NO_POLICY, OperationStatus.valueOf("NO_POLICY"));
        assertEquals(OperationStatus.NO_OPERATIONS, OperationStatus.valueOf("NO_OPERATIONS"));
    }
}
