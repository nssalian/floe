package com.floe.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.orchestrator.OperationResult;
import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.server.api.dto.OperationResultDto;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TriggerResponseTest {

    private final TableIdentifier table = TableIdentifier.of("iceberg", "db", "orders");
    private final Instant startTime = Instant.parse("2024-01-15T10:00:00Z");
    private final Instant endTime = Instant.parse("2024-01-15T10:05:00Z");

    @Test
    void shouldCreateWithAllFields() {
        TriggerResponse response =
                new TriggerResponse(
                        "orch-123",
                        "SUCCESS",
                        "iceberg.db.orders",
                        "daily-compact",
                        startTime,
                        endTime,
                        300000L,
                        List.of(),
                        "All operations completed successfully");

        assertEquals("orch-123", response.orchestrationId());
        assertEquals("SUCCESS", response.status());
        assertEquals("iceberg.db.orders", response.table());
        assertEquals("daily-compact", response.policyName());
        assertEquals(startTime, response.startTime());
        assertEquals(endTime, response.endTime());
        assertEquals(300000L, response.durationMs());
        assertTrue(response.operationResults().isEmpty());
        assertEquals("All operations completed successfully", response.message());
    }

    @Test
    void fromShouldConvertSuccessResult() {
        OperationResult opResult =
                new OperationResult(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        ExecutionStatus.SUCCEEDED,
                        startTime,
                        endTime,
                        Map.of("filesRewritten", 10),
                        null);

        OrchestratorResult result =
                new OrchestratorResult(
                        "orch-123",
                        table,
                        "daily-compact",
                        OrchestratorResult.Status.SUCCESS,
                        startTime,
                        endTime,
                        List.of(opResult),
                        "Orchestration completed");

        TriggerResponse response = TriggerResponse.from(result);

        assertEquals("orch-123", response.orchestrationId());
        assertEquals("SUCCESS", response.status());
        assertEquals("iceberg.db.orders", response.table());
        assertEquals("daily-compact", response.policyName());
        assertEquals(startTime, response.startTime());
        assertEquals(endTime, response.endTime());
        assertEquals(300000L, response.durationMs());
        assertEquals(1, response.operationResults().size());
        assertEquals("All operations completed successfully", response.message());
    }

    @Test
    void fromShouldConvertPartialFailureResult() {
        OrchestratorResult result =
                new OrchestratorResult(
                        "orch-123",
                        table,
                        "daily-compact",
                        OrchestratorResult.Status.PARTIAL_FAILURE,
                        startTime,
                        endTime,
                        List.of(),
                        "Some operations failed");

        TriggerResponse response = TriggerResponse.from(result);

        assertEquals("PARTIAL_FAILURE", response.status());
        assertEquals("Some operations failed", response.message());
    }

    @Test
    void fromShouldConvertFailedResult() {
        OrchestratorResult result =
                new OrchestratorResult(
                        "orch-123",
                        table,
                        "daily-compact",
                        OrchestratorResult.Status.FAILED,
                        startTime,
                        endTime,
                        List.of(),
                        "Maintenance failed");

        TriggerResponse response = TriggerResponse.from(result);

        assertEquals("FAILED", response.status());
        assertEquals("Maintenance failed", response.message());
    }

    @Test
    void fromShouldConvertNoPolicyResult() {
        OrchestratorResult result = OrchestratorResult.noPolicy("orch-123", table, startTime);

        TriggerResponse response = TriggerResponse.from(result);

        assertEquals("NO_POLICY", response.status());
        assertEquals("No matching policy found for table", response.message());
    }

    @Test
    void fromShouldConvertNoOperationsResult() {
        OrchestratorResult result =
                OrchestratorResult.noOperations("orch-123", table, "empty-policy", startTime);

        TriggerResponse response = TriggerResponse.from(result);

        assertEquals("NO_OPERATIONS", response.status());
        assertEquals("Policy has no enabled operations", response.message());
    }

    @Test
    void fromShouldConvertRunningResult() {
        OrchestratorResult result =
                OrchestratorResult.running(
                        "orch-123", table, "daily-compact", startTime, List.of());

        TriggerResponse response = TriggerResponse.from(result);

        assertEquals("RUNNING", response.status());
        assertEquals("Maintenance is running", response.message());
        assertNull(response.durationMs());
    }

    @Test
    void fromShouldHandleNullEndTime() {
        OrchestratorResult result =
                new OrchestratorResult(
                        "orch-123",
                        table,
                        "daily-compact",
                        OrchestratorResult.Status.RUNNING,
                        startTime,
                        null,
                        List.of(),
                        "Running");

        TriggerResponse response = TriggerResponse.from(result);

        assertNull(response.durationMs());
    }

    @Test
    void operationResultDtoShouldConvertFromOperationResult() {
        OperationResult opResult =
                new OperationResult(
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        ExecutionStatus.SUCCEEDED,
                        startTime,
                        endTime,
                        Map.of("snapshotsExpired", 5),
                        null);

        OperationResultDto dto = OperationResultDto.from(opResult);

        assertEquals("EXPIRE_SNAPSHOTS", dto.operationType());
        assertEquals("SUCCEEDED", dto.status());
        assertEquals(startTime, dto.startTime());
        assertEquals(endTime, dto.endTime());
        assertEquals(300000L, dto.durationMs());
        assertEquals(5, dto.metrics().get("snapshotsExpired"));
        assertNull(dto.errorMessage());
    }

    @Test
    void operationResultDtoShouldHandleFailedResult() {
        OperationResult opResult =
                new OperationResult(
                        MaintenanceOperation.Type.REWRITE_MANIFESTS,
                        ExecutionStatus.FAILED,
                        startTime,
                        endTime,
                        Map.of(),
                        "Something went wrong");

        OperationResultDto dto = OperationResultDto.from(opResult);

        assertEquals("REWRITE_MANIFESTS", dto.operationType());
        assertEquals("FAILED", dto.status());
        assertEquals("Something went wrong", dto.errorMessage());
    }

    @Test
    void operationResultDtoShouldHandleNullTimes() {
        OperationResult opResult =
                new OperationResult(
                        MaintenanceOperation.Type.ORPHAN_CLEANUP,
                        ExecutionStatus.PENDING,
                        null,
                        null,
                        Map.of(),
                        null);

        OperationResultDto dto = OperationResultDto.from(opResult);

        assertNull(dto.startTime());
        assertNull(dto.endTime());
        assertNull(dto.durationMs());
    }

    @Test
    void recordShouldSupportEquality() {
        TriggerResponse response1 =
                new TriggerResponse(
                        "orch-123",
                        "SUCCESS",
                        "iceberg.db.orders",
                        "policy",
                        startTime,
                        endTime,
                        300000L,
                        List.of(),
                        "message");
        TriggerResponse response2 =
                new TriggerResponse(
                        "orch-123",
                        "SUCCESS",
                        "iceberg.db.orders",
                        "policy",
                        startTime,
                        endTime,
                        300000L,
                        List.of(),
                        "message");

        assertEquals(response1, response2);
        assertEquals(response1.hashCode(), response2.hashCode());
    }
}
