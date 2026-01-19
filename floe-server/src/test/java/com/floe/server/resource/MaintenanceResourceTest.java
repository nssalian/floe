package com.floe.server.resource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
import com.floe.core.orchestrator.OperationResult;
import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.server.api.TriggerRequest;
import com.floe.server.metrics.FloeMetrics;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MaintenanceResourceTest {

    @Mock MaintenanceOrchestrator orchestrator;

    @Mock FloeMetrics metrics;

    @InjectMocks MaintenanceResource resource;

    @Test
    void shouldReturn404WhenNoPolicyMatches() {
        TableIdentifier tableId = TableIdentifier.of("demo", "db", "table");
        OrchestratorResult result = OrchestratorResult.noPolicy("orch-123", tableId, Instant.now());

        when(orchestrator.runMaintenance(eq("demo"), any(TableIdentifier.class)))
                .thenReturn(result);

        TriggerRequest request = new TriggerRequest("demo", "db", "table", null);
        Response response = resource.trigger(request);

        assertEquals(404, response.getStatus());
    }

    @Test
    void shouldReturnNoOperationsWhenPolicyHasNoneEnabled() {
        TableIdentifier tableId = TableIdentifier.of("catalog", "db", "test_table");
        OrchestratorResult result =
                OrchestratorResult.noOperations("orch-123", tableId, "empty-policy", Instant.now());

        when(orchestrator.runMaintenance(eq("catalog"), any(TableIdentifier.class)))
                .thenReturn(result);

        TriggerRequest request = new TriggerRequest("catalog", "db", "test_table", null);
        Response response = resource.trigger(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void shouldTriggerMaintenanceWithMatchingPolicy() {
        TableIdentifier tableId = TableIdentifier.of("prod", "sales", "orders");
        List<OperationResult> opResults =
                List.of(
                        new OperationResult(
                                MaintenanceOperation.Type.REWRITE_DATA_FILES,
                                ExecutionStatus.SUCCEEDED,
                                Instant.now(),
                                Instant.now(),
                                Map.of(),
                                null));

        OrchestratorResult result =
                OrchestratorResult.completed(
                        "orch-123", tableId, "test-policy", Instant.now(), opResults);

        when(orchestrator.runMaintenance(eq("prod"), any(TableIdentifier.class)))
                .thenReturn(result);

        TriggerRequest request = new TriggerRequest("prod", "sales", "orders", null);
        Response response = resource.trigger(request);

        assertEquals(200, response.getStatus());
        verify(orchestrator).runMaintenance(eq("prod"), any(TableIdentifier.class));
    }

    @Test
    void shouldTriggerMaintenanceWithSpecificPolicy() {
        TableIdentifier tableId = TableIdentifier.of("prod", "sales", "orders");
        List<OperationResult> opResults =
                List.of(
                        new OperationResult(
                                MaintenanceOperation.Type.REWRITE_DATA_FILES,
                                ExecutionStatus.SUCCEEDED,
                                Instant.now(),
                                Instant.now(),
                                Map.of(),
                                null));

        OrchestratorResult result =
                OrchestratorResult.completed(
                        "orch-123", tableId, "specific-policy", Instant.now(), opResults);

        when(orchestrator.runMaintenanceWithPolicy(
                        eq("prod"), any(TableIdentifier.class), eq("specific-policy")))
                .thenReturn(result);

        TriggerRequest request = new TriggerRequest("prod", "sales", "orders", "specific-policy");
        Response response = resource.trigger(request);

        assertEquals(200, response.getStatus());
        verify(orchestrator)
                .runMaintenanceWithPolicy(
                        eq("prod"), any(TableIdentifier.class), eq("specific-policy"));
    }

    @Test
    void shouldReturn500WhenAllOperationsFail() {
        TableIdentifier tableId = TableIdentifier.of("prod", "sales", "orders");
        List<OperationResult> opResults =
                List.of(
                        new OperationResult(
                                MaintenanceOperation.Type.REWRITE_DATA_FILES,
                                ExecutionStatus.FAILED,
                                Instant.now(),
                                Instant.now(),
                                Map.of(),
                                "Spark not available"));

        OrchestratorResult result =
                OrchestratorResult.completed(
                        "orch-123", tableId, "test-policy", Instant.now(), opResults);

        when(orchestrator.runMaintenance(eq("prod"), any(TableIdentifier.class)))
                .thenReturn(result);

        TriggerRequest request = new TriggerRequest("prod", "sales", "orders", null);
        Response response = resource.trigger(request);

        assertEquals(500, response.getStatus());
    }

    @Test
    void shouldReturn200WithPartialFailure() {
        TableIdentifier tableId = TableIdentifier.of("prod", "sales", "orders");
        List<OperationResult> opResults =
                List.of(
                        new OperationResult(
                                MaintenanceOperation.Type.REWRITE_DATA_FILES,
                                ExecutionStatus.SUCCEEDED,
                                Instant.now(),
                                Instant.now(),
                                Map.of(),
                                null),
                        new OperationResult(
                                MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                                ExecutionStatus.FAILED,
                                Instant.now(),
                                Instant.now(),
                                Map.of(),
                                "Timeout"));

        OrchestratorResult result =
                OrchestratorResult.completed(
                        "orch-123", tableId, "test-policy", Instant.now(), opResults);

        when(orchestrator.runMaintenance(eq("prod"), any(TableIdentifier.class)))
                .thenReturn(result);

        TriggerRequest request = new TriggerRequest("prod", "sales", "orders", null);
        Response response = resource.trigger(request);

        // PARTIAL_FAILURE returns 200
        assertEquals(200, response.getStatus());
    }

    @Test
    void shouldReturn500WhenOrchestratorThrowsException() {
        when(orchestrator.runMaintenance(eq("prod"), any(TableIdentifier.class)))
                .thenThrow(new RuntimeException("Connection failed"));

        TriggerRequest request = new TriggerRequest("prod", "sales", "orders", null);
        Response response = resource.trigger(request);

        assertEquals(500, response.getStatus());
        MaintenanceResource.ErrorResponse body =
                (MaintenanceResource.ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("Connection failed"));
    }

    @Test
    void shouldReturn400ForInvalidArgument() {
        when(orchestrator.runMaintenance(eq("prod"), any(TableIdentifier.class)))
                .thenThrow(new IllegalArgumentException("Invalid table name"));

        TriggerRequest request = new TriggerRequest("prod", "sales", "orders", null);
        Response response = resource.trigger(request);

        assertEquals(400, response.getStatus());
        MaintenanceResource.ErrorResponse body =
                (MaintenanceResource.ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("Invalid table name"));
    }

    @Test
    void shouldReturn202WhenOperationIsRunning() {
        TableIdentifier tableId = TableIdentifier.of("prod", "sales", "orders");
        OrchestratorResult result =
                OrchestratorResult.running(
                        "orch-123", tableId, "test-policy", Instant.now(), List.of());

        when(orchestrator.runMaintenance(eq("prod"), any(TableIdentifier.class)))
                .thenReturn(result);

        TriggerRequest request = new TriggerRequest("prod", "sales", "orders", null);
        Response response = resource.trigger(request);

        assertEquals(202, response.getStatus());
    }
}
