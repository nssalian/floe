package com.floe.server.resource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.floe.core.operation.OperationRecord;
import com.floe.core.operation.OperationStats;
import com.floe.core.operation.OperationStatus;
import com.floe.core.operation.OperationStore;
import com.floe.server.api.ErrorResponse;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OperationsResourceTest {

    @Mock OperationStore operationStore;

    @InjectMocks OperationsResource resource;

    private OperationRecord testRecord1;
    private OperationRecord testRecord2;
    private OperationRecord testRecord3;

    @BeforeEach
    void setUp() {
        Instant now = Instant.now();
        testRecord1 =
                createRecord(
                        UUID.randomUUID(),
                        "catalog1",
                        "ns1",
                        "table1",
                        OperationStatus.SUCCESS,
                        now);
        testRecord2 =
                createRecord(
                        UUID.randomUUID(),
                        "catalog1",
                        "ns1",
                        "table2",
                        OperationStatus.FAILED,
                        now);
        testRecord3 =
                createRecord(
                        UUID.randomUUID(),
                        "catalog1",
                        "ns1",
                        "table3",
                        OperationStatus.RUNNING,
                        now);
    }

    @Test
    void shouldListRecentOperations() {
        when(operationStore.findRecent(20, 0)).thenReturn(List.of(testRecord1, testRecord2));
        when(operationStore.count()).thenReturn(2L);

        Response response = resource.listOperations(20, 0, null);

        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(2L, body.get("total"));
        assertEquals(false, body.get("hasMore"));

        @SuppressWarnings("unchecked")
        List<?> operations = (List<?>) body.get("operations");
        assertEquals(2, operations.size());
    }

    @Test
    void shouldListOperationsWithLimit() {
        when(operationStore.findRecent(3, 0))
                .thenReturn(List.of(testRecord1, testRecord2, testRecord3));
        when(operationStore.count()).thenReturn(5L);

        Response response = resource.listOperations(3, 0, null);

        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(5L, body.get("total"));
        assertEquals(3, body.get("limit"));
        assertEquals(0, body.get("offset"));
        assertEquals(true, body.get("hasMore"));
    }

    @Test
    void shouldListOperationsWithPagination() {
        when(operationStore.findRecent(2, 2)).thenReturn(List.of(testRecord1, testRecord2));
        when(operationStore.count()).thenReturn(5L);

        Response response = resource.listOperations(2, 2, null);

        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(5L, body.get("total"));
        assertEquals(2, body.get("limit"));
        assertEquals(2, body.get("offset"));
        assertEquals(true, body.get("hasMore"));
    }

    @Test
    void shouldGetRunningOperations() {
        when(operationStore.findByStatus(OperationStatus.RUNNING, 50, 0))
                .thenReturn(List.of(testRecord3));
        when(operationStore.countByStatus(OperationStatus.RUNNING)).thenReturn(1L);

        Response response = resource.getRunning(50, 0);

        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(1L, body.get("total"));
    }

    @Test
    void shouldGetOperationsForTable() {
        when(operationStore.findByTable("catalog1", "ns1", "events", 20))
                .thenReturn(List.of(testRecord1, testRecord2));

        Response response = resource.getByTable("catalog1", "ns1", "events", 20);

        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(2, body.get("total"));

        @SuppressWarnings("unchecked")
        Map<String, String> table = (Map<String, String>) body.get("table");
        assertEquals("catalog1.ns1.events", table.get("qualified"));
    }

    @Test
    void shouldFilterOperationsByStatus() {
        when(operationStore.findByStatus(OperationStatus.FAILED, 20, 0))
                .thenReturn(List.of(testRecord2));
        when(operationStore.countByStatus(OperationStatus.FAILED)).thenReturn(1L);

        Response response = resource.listOperations(20, 0, "FAILED");

        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(1L, body.get("total"));
    }

    @Test
    void shouldGetOperationStats() {
        OperationStats stats =
                OperationStats.builder()
                        .totalOperations(3)
                        .successCount(2)
                        .failedCount(1)
                        .partialFailureCount(0)
                        .runningCount(0)
                        .noPolicyCount(0)
                        .noOperationsCount(0)
                        .windowStart(Instant.now().minus(Duration.ofHours(24)))
                        .windowEnd(Instant.now())
                        .build();

        when(operationStore.getStats(Duration.ofHours(24))).thenReturn(stats);

        Response response = resource.getStats("24h");

        assertEquals(200, response.getStatus());
    }

    @Test
    void shouldGetOperationById() {
        UUID id = testRecord1.id();
        when(operationStore.findById(id)).thenReturn(Optional.of(testRecord1));

        Response response = resource.getById(id.toString());

        assertEquals(200, response.getStatus());
    }

    @Test
    void shouldReturn404ForNonExistentOperation() {
        UUID id = UUID.randomUUID();
        when(operationStore.findById(id)).thenReturn(Optional.empty());

        Response response = resource.getById(id.toString());

        assertEquals(404, response.getStatus());
    }

    @Test
    void shouldReturn400ForInvalidUUID() {
        Response response = resource.getById("not-a-uuid");

        assertEquals(400, response.getStatus());
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("Invalid UUID"));
    }

    @Test
    void shouldReturn400ForInvalidStatus() {
        Response response = resource.listOperations(20, 0, "INVALID_STATUS");

        assertEquals(400, response.getStatus());
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("Invalid status"));
    }

    @Test
    void shouldReturn400ForInvalidLimit() {
        Response response = resource.listOperations(200, 0, null);

        assertEquals(400, response.getStatus());
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("Limit must be between 1 and 100"));
    }

    @Test
    void shouldReturn400ForNegativeOffset() {
        Response response = resource.listOperations(20, -1, null);

        assertEquals(400, response.getStatus());
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("Offset must be non-negative"));
    }

    // Helper methods

    private OperationRecord createRecord(
            UUID id,
            String catalog,
            String namespace,
            String tableName,
            OperationStatus status,
            Instant now) {
        return new OperationRecord(
                id,
                catalog,
                namespace,
                tableName,
                null, // policyName
                null, // policyId
                null, // engineType
                null, // executionId
                null, // scheduleId
                null, // policyVersion
                status,
                now, // startedAt
                status.isTerminal() ? now : null, // completedAt
                null, // results
                null, // normalizedMetrics
                null, // errorMessage
                now // createdAt
                );
    }
}
