package com.floe.server.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.catalog.TableMetadata;
import com.floe.server.api.ErrorResponse;
import com.floe.server.api.HealthReportResponse;
import com.floe.server.api.TableDetailResponse;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings({
    "deprecation",
    "unchecked"
}) // Iceberg deprecated methods + generic casts in tests
class TablesResourceTest {

    @Mock CatalogClient catalogClient;

    @InjectMocks TablesResource resource;

    private TableIdentifier table1;
    private TableIdentifier table2;
    private TableMetadata tableMetadata;

    @BeforeEach
    void setUp() {
        table1 = TableIdentifier.of("demo", "test", "events");
        table2 = TableIdentifier.of("demo", "test", "users");

        tableMetadata =
                TableMetadata.builder()
                        .identifier(table1)
                        .location("s3://warehouse/test/events")
                        .snapshotCount(5)
                        .currentSnapshotId(123456789L)
                        .currentSnapshotTimestamp(Instant.now())
                        .oldestSnapshotTimestamp(Instant.now().minusSeconds(86400))
                        .dataFileCount(100)
                        .totalDataFileSizeBytes(1024 * 1024 * 500L) // 500MB
                        .deleteFileCount(5)
                        .positionDeleteFileCount(3)
                        .equalityDeleteFileCount(2)
                        .totalRecordCount(1000000L)
                        .manifestCount(10)
                        .totalManifestSizeBytes(1024 * 1024L) // 1MB
                        .formatVersion(2)
                        .partitionSpec("date (day)")
                        .sortOrder("timestamp desc nulls_last")
                        .properties(
                                Map.of("format-version", "2", "write.format.default", "parquet"))
                        .lastModified(Instant.now())
                        .build();
    }

    // List namespaces tests

    @Test
    void shouldListNamespaces() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listNamespaces()).thenReturn(List.of("test", "prod", "staging"));

        Response response = resource.listNamespaces();

        assertEquals(200, response.getStatus());

        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals("demo", body.get("catalog"));
        assertEquals(3, body.get("total"));

        List<String> namespaces = (List<String>) body.get("namespaces");
        assertEquals(3, namespaces.size());
        assertTrue(namespaces.contains("test"));
    }

    @Test
    void shouldReturnEmptyNamespaceList() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listNamespaces()).thenReturn(List.of());

        Response response = resource.listNamespaces();

        assertEquals(200, response.getStatus());

        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(0, body.get("total"));
    }

    @Test
    void shouldReturn500WhenListNamespacesFails() {
        when(catalogClient.listNamespaces()).thenThrow(new RuntimeException("Connection failed"));

        Response response = resource.listNamespaces();

        assertEquals(500, response.getStatus());
    }

    // List tables in namespace tests

    @Test
    void shouldListTablesInNamespace() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listTables("test")).thenReturn(List.of(table1, table2));

        Response response = resource.listTablesInNamespace("test", 20, 0);

        assertEquals(200, response.getStatus());

        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals("demo", body.get("catalog"));
        assertEquals("test", body.get("namespace"));
        assertEquals(2, body.get("total"));
        assertEquals(false, body.get("hasMore"));
    }

    @Test
    void shouldPaginateTablesInNamespace() {
        List<TableIdentifier> tables =
                List.of(
                        TableIdentifier.of("demo", "test", "t1"),
                        TableIdentifier.of("demo", "test", "t2"),
                        TableIdentifier.of("demo", "test", "t3"),
                        TableIdentifier.of("demo", "test", "t4"),
                        TableIdentifier.of("demo", "test", "t5"));

        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listTables("test")).thenReturn(tables);

        Response response = resource.listTablesInNamespace("test", 2, 0);

        assertEquals(200, response.getStatus());

        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(5, body.get("total"));
        assertEquals(2, body.get("limit"));
        assertEquals(0, body.get("offset"));
        assertEquals(true, body.get("hasMore"));

        List<?> returnedTables = (List<?>) body.get("tables");
        assertEquals(2, returnedTables.size());
    }

    @Test
    void shouldReturn400ForInvalidLimitInNamespace() {
        Response response = resource.listTablesInNamespace("test", 200, 0);

        assertEquals(400, response.getStatus());

        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertThat(body.error()).isEqualTo("Limit must be between 1 and 100");
    }

    @Test
    void shouldReturn400ForNegativeOffsetInNamespace() {
        Response response = resource.listTablesInNamespace("test", 20, -1);

        assertEquals(400, response.getStatus());
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertThat(body.error()).isEqualTo("Offset must be non-negative");
    }

    // List all tables tests

    @Test
    void shouldListAllTables() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables()).thenReturn(List.of(table1, table2));

        Response response = resource.listAllTables(20, 0);

        assertEquals(200, response.getStatus());

        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals("demo", body.get("catalog"));
        assertEquals(2, body.get("total"));
        assertEquals(false, body.get("hasMore"));
    }

    @Test
    void shouldPaginateAllTables() {
        List<TableIdentifier> tables =
                List.of(
                        TableIdentifier.of("demo", "ns1", "t1"),
                        TableIdentifier.of("demo", "ns1", "t2"),
                        TableIdentifier.of("demo", "ns2", "t3"),
                        TableIdentifier.of("demo", "ns2", "t4"),
                        TableIdentifier.of("demo", "ns3", "t5"));

        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables()).thenReturn(tables);

        Response response = resource.listAllTables(2, 2);

        assertEquals(200, response.getStatus());

        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(5, body.get("total"));
        assertEquals(2, body.get("limit"));
        assertEquals(2, body.get("offset"));
        assertEquals(true, body.get("hasMore"));
    }

    @Test
    void shouldReturn400ForInvalidLimit() {
        Response response = resource.listAllTables(0, 0);

        assertEquals(400, response.getStatus());
    }

    @Test
    void shouldReturn400ForNegativeOffset() {
        Response response = resource.listAllTables(20, -5);

        assertEquals(400, response.getStatus());
    }

    // Get table tests

    @Test
    void shouldGetTable() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.getTableMetadata(any(TableIdentifier.class)))
                .thenReturn(Optional.of(tableMetadata));

        Response response = resource.getTable("test", "events");

        assertEquals(200, response.getStatus());
        TableDetailResponse body = (TableDetailResponse) response.getEntity();
        assertEquals("demo", body.catalog());
        assertEquals("test", body.namespace());
        assertEquals("events", body.name());
        assertEquals(5, body.snapshotCount());
        assertEquals(100, body.dataFileCount());
    }

    @Test
    void shouldReturn404WhenTableNotFound() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.getTableMetadata(any(TableIdentifier.class)))
                .thenReturn(Optional.empty());

        Response response = resource.getTable("test", "nonexistent");

        assertEquals(404, response.getStatus());

        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertThat(body.error()).isEqualTo("Table not found: test.nonexistent");
    }

    @Test
    void shouldReturn500WhenGetTableFails() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.getTableMetadata(any(TableIdentifier.class)))
                .thenThrow(new RuntimeException("Connection failed"));

        Response response = resource.getTable("test", "events");

        assertEquals(500, response.getStatus());
    }

    // Table exists (HEAD) tests

    @Test
    void shouldReturn200WhenTableExists() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.getTableMetadata(any(TableIdentifier.class)))
                .thenReturn(Optional.of(tableMetadata));

        Response response = resource.tableExists("test", "events");

        assertEquals(200, response.getStatus());
    }

    @Test
    void shouldReturn404WhenTableDoesNotExist() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.getTableMetadata(any(TableIdentifier.class)))
                .thenReturn(Optional.empty());

        Response response = resource.tableExists("test", "nonexistent");

        assertEquals(404, response.getStatus());
    }

    // Get table health tests

    @Test
    void shouldReturnHealthReport() {
        when(catalogClient.getCatalogName()).thenReturn("demo");

        // Create a mock Table - empty table with no snapshots
        org.apache.iceberg.Table mockTable =
                mock(org.apache.iceberg.Table.class, withSettings().lenient());

        when(mockTable.currentSnapshot()).thenReturn(null);
        when(mockTable.snapshots()).thenReturn(List.of());

        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mockTable));

        Response response = resource.getTableHealth("test", "events");

        assertEquals(200, response.getStatus());
        HealthReportResponse body = (HealthReportResponse) response.getEntity();
        assertNotNull(body);
        assertEquals("demo.test.events", body.qualifiedName());
        assertNotNull(body.assessedAt());
        assertNotNull(body.healthStatus());
    }

    @Test
    void shouldReturn404WhenTableNotFoundForHealth() {
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.loadTable(any(TableIdentifier.class))).thenReturn(Optional.empty());

        Response response = resource.getTableHealth("test", "nonexistent");

        assertEquals(404, response.getStatus());

        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertThat(body.error()).isEqualTo("Table not found: test.nonexistent");
    }
}
