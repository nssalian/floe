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

package com.floe.core.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("AbstractIcebergCatalogClient")
class AbstractIcebergCatalogClientTest {

    private static final String CATALOG_NAME = "test-catalog";

    @Mock private Catalog mockCatalog;

    @Mock private NamespaceCatalog mockNamespacesCatalog;

    private TestCatalogClient client;
    private TestCatalogClient clientWithNamespaces;

    @BeforeEach
    void setUp() {
        client = new TestCatalogClient(CATALOG_NAME, mockCatalog);
        clientWithNamespaces = new TestCatalogClient(CATALOG_NAME, mockNamespacesCatalog);
    }

    @Nested
    @DisplayName("getCatalogName()")
    class GetCatalogName {

        @Test
        @DisplayName("should return the catalog name")
        void shouldReturnCatalogName() {
            assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
        }
    }

    @Nested
    @DisplayName("isHealthy()")
    class IsHealthy {

        @Test
        @DisplayName("should return true when catalog supports namespaces and responds")
        void shouldReturnTrueWhenHealthy() {
            when(mockNamespacesCatalog.listNamespaces()).thenReturn(List.of(Namespace.of("db1")));

            assertThat(clientWithNamespaces.isHealthy()).isTrue();
            verify(mockNamespacesCatalog).listNamespaces();
        }

        @Test
        @DisplayName("should return false when catalog throws exception")
        void shouldReturnFalseWhenUnhealthy() {
            when(mockNamespacesCatalog.listNamespaces())
                    .thenThrow(new RuntimeException("Connection failed"));

            assertThat(clientWithNamespaces.isHealthy()).isFalse();
        }

        @Test
        @DisplayName("should use listTables fallback when catalog doesn't support namespaces")
        void shouldUseFallbackForNonNamespaceCatalog() {
            when(mockCatalog.listTables(Namespace.empty())).thenReturn(Collections.emptyList());

            assertThat(client.isHealthy()).isTrue();
            verify(mockCatalog).listTables(Namespace.empty());
        }
    }

    @Nested
    @DisplayName("listNamespaces()")
    class ListNamespaces {

        @Test
        @DisplayName("should return namespace names when catalog supports namespaces")
        void shouldReturnNamespaces() {
            when(mockNamespacesCatalog.listNamespaces())
                    .thenReturn(List.of(Namespace.of("db1"), Namespace.of("db2", "schema1")));

            List<String> namespaces = clientWithNamespaces.listNamespaces();

            assertThat(namespaces).containsExactly("db1", "db2.schema1");
        }

        @Test
        @DisplayName("should return empty list when catalog doesn't support namespaces")
        void shouldReturnEmptyForNonNamespaceCatalog() {
            List<String> namespaces = client.listNamespaces();

            assertThat(namespaces).isEmpty();
        }

        @Test
        @DisplayName("should return empty list on exception")
        void shouldReturnEmptyOnException() {
            when(mockNamespacesCatalog.listNamespaces())
                    .thenThrow(new RuntimeException("Error listing namespaces"));

            List<String> namespaces = clientWithNamespaces.listNamespaces();

            assertThat(namespaces).isEmpty();
        }
    }

    @Nested
    @DisplayName("listTables()")
    class ListTables {

        @Test
        @DisplayName("should return table identifiers for namespace")
        void shouldReturnTables() {
            Namespace ns = Namespace.of("mydb");
            when(mockCatalog.listTables(ns))
                    .thenReturn(
                            List.of(
                                    org.apache.iceberg.catalog.TableIdentifier.of(ns, "table1"),
                                    org.apache.iceberg.catalog.TableIdentifier.of(ns, "table2")));

            List<TableIdentifier> tables = client.listTables("mydb");

            assertThat(tables).hasSize(2);
            assertThat(tables.get(0).catalog()).isEqualTo(CATALOG_NAME);
            assertThat(tables.get(0).namespace()).isEqualTo("mydb");
            assertThat(tables.get(0).table()).isEqualTo("table1");
            assertThat(tables.get(1).table()).isEqualTo("table2");
        }

        @Test
        @DisplayName("should handle multi-level namespace")
        void shouldHandleMultiLevelNamespace() {
            Namespace ns = Namespace.of("db", "schema");
            when(mockCatalog.listTables(ns))
                    .thenReturn(
                            List.of(org.apache.iceberg.catalog.TableIdentifier.of(ns, "table1")));

            List<TableIdentifier> tables = client.listTables("db.schema");

            assertThat(tables).hasSize(1);
            assertThat(tables.get(0).namespace()).isEqualTo("db.schema");
        }

        @Test
        @DisplayName("should return empty list for non-existent namespace")
        void shouldReturnEmptyForNonExistentNamespace() {
            when(mockCatalog.listTables(any()))
                    .thenThrow(
                            new org.apache.iceberg.exceptions.NoSuchNamespaceException(
                                    "Namespace not found"));

            List<TableIdentifier> tables = client.listTables("nonexistent");

            assertThat(tables).isEmpty();
        }
    }

    @Nested
    @DisplayName("listAllTables()")
    class ListAllTables {

        @Test
        @DisplayName("should aggregate tables from all namespaces")
        void shouldAggregateAllTables() {
            when(mockNamespacesCatalog.listNamespaces())
                    .thenReturn(List.of(Namespace.of("db1"), Namespace.of("db2")));

            when(mockNamespacesCatalog.listTables(Namespace.of("db1")))
                    .thenReturn(
                            List.of(
                                    org.apache.iceberg.catalog.TableIdentifier.of(
                                            Namespace.of("db1"), "t1")));

            when(mockNamespacesCatalog.listTables(Namespace.of("db2")))
                    .thenReturn(
                            List.of(
                                    org.apache.iceberg.catalog.TableIdentifier.of(
                                            Namespace.of("db2"), "t2"),
                                    org.apache.iceberg.catalog.TableIdentifier.of(
                                            Namespace.of("db2"), "t3")));

            List<TableIdentifier> allTables = clientWithNamespaces.listAllTables();

            assertThat(allTables).hasSize(3);
            assertThat(allTables)
                    .extracting(TableIdentifier::table)
                    .containsExactly("t1", "t2", "t3");
        }
    }

    @Nested
    @DisplayName("loadTable()")
    class LoadTable {

        @Mock private Table mockTable;

        @Test
        @DisplayName("should return table when it exists")
        void shouldReturnTableWhenExists() {
            org.apache.iceberg.catalog.TableIdentifier icebergId =
                    org.apache.iceberg.catalog.TableIdentifier.of(Namespace.of("mydb"), "mytable");
            when(mockCatalog.loadTable(icebergId)).thenReturn(mockTable);

            TableIdentifier id = new TableIdentifier(CATALOG_NAME, "mydb", "mytable");
            Optional<Table> result = client.loadTable(id);

            assertThat(result).isPresent();
            assertThat(result.get()).isSameAs(mockTable);
        }

        @Test
        @DisplayName("should return empty when table doesn't exist")
        void shouldReturnEmptyWhenNotExists() {
            when(mockCatalog.loadTable(any()))
                    .thenThrow(
                            new org.apache.iceberg.exceptions.NoSuchTableException(
                                    "Table not found"));

            TableIdentifier id = new TableIdentifier(CATALOG_NAME, "mydb", "nonexistent");
            Optional<Table> result = client.loadTable(id);

            assertThat(result).isEmpty();
        }
    }

    @Nested
    @DisplayName("getTableMetadata()")
    class GetTableMetadata {

        @Mock private Table mockTable;

        @Mock private Snapshot mockSnapshot;

        @Mock private FileIO mockFileIO;

        @Mock private TableScan mockScan;

        @Mock private Schema mockSchema;

        @Mock private PartitionSpec mockSpec;

        @Mock private SortOrder mockSortOrder;

        @Test
        @DisplayName("should build metadata from table")
        void shouldBuildMetadata() {
            // Setup table mock
            org.apache.iceberg.catalog.TableIdentifier icebergId =
                    org.apache.iceberg.catalog.TableIdentifier.of(Namespace.of("mydb"), "mytable");
            when(mockCatalog.loadTable(icebergId)).thenReturn(mockTable);

            // Setup snapshot
            when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
            when(mockTable.snapshots()).thenReturn(List.of(mockSnapshot));
            when(mockSnapshot.snapshotId()).thenReturn(123L);
            when(mockSnapshot.timestampMillis()).thenReturn(Instant.now().toEpochMilli());
            when(mockSnapshot.allManifests(any())).thenReturn(Collections.emptyList());

            // Setup table properties
            when(mockTable.location()).thenReturn("s3://bucket/warehouse/mydb/mytable");
            when(mockTable.properties()).thenReturn(Map.of("key", "value"));
            when(mockTable.io()).thenReturn(mockFileIO);

            // Setup partition spec (empty = unpartitioned)
            when(mockTable.spec()).thenReturn(mockSpec);
            when(mockSpec.fields()).thenReturn(Collections.emptyList());

            // Setup sort order (unsorted)
            when(mockTable.sortOrder()).thenReturn(mockSortOrder);
            when(mockSortOrder.isUnsorted()).thenReturn(true);

            // Setup scan (for data file counting)
            when(mockTable.newScan()).thenReturn(mockScan);
            when(mockScan.planFiles()).thenReturn(CloseableIterable.empty());

            TableIdentifier id = new TableIdentifier(CATALOG_NAME, "mydb", "mytable");
            Optional<TableMetadata> result = client.getTableMetadata(id);

            assertThat(result).isPresent();
            TableMetadata metadata = result.get();
            assertThat(metadata.identifier()).isEqualTo(id);
            assertThat(metadata.location()).isEqualTo("s3://bucket/warehouse/mydb/mytable");
            assertThat(metadata.snapshotCount()).isEqualTo(1);
            assertThat(metadata.currentSnapshotId()).isEqualTo(123L);
            assertThat(metadata.formatVersion()).isEqualTo(1);
            assertThat(metadata.partitionSpec()).isEqualTo("unpartitioned");
            assertThat(metadata.sortOrder()).isEqualTo("unsorted");
        }

        @Test
        @DisplayName("should handle table with no snapshots")
        void shouldHandleNoSnapshots() {
            org.apache.iceberg.catalog.TableIdentifier icebergId =
                    org.apache.iceberg.catalog.TableIdentifier.of(Namespace.of("mydb"), "empty");
            when(mockCatalog.loadTable(icebergId)).thenReturn(mockTable);

            when(mockTable.currentSnapshot()).thenReturn(null);
            when(mockTable.snapshots()).thenReturn(Collections.emptyList());
            when(mockTable.location()).thenReturn("s3://bucket/warehouse/mydb/empty");
            when(mockTable.properties()).thenReturn(Collections.emptyMap());
            when(mockTable.spec()).thenReturn(mockSpec);
            when(mockSpec.fields()).thenReturn(Collections.emptyList());
            when(mockTable.sortOrder()).thenReturn(mockSortOrder);
            when(mockSortOrder.isUnsorted()).thenReturn(true);

            TableIdentifier id = new TableIdentifier(CATALOG_NAME, "mydb", "empty");
            Optional<TableMetadata> result = client.getTableMetadata(id);

            assertThat(result).isPresent();
            TableMetadata metadata = result.get();
            assertThat(metadata.snapshotCount()).isEqualTo(0);
            assertThat(metadata.currentSnapshotId()).isEqualTo(-1);
            assertThat(metadata.dataFileCount()).isEqualTo(0);
        }
    }

    /** Test implementation of AbstractIcebergCatalogClient for testing. */
    private static class TestCatalogClient extends AbstractIcebergCatalogClient {

        private final Catalog catalog;

        TestCatalogClient(String catalogName, Catalog catalog) {
            super(catalogName);
            this.catalog = catalog;
        }

        @Override
        protected Catalog getCatalog() {
            return catalog;
        }

        @Override
        public void close() {
            // No-op for tests
        }
    }

    /** Mock catalog that also implements SupportsNamespaces. */
    private interface NamespaceCatalog extends Catalog, SupportsNamespaces {}
}
