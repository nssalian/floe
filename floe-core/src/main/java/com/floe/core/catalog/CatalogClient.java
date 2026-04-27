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

import java.util.List;
import java.util.Optional;
import org.apache.iceberg.Table;

/**
 * Client interface for interacting with Iceberg catalogs.
 *
 * <p>Implementations connect to specific catalog types (REST, Hive, Nessie, Polaris) and provide
 * table discovery and metadata access for maintenance operations.
 */
public interface CatalogClient {
    /**
     * List all namespaces (databases) in the catalog.
     *
     * @return list of namespace names
     */
    List<String> listNamespaces();

    /**
     * List all tables in a namespace.
     *
     * @param namespace the namespace to list tables from
     * @return list of table identifiers in the namespace
     */
    List<TableIdentifier> listTables(String namespace);

    /**
     * List all tables across all namespaces.
     *
     * @return list of all table identifiers in the catalog
     */
    List<TableIdentifier> listAllTables();

    /**
     * Load a table by its identifier.
     *
     * @param identifier the table identifier
     * @return the Iceberg table if found, empty otherwise
     */
    Optional<Table> loadTable(TableIdentifier identifier);

    /**
     * Get metadata for a table.
     *
     * @param identifier the table identifier
     * @return the table metadata if found, empty otherwise
     */
    Optional<TableMetadata> getTableMetadata(TableIdentifier identifier);

    /**
     * Check if the catalog is reachable.
     *
     * @return true if the catalog is healthy and responding, false otherwise
     */
    boolean isHealthy();

    /**
     * Get the catalog name.
     *
     * @return the configured catalog name
     */
    String getCatalogName();

    /** Close the catalog client and release any resources. */
    void close();
}
