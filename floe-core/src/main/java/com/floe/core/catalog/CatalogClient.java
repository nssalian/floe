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
    /** List all namespaces (databases) in the catalog. */
    List<String> listNamespaces();

    /** List all tables in a namespace. */
    List<TableIdentifier> listTables(String namespace);

    /** List all tables across all namespaces. */
    List<TableIdentifier> listAllTables();

    /** Load a table by its identifier. */
    Optional<Table> loadTable(TableIdentifier identifier);

    /** Get metadata for a table. */
    Optional<TableMetadata> getTableMetadata(TableIdentifier identifier);

    /** Check if the catalog is reachable. */
    boolean isHealthy();

    /** Get the catalog name. */
    String getCatalogName();

    /** Close the catalog client and release any resources. */
    void close();
}
