package com.floe.core.catalog;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for Iceberg catalog clients.
 *
 * <p>Provides common implementation for catalog operations that work with any Iceberg Catalog
 * implementation (REST, Hive, etc.).
 */
public abstract class AbstractIcebergCatalogClient implements CatalogClient {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    protected final String catalogName;

    protected AbstractIcebergCatalogClient(String catalogName) {
        this.catalogName = catalogName;
    }

    /** Get the underlying Iceberg Catalog instance. */
    protected abstract Catalog getCatalog();

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public boolean isHealthy() {
        try {
            Catalog catalog = getCatalog();
            if (catalog instanceof SupportsNamespaces) {
                ((SupportsNamespaces) catalog).listNamespaces();
            } else {
                // Fallback: try to list tables in root namespace
                catalog.listTables(Namespace.empty());
            }
            return true;
        } catch (Exception e) {
            LOG.warn("Catalog '{}' health check failed: {}", catalogName, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public List<String> listNamespaces() {
        try {
            Catalog catalog = getCatalog();
            if (catalog instanceof SupportsNamespaces) {
                return ((SupportsNamespaces) catalog)
                        .listNamespaces().stream()
                                .map(ns -> String.join(".", ns.levels()))
                                .toList();
            } else {
                LOG.warn("Catalog '{}' does not support namespace listing", catalogName);
                return Collections.emptyList();
            }
        } catch (Exception e) {
            LOG.error("Failed to list namespaces in catalog '{}': {}", catalogName, e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public List<TableIdentifier> listTables(String namespace) {
        try {
            Namespace ns = Namespace.of(namespace.split("\\."));
            return getCatalog().listTables(ns).stream()
                    .map(id -> new TableIdentifier(catalogName, namespace, id.name()))
                    .toList();
        } catch (NoSuchNamespaceException e) {
            LOG.debug("Namespace '{}' does not exist in catalog '{}'", namespace, catalogName);
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error(
                    "Failed to list tables in namespace '{}' for catalog '{}': {}",
                    namespace,
                    catalogName,
                    e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public List<TableIdentifier> listAllTables() {
        List<TableIdentifier> allTables = new ArrayList<>();
        for (String namespace : listNamespaces()) {
            allTables.addAll(listTables(namespace));
        }
        return allTables;
    }

    @Override
    public Optional<Table> loadTable(TableIdentifier identifier) {
        try {
            org.apache.iceberg.catalog.TableIdentifier icebergId =
                    org.apache.iceberg.catalog.TableIdentifier.of(
                            Namespace.of(identifier.namespace().split("\\.")), identifier.table());
            return Optional.of(getCatalog().loadTable(icebergId));
        } catch (Exception e) {
            LOG.warn("Failed to load table '{}': {}", identifier, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public Optional<TableMetadata> getTableMetadata(TableIdentifier identifier) {
        return loadTable(identifier).map(table -> buildMetadata(identifier, table));
    }

    protected TableMetadata buildMetadata(TableIdentifier identifier, Table table) {
        Snapshot currentSnapshot = table.currentSnapshot();

        // Count snapshots and find oldest
        int snapshotCount = 0;
        Instant oldestSnapshotTimestamp = null;
        for (Snapshot snapshot : table.snapshots()) {
            snapshotCount++;
            Instant ts = Instant.ofEpochMilli(snapshot.timestampMillis());
            if (oldestSnapshotTimestamp == null || ts.isBefore(oldestSnapshotTimestamp)) {
                oldestSnapshotTimestamp = ts;
            }
        }

        long currentSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : -1;
        Instant snapshotTimestamp =
                currentSnapshot != null
                        ? Instant.ofEpochMilli(currentSnapshot.timestampMillis())
                        : null;

        int dataFileCount = 0;
        long totalDataFileSize = 0;
        int deleteFileCount = 0;
        int positionDeleteFileCount = 0;
        int equalityDeleteFileCount = 0;
        long totalRecordCount = 0;
        int manifestCount = 0;
        long totalManifestSize = 0;

        if (currentSnapshot != null) {
            try {
                for (ManifestFile manifest : currentSnapshot.allManifests(table.io())) {
                    manifestCount++;
                    totalManifestSize += manifest.length();
                }
            } catch (Exception e) {
                LOG.warn("Error reading manifests for {}: {}", identifier, e.getMessage());
            }

            try (var tasks = table.newScan().planFiles().iterator()) {
                while (tasks.hasNext()) {
                    var task = tasks.next();
                    var dataFile = task.file();
                    dataFileCount++;
                    totalDataFileSize += dataFile.fileSizeInBytes();
                    totalRecordCount += dataFile.recordCount();

                    // Count delete files associated with this task
                    for (var deleteFile : task.deletes()) {
                        deleteFileCount++;
                        if (deleteFile.content() == FileContent.POSITION_DELETES) {
                            positionDeleteFileCount++;
                        } else if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
                            equalityDeleteFileCount++;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Error counting data files for {}: {}", identifier, e.getMessage());
            }
        }

        // Get format version from table properties
        int formatVersion = 1;
        String formatVersionStr = table.properties().get("format-version");
        if (formatVersionStr != null) {
            try {
                formatVersion = Integer.parseInt(formatVersionStr);
            } catch (NumberFormatException e) {
                LOG.debug("Could not parse format-version: {}", formatVersionStr);
            }
        }

        // Build partition spec
        String partitionSpec =
                table.spec().fields().stream()
                        .map(
                                field -> {
                                    String sourceName =
                                            table.schema().findColumnName(field.sourceId());
                                    String transform = field.transform().toString();
                                    return sourceName + " (" + transform + ")";
                                })
                        .collect(Collectors.joining(", "));
        if (partitionSpec.isEmpty()) {
            partitionSpec = "unpartitioned";
        }

        // Build sort order
        String sortOrder;
        if (table.sortOrder().isUnsorted()) {
            sortOrder = "unsorted";
        } else {
            sortOrder =
                    table.sortOrder().fields().stream()
                            .map(
                                    field -> {
                                        String sourceName =
                                                table.schema().findColumnName(field.sourceId());
                                        String direction =
                                                field.direction()
                                                        .toString()
                                                        .toLowerCase(Locale.ROOT);
                                        String nullOrder =
                                                field.nullOrder()
                                                        .toString()
                                                        .toLowerCase(Locale.ROOT);
                                        return sourceName + " " + direction + " " + nullOrder;
                                    })
                            .collect(Collectors.joining(", "));
        }

        return TableMetadata.builder()
                .identifier(identifier)
                .location(table.location())
                .snapshotCount(snapshotCount)
                .currentSnapshotId(currentSnapshotId)
                .currentSnapshotTimestamp(snapshotTimestamp)
                .oldestSnapshotTimestamp(oldestSnapshotTimestamp)
                .dataFileCount(dataFileCount)
                .totalDataFileSizeBytes(totalDataFileSize)
                .deleteFileCount(deleteFileCount)
                .positionDeleteFileCount(positionDeleteFileCount)
                .equalityDeleteFileCount(equalityDeleteFileCount)
                .totalRecordCount(totalRecordCount)
                .manifestCount(manifestCount)
                .totalManifestSizeBytes(totalManifestSize)
                .formatVersion(formatVersion)
                .partitionSpec(partitionSpec)
                .sortOrder(sortOrder)
                .properties(table.properties())
                .lastModified(snapshotTimestamp)
                .build();
    }
}
