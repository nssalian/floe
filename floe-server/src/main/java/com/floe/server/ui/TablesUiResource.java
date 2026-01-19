package com.floe.server.ui;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.catalog.TableMetadata;
import com.floe.core.health.HealthIssue;
import com.floe.core.health.HealthReport;
import com.floe.core.health.TableHealthAssessor;
import com.floe.server.config.FloeConfig;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** UI resource for tables pages. */
@Path("/ui/tables")
@Produces(MediaType.TEXT_HTML)
public class TablesUiResource extends UiResource {

    private static final Logger LOG = LoggerFactory.getLogger(TablesUiResource.class);

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("MMM d, yyyy h:mm a").withZone(ZoneId.systemDefault());

    @Inject Template tables;

    @Inject
    @io.quarkus.qute.Location("table-detail.html")
    Template tableDetail;

    @Inject CatalogClient catalogClient;

    @Inject FloeConfig config;

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 100;

    /** Main tables list page with pagination. */
    @GET
    public TemplateInstance index(
            @QueryParam("limit") @DefaultValue("50") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        LOG.debug("Loading tables list: limit={}, offset={}", limit, offset);

        // Clamp limit to valid range
        limit = Math.max(1, Math.min(limit, MAX_LIMIT));
        offset = Math.max(0, offset);

        boolean catalogHealthy = false;
        List<TableListView> tableViews = List.of();
        List<String> namespaces = List.of();
        String errorMessage = null;
        int totalTables = 0;
        boolean hasMore = false;

        try {
            catalogHealthy = catalogClient.isHealthy();
            if (catalogHealthy) {
                namespaces = catalogClient.listNamespaces();
                List<TableIdentifier> allTables = catalogClient.listAllTables();
                totalTables = allTables.size();

                // Apply pagination
                List<TableIdentifier> paginatedTables =
                        allTables.stream().skip(offset).limit(limit).toList();

                tableViews = paginatedTables.stream().map(TableListView::from).toList();
                hasMore = offset + tableViews.size() < totalTables;
            }
        } catch (Exception e) {
            LOG.error("Failed to load tables", e);
            errorMessage = "Failed to connect to catalog: " + e.getMessage();
        }

        return page(tables, "/ui/tables")
                .data("catalogName", catalogClient.getCatalogName())
                .data("catalogType", formatCatalogType(config.catalog().type()))
                .data("catalogHealthy", catalogHealthy)
                .data("tables", tableViews)
                .data("namespaces", namespaces)
                .data("totalTables", totalTables)
                .data("totalNamespaces", namespaces.size())
                .data("errorMessage", errorMessage)
                .data("limit", limit)
                .data("offset", offset)
                .data("hasMore", hasMore)
                .data("nextOffset", offset + limit)
                .data("prevOffset", Math.max(0, offset - limit))
                .data("hasPrev", offset > 0);
    }

    /** Table detail page. */
    @GET
    @Path("/{namespace}/{table}")
    public Response detail(
            @PathParam("namespace") String namespace, @PathParam("table") String tableName) {
        LOG.debug("Loading table detail: {}.{}", namespace, tableName);

        try {
            TableIdentifier id =
                    TableIdentifier.of(catalogClient.getCatalogName(), namespace, tableName);

            // Load the table for health assessment
            Optional<Table> tableOpt = catalogClient.loadTable(id);
            if (tableOpt.isEmpty()) {
                return Response.seeOther(URI.create("/ui/tables")).build();
            }

            Optional<TableMetadata> metadataOpt = catalogClient.getTableMetadata(id);
            if (metadataOpt.isEmpty()) {
                return Response.seeOther(URI.create("/ui/tables")).build();
            }

            // Perform health assessment
            TableHealthAssessor assessor = new TableHealthAssessor();
            HealthReport healthReport = assessor.assess(id, tableOpt.get());

            TableDetailView view = TableDetailView.from(metadataOpt.get(), healthReport);
            TemplateInstance template =
                    page(tableDetail, "/ui/tables")
                            .data("table", view)
                            .data("catalogName", catalogClient.getCatalogName());

            return Response.ok(template).build();
        } catch (Exception e) {
            LOG.error("Failed to load table {}.{}", namespace, tableName, e);
            return Response.seeOther(URI.create("/ui/tables")).build();
        }
    }

    // View models for templates

    /** View model for tables list. */
    public record TableListView(
            String catalog, String namespace, String name, String qualifiedName) {
        public static TableListView from(TableIdentifier id) {
            return new TableListView(
                    id.catalog(), id.namespace(), id.table(), id.toQualifiedName());
        }
    }

    /** View model for table detail page. */
    public record TableDetailView(
            String catalog,
            String namespace,
            String name,
            String qualifiedName,
            String location,
            int snapshotCount,
            long currentSnapshotId,
            String currentSnapshotTimestamp,
            String oldestSnapshotTimestamp,
            int dataFileCount,
            String totalDataSizeFormatted,
            String averageFileSizeFormatted,
            long totalRecordCount,
            String totalRecordCountFormatted,
            int manifestCount,
            String totalManifestSizeFormatted,
            int formatVersion,
            String partitionSpec,
            String sortOrder,
            String lastModifiedFormatted,
            boolean isEmpty,
            // Health assessment fields
            String healthStatus,
            int smallFileCount,
            int deleteFileCount,
            int positionDeleteFileCount,
            int equalityDeleteFileCount,
            int partitionCount,
            List<HealthIssueView> issues) {
        public static TableDetailView from(TableMetadata meta, HealthReport health) {
            String status = "healthy";
            if (health.issues().stream()
                    .anyMatch(i -> i.severity() == HealthIssue.Severity.CRITICAL)) {
                status = "critical";
            } else if (health.issues().stream()
                    .anyMatch(i -> i.severity() == HealthIssue.Severity.WARNING)) {
                status = "warning";
            }

            return new TableDetailView(
                    meta.identifier().catalog(),
                    meta.identifier().namespace(),
                    meta.identifier().table(),
                    meta.identifier().toQualifiedName(),
                    meta.location(),
                    meta.snapshotCount(),
                    meta.currentSnapshotId(),
                    formatInstant(meta.currentSnapshotTimestamp()),
                    formatInstant(meta.oldestSnapshotTimestamp()),
                    meta.dataFileCount(),
                    formatBytes(meta.totalDataFileSizeBytes()),
                    formatAverageFileSize(meta.totalDataFileSizeBytes(), meta.dataFileCount()),
                    meta.totalRecordCount(),
                    formatNumber(meta.totalRecordCount()),
                    meta.manifestCount(),
                    formatBytes(meta.totalManifestSizeBytes()),
                    meta.formatVersion(),
                    meta.partitionSpec(),
                    meta.sortOrder(),
                    formatInstant(meta.lastModified()),
                    meta.isEmpty(),
                    status,
                    health.smallFileCount(),
                    meta.deleteFileCount(),
                    meta.positionDeleteFileCount(),
                    meta.equalityDeleteFileCount(),
                    health.partitionCount(),
                    health.issues().stream().map(HealthIssueView::from).toList());
        }
    }

    /** View model for health issues. */
    public record HealthIssueView(String type, String severity, String message) {
        public static HealthIssueView from(HealthIssue issue) {
            return new HealthIssueView(
                    formatIssueType(issue.type()),
                    issue.severity().name().toLowerCase(Locale.ROOT),
                    issue.message());
        }

        private static String formatIssueType(HealthIssue.Type type) {
            return switch (type) {
                case TOO_MANY_SMALL_FILES -> "Too Many Small Files";
                case TOO_MANY_LARGE_FILES -> "Too Many Large Files";
                case HIGH_FILE_COUNT -> "High File Count";
                case TOO_MANY_SNAPSHOTS -> "Too Many Snapshots";
                case OLD_SNAPSHOTS -> "Old Snapshots";
                case TOO_MANY_DELETE_FILES -> "Too Many Delete Files";
                case HIGH_DELETE_FILE_RATIO -> "High Delete File Ratio";
                case TOO_MANY_MANIFESTS -> "Too Many Manifests";
                case LARGE_MANIFEST_LIST -> "Large Manifest List";
                case TOO_MANY_PARTITIONS -> "Too Many Partitions";
                case PARTITION_SKEW -> "Partition Skew";
                case TABLE_EMPTY -> "Table Empty";
                case STALE_METADATA -> "Stale Metadata";
            };
        }
    }

    // Formatting helpers

    private static String formatInstant(Instant instant) {
        if (instant == null) {
            return "-";
        }
        return DATE_FORMAT.format(instant);
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static String formatAverageFileSize(long totalBytes, int fileCount) {
        if (fileCount == 0) return "-";
        long avgBytes = totalBytes / fileCount;
        return formatBytes(avgBytes);
    }

    private static String formatNumber(long number) {
        if (number < 1_000) return String.valueOf(number);
        if (number < 1_000_000) return String.format("%.1fK", number / 1_000.0);
        if (number < 1_000_000_000) return String.format("%.1fM", number / 1_000_000.0);
        return String.format("%.2fB", number / 1_000_000_000.0);
    }

    private static String formatCatalogType(String type) {
        return switch (type.toUpperCase(Locale.ROOT)) {
            case "REST" -> "REST Catalog";
            case "HIVE" -> "Hive Metastore";
            case "POLARIS" -> "Polaris";
            case "NESSIE" -> "Nessie";
            default -> type;
        };
    }
}
