package com.floe.server.resource;

import com.floe.core.auth.Permission;
import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.catalog.TableMetadata;
import com.floe.core.health.HealthReport;
import com.floe.core.health.TableHealthAssessor;
import com.floe.server.api.ErrorResponse;
import com.floe.server.api.HealthReportResponse;
import com.floe.server.api.TableDetailResponse;
import com.floe.server.api.validation.PaginationValidator;
import com.floe.server.auth.Secured;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST resource for browsing Iceberg tables via the catalog.
 *
 * <p>Provides endpoints for listing namespaces, tables, retrieving table metadata and health.
 */
@Path("/api/v1/tables")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class TablesResource {

    private static final Logger LOG = LoggerFactory.getLogger(TablesResource.class);

    @Inject CatalogClient catalogClient;

    /**
     * List all namespaces in the catalog.
     *
     * @return JSON response containing catalog name, namespace list, and total count
     */
    @GET
    @Path("/namespaces")
    @Secured(Permission.READ_TABLES)
    public Response listNamespaces() {
        LOG.debug("Listing namespaces");

        try {
            List<String> namespaces = catalogClient.listNamespaces();
            return Response.ok(
                            Map.of(
                                    "catalog",
                                    catalogClient.getCatalogName(),
                                    "namespaces",
                                    namespaces,
                                    "total",
                                    namespaces.size()))
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to list namespaces", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /** List all tables in a namespace with pagination. */
    @GET
    @Path("/namespaces/{namespace}")
    @Secured(Permission.READ_TABLES)
    public Response listTablesInNamespace(
            @PathParam("namespace") String namespace,
            @QueryParam("limit") @DefaultValue("20") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        LOG.debug("Listing tables in namespace: {}, limit={}, offset={}", namespace, limit, offset);

        // Validate pagination params
        Optional<Response> validationError = PaginationValidator.validate(limit, offset);
        if (validationError.isPresent()) {
            return validationError.get();
        }

        try {
            // Get all tables in namespace and paginate in-memory
            List<TableIdentifier> allTables = catalogClient.listTables(namespace);
            int total = allTables.size();

            List<TableIdentifier> paginatedTables =
                    allTables.stream().skip(offset).limit(limit).toList();

            List<TableSummary> summaries =
                    paginatedTables.stream().map(TableSummary::from).toList();

            boolean hasMore = offset + summaries.size() < total;

            return Response.ok(
                            Map.of(
                                    "catalog",
                                    catalogClient.getCatalogName(),
                                    "namespace",
                                    namespace,
                                    "tables",
                                    summaries,
                                    "total",
                                    total,
                                    "limit",
                                    limit,
                                    "offset",
                                    offset,
                                    "hasMore",
                                    hasMore))
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to list tables in namespace: {}", namespace, e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /** List all tables across all namespaces with pagination. */
    @GET
    @Secured(Permission.READ_TABLES)
    public Response listAllTables(
            @QueryParam("limit") @DefaultValue("20") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        LOG.debug("Listing all tables: limit={}, offset={}", limit, offset);

        // Validate pagination params
        Optional<Response> validationError = PaginationValidator.validate(limit, offset);
        if (validationError.isPresent()) {
            return validationError.get();
        }

        try {
            // Get all tables and paginate in-memory
            // (Iceberg REST catalog doesn't support server-side pagination)
            List<TableIdentifier> allTables = catalogClient.listAllTables();
            int total = allTables.size();

            List<TableIdentifier> paginatedTables =
                    allTables.stream().skip(offset).limit(limit).toList();

            List<TableSummary> summaries =
                    paginatedTables.stream().map(TableSummary::from).toList();

            boolean hasMore = offset + summaries.size() < total;

            return Response.ok(
                            Map.of(
                                    "catalog",
                                    catalogClient.getCatalogName(),
                                    "tables",
                                    summaries,
                                    "total",
                                    total,
                                    "limit",
                                    limit,
                                    "offset",
                                    offset,
                                    "hasMore",
                                    hasMore))
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to list all tables", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /** Get detailed metadata for a specific table. */
    @GET
    @Path("/{namespace}/{table}")
    @Secured(Permission.READ_TABLES)
    public Response getTable(
            @PathParam("namespace") String namespace, @PathParam("table") String table) {
        LOG.debug("Getting table: {}.{}", namespace, table);

        try {
            TableIdentifier id =
                    TableIdentifier.of(catalogClient.getCatalogName(), namespace, table);
            Optional<TableMetadata> metadataOpt = catalogClient.getTableMetadata(id);

            if (metadataOpt.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(ErrorResponse.notFound("Table", namespace + "." + table))
                        .build();
            }

            return Response.ok(TableDetailResponse.from(metadataOpt.get())).build();
        } catch (Exception e) {
            LOG.error("Failed to get table {}.{}", namespace, table, e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /** Check if a table exists. */
    @HEAD
    @Path("/{namespace}/{table}")
    @Secured(Permission.READ_TABLES)
    public Response tableExists(
            @PathParam("namespace") String namespace, @PathParam("table") String table) {
        LOG.debug("Checking if table exists: {}.{}", namespace, table);

        try {
            TableIdentifier id =
                    TableIdentifier.of(catalogClient.getCatalogName(), namespace, table);
            Optional<TableMetadata> metadataOpt = catalogClient.getTableMetadata(id);

            if (metadataOpt.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            return Response.ok().build();
        } catch (Exception e) {
            LOG.error("Failed to check table {}.{}", namespace, table, e);
            return Response.serverError().build();
        }
    }

    /** Get health assessment for a table. */
    @GET
    @Path("/{namespace}/{table}/health")
    @Secured(Permission.READ_TABLES)
    public Response getTableHealth(
            @PathParam("namespace") String namespace, @PathParam("table") String table) {
        LOG.debug("Getting health for table: {}.{}", namespace, table);

        TableIdentifier tableIdentifier =
                TableIdentifier.of(catalogClient.getCatalogName(), namespace, table);
        Optional<Table> tableOpt = catalogClient.loadTable(tableIdentifier);
        if (tableOpt.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("Table", namespace + "." + table))
                    .build();
        }
        TableHealthAssessor assessor = new TableHealthAssessor();
        HealthReport healthReport = assessor.assess(tableIdentifier, tableOpt.get());
        return Response.ok(HealthReportResponse.from(healthReport)).build();
    }

    /** Table summary. */
    private record TableSummary(
            String catalog, String namespace, String name, String qualifiedName) {
        public static TableSummary from(TableIdentifier id) {
            return new TableSummary(id.catalog(), id.namespace(), id.table(), id.toQualifiedName());
        }
    }
}
