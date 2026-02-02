package com.floe.server.resource;

import com.floe.core.auth.Permission;
import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.catalog.TableMetadata;
import com.floe.core.health.HealthReport;
import com.floe.core.health.HealthThresholds;
import com.floe.core.health.TableHealthAssessor;
import com.floe.core.health.TableHealthStore;
import com.floe.core.operation.OperationStore;
import com.floe.core.orchestrator.TriggerEvaluator;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OperationType;
import com.floe.core.policy.PolicyMatcher;
import com.floe.core.policy.TriggerConditions;
import com.floe.server.api.ErrorResponse;
import com.floe.server.api.HealthReportResponse;
import com.floe.server.api.TableDetailResponse;
import com.floe.server.api.TriggerStatusResponse;
import com.floe.server.api.TriggerStatusResponse.OperationTriggerStatus;
import com.floe.server.api.validation.PaginationValidator;
import com.floe.server.auth.Secured;
import com.floe.server.config.HealthConfig;
import com.floe.server.health.HealthReportCache;
import com.floe.server.health.HealthReportCache.HealthCacheKey;
import com.floe.server.scheduler.SchedulerConfig;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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

    @Inject HealthConfig healthConfig;

    @Inject TableHealthStore healthStore;

    @Inject PolicyMatcher policyMatcher;

    @Inject HealthReportCache healthReportCache;

    @Inject OperationStore operationStore;

    @Inject SchedulerConfig schedulerConfig;

    private boolean shouldPersistHealth(TableIdentifier id, HealthReport report) {
        List<HealthReport> history =
                healthStore.findHistory(id.catalog(), id.namespace(), id.table(), 1);
        if (history.isEmpty()) {
            return true;
        }
        HealthReport latest = history.get(0);
        int latestScore = computeScore(latest);
        int currentScore = computeScore(report);
        if (latestScore != currentScore) {
            return true;
        }
        return !issueSignature(latest).equals(issueSignature(report));
    }

    private static int computeScore(HealthReport report) {
        if (report == null) {
            return 100;
        }
        int score = 100;
        for (var issue : report.issues()) {
            if (issue.severity() == com.floe.core.health.HealthIssue.Severity.CRITICAL) {
                score -= 30;
            } else if (issue.severity() == com.floe.core.health.HealthIssue.Severity.WARNING) {
                score -= 10;
            }
        }
        return Math.max(0, score);
    }

    private static Set<String> issueSignature(HealthReport report) {
        if (report == null || report.issues() == null) {
            return Set.of();
        }
        return report.issues().stream()
                .map(issue -> issue.type().name() + ":" + issue.severity().name())
                .collect(Collectors.toSet());
    }

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

    /**
     * List all tables in a namespace with pagination.
     *
     * @param namespace the namespace to list tables from
     * @param limit maximum number of tables to return (default 20)
     * @param offset number of tables to skip for pagination (default 0)
     * @return 200 OK with paginated list of tables
     */
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

    /**
     * List all tables across all namespaces with pagination.
     *
     * @param limit maximum number of tables to return (default 20)
     * @param offset number of tables to skip for pagination (default 0)
     * @return 200 OK with paginated list of all tables
     */
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

    /**
     * Get detailed metadata for a specific table.
     *
     * @param namespace the namespace containing the table
     * @param table the table name
     * @return 200 OK with table metadata, or 404 Not Found
     */
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

    /**
     * Check if a table exists.
     *
     * @param namespace the namespace containing the table
     * @param table the table name
     * @return 200 OK if the table exists, or 404 Not Found
     */
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

    /**
     * Get health assessment for a table.
     *
     * @param namespace the namespace containing the table
     * @param table the table name
     * @return 200 OK with health report, or 404 Not Found
     */
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
        MaintenancePolicy matchedPolicy =
                policyMatcher
                        .findEffectivePolicy(catalogClient.getCatalogName(), tableIdentifier)
                        .orElse(null);
        HealthThresholds thresholds =
                matchedPolicy != null
                        ? matchedPolicy.effectiveThresholds()
                        : HealthThresholds.defaults();
        TableHealthAssessor.ScanMode scanMode =
                TableHealthAssessor.ScanMode.fromString(healthConfig.scanMode());
        HealthCacheKey cacheKey =
                HealthCacheKey.of(
                        tableIdentifier, scanMode, healthConfig.sampleLimit(), thresholds);
        Optional<HealthReport> cachedReport = healthReportCache.getIfFresh(cacheKey);
        HealthReport healthReport;
        boolean fromCache = false;
        if (cachedReport.isPresent()) {
            healthReport = cachedReport.get();
            fromCache = true;
        } else {
            TableHealthAssessor assessor =
                    new TableHealthAssessor(thresholds, scanMode, healthConfig.sampleLimit());
            healthReport = assessor.assess(tableIdentifier, tableOpt.get());
            healthReportCache.put(cacheKey, healthReport);
        }

        // Persist the health report if persistence is enabled
        if (!fromCache && healthConfig.persistenceEnabled() && healthStore != null) {
            try {
                if (shouldPersistHealth(tableIdentifier, healthReport)) {
                    healthStore.save(healthReport);
                    if (healthConfig.maxReportAgeDays() > 0) {
                        long cutoff =
                                Instant.now()
                                        .minusSeconds(healthConfig.maxReportAgeDays() * 86400L)
                                        .toEpochMilli();
                        healthStore.pruneOlderThan(cutoff);
                    }
                    if (healthConfig.maxReportsPerTable() > 0) {
                        healthStore.pruneHistory(
                                tableIdentifier.catalog(),
                                tableIdentifier.namespace(),
                                tableIdentifier.table(),
                                healthConfig.maxReportsPerTable());
                    }
                    LOG.debug("Persisted health report for table: {}.{}", namespace, table);
                }
            } catch (Exception e) {
                LOG.warn(
                        "Failed to persist health report for table {}.{}: {}",
                        namespace,
                        table,
                        e.getMessage());
            }
        }

        return Response.ok(HealthReportResponse.from(healthReport, thresholds)).build();
    }

    /**
     * Get health assessment history for a table.
     *
     * @param namespace the namespace containing the table
     * @param table the table name
     * @param limit maximum number of reports to return (default 10)
     * @return 200 OK with health report history
     */
    @GET
    @Path("/{namespace}/{table}/health/history")
    @Secured(Permission.READ_TABLES)
    public Response getTableHealthHistory(
            @PathParam("namespace") String namespace,
            @PathParam("table") String table,
            @QueryParam("limit") @DefaultValue("10") int limit) {
        LOG.debug("Getting health history for table: {}.{}, limit={}", namespace, table, limit);

        if (healthStore == null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(ErrorResponse.serviceUnavailable("Health store not configured"))
                    .build();
        }

        String catalog = catalogClient.getCatalogName();
        List<HealthReport> history = healthStore.findHistory(catalog, namespace, table, limit);

        if (history.isEmpty()) {
            // Check if table exists
            TableIdentifier tableIdentifier = TableIdentifier.of(catalog, namespace, table);
            Optional<Table> tableOpt = catalogClient.loadTable(tableIdentifier);
            if (tableOpt.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(ErrorResponse.notFound("Table", namespace + "." + table))
                        .build();
            }
        }

        List<HealthReportResponse> responses =
                history.stream().map(HealthReportResponse::from).toList();

        return Response.ok(
                        Map.of(
                                "catalog",
                                catalog,
                                "namespace",
                                namespace,
                                "table",
                                table,
                                "history",
                                responses,
                                "total",
                                responses.size()))
                .build();
    }

    /**
     * Get latest health assessments across all tables.
     *
     * @param limit maximum number of reports to return (default 20)
     * @return 200 OK with latest health reports
     */
    @GET
    @Path("/health/latest")
    @Secured(Permission.READ_TABLES)
    public Response getLatestHealthReports(@QueryParam("limit") @DefaultValue("20") int limit) {
        LOG.debug("Getting latest health reports, limit={}", limit);

        if (healthStore == null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(ErrorResponse.serviceUnavailable("Health store not configured"))
                    .build();
        }

        List<HealthReport> latest = healthStore.findLatest(limit);
        List<HealthReportResponse> responses =
                latest.stream().map(HealthReportResponse::from).toList();

        return Response.ok(
                        Map.of(
                                "catalog",
                                catalogClient.getCatalogName(),
                                "reports",
                                responses,
                                "total",
                                responses.size()))
                .build();
    }

    /**
     * Get trigger status for a table's maintenance operations.
     *
     * <p>Evaluates the current trigger conditions for each operation type against the table's
     * health metrics and returns whether each operation would trigger if scheduled now.
     *
     * @param namespace the namespace containing the table
     * @param table the table name
     * @return 200 OK with trigger status for each operation type
     */
    @GET
    @Path("/{namespace}/{table}/trigger-status")
    @Secured(Permission.READ_TABLES)
    public Response getTriggerStatus(
            @PathParam("namespace") String namespace, @PathParam("table") String table) {
        LOG.debug("Getting trigger status for table: {}.{}", namespace, table);

        String catalog = catalogClient.getCatalogName();
        TableIdentifier tableIdentifier = TableIdentifier.of(catalog, namespace, table);

        // Check if table exists
        Optional<Table> tableOpt = catalogClient.loadTable(tableIdentifier);
        if (tableOpt.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("Table", namespace + "." + table))
                    .build();
        }

        // Find matching policy
        Optional<MaintenancePolicy> policyOpt =
                policyMatcher.findEffectivePolicy(catalog, tableIdentifier);

        boolean conditionBasedEnabled = schedulerConfig.conditionBasedTriggeringEnabled();
        Map<String, OperationTriggerStatus> operationStatuses = new java.util.LinkedHashMap<>();

        if (policyOpt.isEmpty()) {
            // No policy - all operations show as "no policy"
            for (OperationType opType : OperationType.values()) {
                operationStatuses.put(
                        opType.name(), OperationTriggerStatus.noPolicy(opType.name()));
            }

            return Response.ok(
                            new TriggerStatusResponse(
                                    catalog,
                                    namespace,
                                    table,
                                    null,
                                    conditionBasedEnabled,
                                    operationStatuses))
                    .build();
        }

        MaintenancePolicy policy = policyOpt.get();
        TriggerConditions conditions = policy.triggerConditions();

        // Get health report for evaluation
        HealthReport health = null;
        try {
            HealthThresholds thresholds = policy.effectiveThresholds();
            TableHealthAssessor.ScanMode scanMode =
                    TableHealthAssessor.ScanMode.fromString(healthConfig.scanMode());
            HealthCacheKey cacheKey =
                    HealthCacheKey.of(
                            tableIdentifier, scanMode, healthConfig.sampleLimit(), thresholds);
            Optional<HealthReport> cachedReport = healthReportCache.getIfFresh(cacheKey);
            if (cachedReport.isPresent()) {
                health = cachedReport.get();
            } else {
                TableHealthAssessor assessor =
                        new TableHealthAssessor(thresholds, scanMode, healthConfig.sampleLimit());
                health = assessor.assess(tableIdentifier, tableOpt.get());
                healthReportCache.put(cacheKey, health);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get health report for trigger status: {}", e.getMessage());
        }

        TriggerEvaluator evaluator = new TriggerEvaluator();
        Instant now = Instant.now();

        // Evaluate each operation type
        for (OperationType opType : OperationType.values()) {
            boolean enabled = policy.isOperationEnabled(opType);

            if (!enabled) {
                operationStatuses.put(
                        opType.name(), OperationTriggerStatus.disabled(opType.name()));
                continue;
            }

            // Get last operation time for this type
            Instant lastOpTime =
                    operationStore
                            .findLastOperationTime(catalog, namespace, table, opType.name())
                            .orElse(null);

            // Evaluate conditions
            TriggerEvaluator.EvaluationResult result;
            if (!conditionBasedEnabled || conditions == null) {
                result = TriggerEvaluator.EvaluationResult.alwaysTrigger();
            } else {
                result = evaluator.evaluate(conditions, health, lastOpTime, opType, now);
            }

            operationStatuses.put(
                    opType.name(), OperationTriggerStatus.from(opType.name(), result, enabled));
        }

        return Response.ok(
                        new TriggerStatusResponse(
                                catalog,
                                namespace,
                                table,
                                policy.name(),
                                conditionBasedEnabled,
                                operationStatuses))
                .build();
    }

    /** Table summary. */
    private record TableSummary(
            String catalog, String namespace, String name, String qualifiedName) {
        public static TableSummary from(TableIdentifier id) {
            return new TableSummary(id.catalog(), id.namespace(), id.table(), id.toQualifiedName());
        }
    }
}
