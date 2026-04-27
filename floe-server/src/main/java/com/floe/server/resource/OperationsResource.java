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

package com.floe.server.resource;

import com.floe.core.auth.Permission;
import com.floe.core.operation.*;
import com.floe.server.api.ErrorResponse;
import com.floe.server.api.OperationResponse;
import com.floe.server.api.StatsResponse;
import com.floe.server.api.validation.PaginationValidator;
import com.floe.server.auth.Secured;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST resource for querying maintenance operation history. */
@Path("/api/v1/operations")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OperationsResource {

    private static final Logger LOG = LoggerFactory.getLogger(OperationsResource.class);

    @Inject OperationStore operationStore;

    /**
     * List recent operations with pagination.
     *
     * <p>GET /api/v1/operations GET /api/v1/operations?limit=50&offset=0 GET
     * /api/v1/operations?status=FAILED&limit=20
     *
     * @param limit maximum number of operations to return (default 20)
     * @param offset number of operations to skip for pagination (default 0)
     * @param status optional status filter (PENDING, RUNNING, COMPLETED, FAILED)
     * @return 200 OK with paginated list of operations
     */
    @GET
    @Secured(Permission.READ_OPERATIONS)
    public Response listOperations(
            @QueryParam("limit") @DefaultValue("20") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset,
            @QueryParam("status") String status) {
        LOG.debug("Listing operations: limit={}, offset={}, status={}", limit, offset, status);

        // Validate pagination params
        Optional<Response> validationError = PaginationValidator.validate(limit, offset);
        if (validationError.isPresent()) {
            return validationError.get();
        }

        try {
            List<OperationRecord> operations;
            long total;

            if (status != null && !status.isBlank()) {
                OperationStatus opStatus = parseStatus(status);
                if (opStatus == null) {
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity(
                                    ErrorResponse.validation(
                                            "Invalid status: "
                                                    + status
                                                    + ". Valid values: "
                                                    + Arrays.toString(OperationStatus.values())))
                            .build();
                }
                operations = operationStore.findByStatus(opStatus, limit, offset);
                total = operationStore.countByStatus(opStatus);
            } else {
                operations = operationStore.findRecent(limit, offset);
                total = operationStore.count();
            }

            List<OperationResponse> responses =
                    operations.stream().map(OperationResponse::from).toList();
            boolean hasMore = offset + responses.size() < total;

            return Response.ok(
                            Map.of(
                                    "operations",
                                    responses,
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
            LOG.error("Failed to list operations", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * Get operation by ID.
     *
     * <p>GET /api/v1/operations/{id}
     *
     * @param id the operation UUID
     * @return 200 OK with the operation, 400 Bad Request for invalid UUID, or 404 Not Found
     */
    @GET
    @Path("/{id}")
    @Secured(Permission.READ_OPERATIONS)
    public Response getById(@PathParam("id") String id) {
        LOG.debug("Getting operation: {}", id);

        try {
            UUID uuid = UUID.fromString(id);
            Optional<OperationRecord> recordOpt = operationStore.findById(uuid);

            if (recordOpt.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(ErrorResponse.notFound("Operation", id))
                        .build();
            }

            return Response.ok(OperationResponse.from(recordOpt.get())).build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ErrorResponse.validation("Invalid UUID: " + id))
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to get operation: {}", id, e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * Get operation statistics.
     *
     * <p>GET /api/v1/operations/stats GET /api/v1/operations/stats?window=24h GET
     * /api/v1/operations/stats?window=7d
     *
     * @param window time window for statistics (e.g., "24h", "7d", "1h")
     * @return 200 OK with aggregated statistics, or 400 Bad Request for invalid window format
     */
    @GET
    @Path("/stats")
    @Secured(Permission.READ_OPERATIONS)
    public Response getStats(@QueryParam("window") @DefaultValue("24h") String window) {
        LOG.debug("Getting operation stats: window={}", window);

        try {
            Duration duration = parseWindow(window);
            if (duration == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(
                                ErrorResponse.validation(
                                        "Invalid window format: "
                                                + window
                                                + ". Use format like '24h', '7d', '1h'"))
                        .build();
            }

            OperationStats stats = operationStore.getStats(duration);
            return Response.ok(StatsResponse.from(stats)).build();
        } catch (Exception e) {
            LOG.error("Failed to get operation stats", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * Get currently running operations.
     *
     * <p>GET /api/v1/operations/running
     *
     * @param limit maximum number of operations to return (default 50)
     * @param offset number of operations to skip for pagination (default 0)
     * @return 200 OK with paginated list of running operations
     */
    @GET
    @Path("/running")
    @Secured(Permission.READ_OPERATIONS)
    public Response getRunning(
            @QueryParam("limit") @DefaultValue("50") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        LOG.debug("Getting running operations: limit={}, offset={}", limit, offset);

        try {
            List<OperationRecord> running =
                    operationStore.findByStatus(OperationStatus.RUNNING, limit, offset);
            long total = operationStore.countByStatus(OperationStatus.RUNNING);
            List<OperationResponse> responses =
                    running.stream().map(OperationResponse::from).toList();
            boolean hasMore = offset + responses.size() < total;

            return Response.ok(
                            Map.of(
                                    "operations",
                                    responses,
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
            LOG.error("Failed to get running operations", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * Get operations for a specific table.
     *
     * <p>GET /api/v1/operations/table/{catalog}/{namespace}/{table}
     *
     * @param catalog the catalog name
     * @param namespace the namespace name
     * @param table the table name
     * @param limit maximum number of operations to return (default 20)
     * @return 200 OK with list of operations for the table
     */
    @GET
    @Path("/table/{catalog}/{namespace}/{table}")
    @Secured(Permission.READ_OPERATIONS)
    public Response getByTable(
            @PathParam("catalog") String catalog,
            @PathParam("namespace") String namespace,
            @PathParam("table") String table,
            @QueryParam("limit") @DefaultValue("20") int limit) {
        LOG.debug(
                "Getting operations for table: {}.{}.{}, limit={}",
                catalog,
                namespace,
                table,
                limit);

        try {
            List<OperationRecord> operations =
                    operationStore.findByTable(catalog, namespace, table, limit);
            List<OperationResponse> responses =
                    operations.stream().map(OperationResponse::from).toList();

            return Response.ok(
                            Map.of(
                                    "operations",
                                    responses,
                                    "total",
                                    responses.size(),
                                    "table",
                                    Map.of(
                                            "catalog",
                                            catalog,
                                            "namespace",
                                            namespace,
                                            "name",
                                            table,
                                            "qualified",
                                            catalog + "." + namespace + "." + table)))
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to get operations for table {}.{}.{}", catalog, namespace, table, e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    // Helper methods

    /**
     * Parse a status string to OperationStatus enum.
     *
     * @param status the status string to parse
     * @return the OperationStatus, or null if invalid
     */
    private OperationStatus parseStatus(String status) {
        try {
            return OperationStatus.valueOf(status.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Parse a time window string to Duration.
     *
     * @param window the window string (e.g., "24h", "7d", "30m")
     * @return the Duration, or null if invalid format
     */
    private Duration parseWindow(String window) {
        if (window == null || window.isBlank()) {
            return Duration.ofHours(24);
        }

        try {
            String lower = window.toLowerCase(Locale.ROOT).trim();
            if (lower.endsWith("h")) {
                int hours = Integer.parseInt(lower.substring(0, lower.length() - 1));
                return Duration.ofHours(hours);
            } else if (lower.endsWith("d")) {
                int days = Integer.parseInt(lower.substring(0, lower.length() - 1));
                return Duration.ofDays(days);
            } else if (lower.endsWith("m")) {
                int minutes = Integer.parseInt(lower.substring(0, lower.length() - 1));
                return Duration.ofMinutes(minutes);
            } else {
                // Try parsing as hours
                return Duration.ofHours(Integer.parseInt(lower));
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
