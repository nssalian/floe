package com.floe.server.resource;

import com.floe.core.auth.Permission;
import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.TablePattern;
import com.floe.server.api.CreatePolicyRequest;
import com.floe.server.api.ErrorResponse;
import com.floe.server.api.PolicyResponse;
import com.floe.server.api.UpdatePolicyRequest;
import com.floe.server.api.validation.PaginationValidator;
import com.floe.server.auth.Secured;
import com.floe.server.metrics.FloeMetrics;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST resource for managing maintenance policies for tables. */
@Path("/api/v1/policies")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PolicyResource {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyResource.class);

    @Inject PolicyStore policyStore;

    @Inject CatalogClient catalogClient;

    @Inject FloeMetrics metrics;

    /**
     * Create a new maintenance policy.
     *
     * @param request the policy creation request
     * @return 201 Created with the new policy, 409 Conflict if name exists, or 400 Bad Request
     */
    @POST
    @Secured(Permission.WRITE_POLICIES)
    public Response create(@Valid CreatePolicyRequest request) {
        LOG.info("Creating policy: {}", request.name());

        if (policyStore.existsByName(request.name())) {
            LOG.warn("Policy with name '{}' already exists", request.name());
            return Response.status(Response.Status.CONFLICT)
                    .entity(
                            ErrorResponse.conflict(
                                    "Policy with name '" + request.name() + "' already exists"))
                    .build();
        }

        try {
            MaintenancePolicy policy = request.toPolicy();

            // Validate table pattern matches existing tables
            TableValidationResult validation = validateTablePattern(policy.tablePattern());
            if (!validation.valid()) {
                LOG.warn(
                        "Table validation failed for policy '{}': {}",
                        request.name(),
                        validation.message());
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(
                                ErrorResponse.validation(
                                        validation.message(), Map.copyOf(validation.details())))
                        .build();
            }

            policyStore.save(policy);
            metrics.recordPolicyCreated();
            metrics.setActivePoliciesCount(policyStore.listEnabled().size());

            LOG.info(
                    "Created policy: {} ({}) - matches {} table(s)",
                    policy.name(),
                    policy.id(),
                    validation.matchedTables().size());

            return Response.created(URI.create("/api/v1/policies/" + policy.id()))
                    .entity(PolicyResponse.from(policy))
                    .build();
        } catch (IllegalArgumentException e) {
            LOG.warn("Invalid policy request: {}", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ErrorResponse.validation(e.getMessage()))
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to create policy", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * List all policies with pagination.
     *
     * @param enabledOnly if true, only return enabled policies
     * @param limit maximum number of policies to return (default 20)
     * @param offset number of policies to skip for pagination (default 0)
     * @return 200 OK with paginated list of policies
     */
    @GET
    @Secured(Permission.READ_POLICIES)
    public Response list(
            @QueryParam("enabled") Boolean enabledOnly,
            @QueryParam("limit") @DefaultValue("20") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        LOG.debug(
                "Listing policies, enabledOnly={}, limit={}, offset={}",
                enabledOnly,
                limit,
                offset);

        // Validate pagination params
        Optional<Response> validationError = PaginationValidator.validate(limit, offset);
        if (validationError.isPresent()) {
            return validationError.get();
        }

        try {
            List<MaintenancePolicy> paginatedPolicies;
            int total;
            if (Boolean.TRUE.equals(enabledOnly)) {
                paginatedPolicies = policyStore.listEnabled(limit, offset);
                total = policyStore.countEnabled();
            } else {
                paginatedPolicies = policyStore.listAll(limit, offset);
                total = policyStore.count();
            }

            List<PolicyResponse> responses =
                    paginatedPolicies.stream().map(PolicyResponse::from).toList();

            boolean hasMore = offset + responses.size() < total;

            return Response.ok(
                            Map.of(
                                    "policies",
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
            LOG.error("Failed to list policies", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * Get a policy by ID.
     *
     * @param id the policy ID
     * @return 200 OK with the policy, or 404 Not Found
     */
    @GET
    @Path("/{id}")
    @Secured(Permission.READ_POLICIES)
    public Response getById(@PathParam("id") String id) {
        LOG.debug("Getting policy: {}", id);

        Optional<MaintenancePolicy> policyOpt = policyStore.getById(id);

        if (policyOpt.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("Policy", id))
                    .build();
        }

        return Response.ok(PolicyResponse.from(policyOpt.get())).build();
    }

    /**
     * Update a policy.
     *
     * @param id the policy ID
     * @param request the update request with fields to modify
     * @return 200 OK with the updated policy, 404 Not Found, or 409 Conflict
     */
    @PUT
    @Path("/{id}")
    @Secured(Permission.WRITE_POLICIES)
    public Response update(@PathParam("id") String id, @Valid UpdatePolicyRequest request) {
        LOG.info("Updating policy: {}", id);

        Optional<MaintenancePolicy> existingOpt = policyStore.getById(id);

        if (existingOpt.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("Policy", id))
                    .build();
        }

        MaintenancePolicy existing = existingOpt.get();

        // Check if name is being changed to an existing name
        if (request.name() != null && !request.name().equals(existing.name())) {
            if (policyStore.existsByName(request.name())) {
                return Response.status(Response.Status.CONFLICT)
                        .entity(
                                ErrorResponse.conflict(
                                        "Policy with name '" + request.name() + "' already exists"))
                        .build();
            }
        }

        try {
            MaintenancePolicy updated = request.applyTo(existing);

            // Validate table pattern if it changed
            if (request.tablePattern() != null) {
                TableValidationResult validation = validateTablePattern(updated.tablePattern());
                if (!validation.valid()) {
                    LOG.warn(
                            "Table validation failed for policy '{}': {}",
                            id,
                            validation.message());
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity(
                                    ErrorResponse.validation(
                                            validation.message(), Map.copyOf(validation.details())))
                            .build();
                }
                LOG.info(
                        "Updated policy {} table pattern - matches {} table(s)",
                        id,
                        validation.matchedTables().size());
            }

            policyStore.save(updated);
            metrics.recordPolicyUpdated();
            metrics.setActivePoliciesCount(policyStore.listEnabled().size());

            LOG.info("Updated policy: {} ({})", updated.name(), updated.id());

            return Response.ok(PolicyResponse.from(updated)).build();
        } catch (IllegalArgumentException e) {
            LOG.warn("Invalid update request: {}", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ErrorResponse.validation(e.getMessage()))
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to update policy", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * Delete a policy.
     *
     * @param id the policy ID
     * @return 204 No Content on success, or 404 Not Found
     */
    @DELETE
    @Path("/{id}")
    @Secured(Permission.DELETE_POLICIES)
    public Response delete(@PathParam("id") String id) {
        LOG.info("Deleting policy: {}", id);

        if (!policyStore.existsById(id)) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("Policy", id))
                    .build();
        }

        if (policyStore.deleteById(id)) {
            metrics.recordPolicyDeleted();
            metrics.setActivePoliciesCount(policyStore.listEnabled().size());
            LOG.info("Deleted policy: {}", id);
            return Response.noContent().build();
        } else {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("Policy", id))
                    .build();
        }
    }

    /**
     * Validate tables matching a pattern (for UI preview).
     *
     * @param request map containing "pattern" key with the table pattern to validate
     * @return 200 OK with validation results including matched tables
     */
    @POST
    @Path("/validate-pattern")
    public Response validatePattern(Map<String, String> request) {
        String pattern = request.get("pattern");
        if (pattern == null || pattern.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ErrorResponse.validation("Pattern is required"))
                    .build();
        }

        try {
            TablePattern tablePattern = TablePattern.parse(pattern);
            TableValidationResult validation = validateTablePattern(tablePattern);

            return Response.ok(
                            Map.of(
                                    "valid",
                                    validation.valid(),
                                    "message",
                                    validation.message(),
                                    "matchedTables",
                                    validation.matchedTables().stream()
                                            .map(TableIdentifier::toQualifiedName)
                                            .toList(),
                                    "matchCount",
                                    validation.matchedTables().size()))
                    .build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ErrorResponse.validation("Invalid pattern: " + e.getMessage()))
                    .build();
        }
    }

    // Table Validation

    /**
     * Validate that a table pattern matches at least one existing table. For exact patterns (no
     * wildcards), the specific Iceberg table must exist. For wildcard patterns, at least one
     * matching table should exist (warning if none).
     */
    private TableValidationResult validateTablePattern(TablePattern pattern) {
        if (pattern == null) {
            return new TableValidationResult(
                    true, "No table pattern specified (matches all tables)", List.of(), Map.of());
        }

        // Check if catalog is healthy - fail validation if catalog is unavailable
        if (!catalogClient.isHealthy()) {
            LOG.warn("Catalog is not healthy, rejecting policy creation");
            return new TableValidationResult(
                    false,
                    "Catalog unavailable - cannot validate table pattern",
                    List.of(),
                    Map.of("error", "Catalog connection failed. Check catalog configuration."));
        }

        try {
            List<TableIdentifier> allTables = catalogClient.listAllTables();
            List<TableIdentifier> matchedTables = new ArrayList<>();

            String catalogName = catalogClient.getCatalogName();

            for (TableIdentifier table : allTables) {
                if (pattern.matches(catalogName, table)) {
                    matchedTables.add(table);
                }
            }

            boolean isExactPattern = isExactPattern(pattern);

            if (matchedTables.isEmpty()) {
                if (isExactPattern) {
                    // Exact pattern with no matches = error
                    String patternStr = pattern.toString();
                    return new TableValidationResult(
                            false,
                            "Table not found: " + patternStr,
                            List.of(),
                            Map.of(
                                    "pattern",
                                    patternStr,
                                    "suggestion",
                                    "Verify the table exists in the catalog"));
                } else {
                    // Wildcard pattern with no matches = warning but allow
                    return new TableValidationResult(
                            true,
                            "Warning: No tables currently match pattern '" + pattern + "'",
                            List.of(),
                            Map.of(
                                    "warning",
                                    "Policy will have no effect until matching tables exist"));
                }
            }

            return new TableValidationResult(
                    true,
                    "Pattern matches " + matchedTables.size() + " table(s)",
                    matchedTables,
                    Map.of());
        } catch (Exception e) {
            LOG.error("Error validating table pattern: {}", e.getMessage(), e);
            // Don't block policy creation on validation errors
            return new TableValidationResult(
                    true,
                    "Validation error (policy allowed): " + e.getMessage(),
                    List.of(),
                    Map.of("warning", "Could not validate table pattern"));
        }
    }

    /** Check if a pattern is exact (no wildcards) vs a wildcard pattern. */
    private boolean isExactPattern(TablePattern pattern) {
        String patternStr = pattern.toString();
        return !patternStr.contains("*") && !patternStr.contains("?");
    }

    /** Result of table pattern validation. */
    private record TableValidationResult(
            boolean valid,
            String message,
            List<TableIdentifier> matchedTables,
            Map<String, String> details) {}
}
