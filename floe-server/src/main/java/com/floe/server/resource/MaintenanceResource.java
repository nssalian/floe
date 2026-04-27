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
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.operation.OperationStatus;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.server.api.TriggerRequest;
import com.floe.server.api.TriggerResponse;
import com.floe.server.auth.Secured;
import com.floe.server.health.HealthReportCache;
import com.floe.server.metrics.FloeMetrics;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST resource for triggering Iceberg table maintenance operations. */
@Path("/api/v1/maintenance")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MaintenanceResource {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceResource.class);

    @Inject MaintenanceOrchestrator orchestrator;

    @Inject FloeMetrics metrics;

    @Inject HealthReportCache healthReportCache;

    /**
     * Trigger policy-driven maintenance for a table.
     *
     * <p>If policyName is provided, that specific policy will be used. Otherwise, the system finds
     * the best matching policy for the table.
     *
     * <p>Example requests:
     *
     * <pre>
     * // Trigger using best matching policy
     * POST /api/v1/maintenance/trigger
     * {
     *   "catalog": "demo",
     *   "namespace": "test",
     *   "table": "events"
     * }
     *
     * // Trigger using specific policy
     * POST /api/v1/maintenance/trigger
     * {
     *   "catalog": "demo",
     *   "namespace": "test",
     *   "table": "events",
     *   "policyName": "events-compaction"
     * }
     * </pre>
     *
     * @param request the trigger request containing catalog, namespace, table, and optional policy
     * @return 200 OK on success, 202 Accepted if running async, 404 if no matching policy, or 500
     *     on failure
     */
    @POST
    @Path("/trigger")
    @Secured(Permission.TRIGGER_MAINTENANCE)
    public Response trigger(@Valid TriggerRequest request) {
        TableIdentifier tableId =
                TableIdentifier.of(request.catalog(), request.namespace(), request.table());

        if (request.hasSpecificPolicy()) {
            LOG.info(
                    "Triggering maintenance for {} using policy '{}'",
                    tableId.toQualifiedName(),
                    request.policyName());
        } else {
            LOG.info(
                    "Triggering maintenance for {} using best matching policy",
                    tableId.toQualifiedName());
        }

        try {
            // Record maintenance trigger
            metrics.recordMaintenanceTriggered();
            metrics.incrementRunningOperations();

            healthReportCache.invalidateTable(
                    request.catalog(), request.namespace(), request.table());

            OrchestratorResult result;

            if (request.hasSpecificPolicy()) {
                result =
                        orchestrator.runMaintenanceWithPolicy(
                                request.catalog(), tableId, request.policyName());
            } else {
                result = orchestrator.runMaintenance(request.catalog(), tableId);
            }

            // Record result metrics
            metrics.decrementRunningOperations();
            metrics.recordMaintenanceResult(
                    OperationStatus.fromOrchestratorStatus(result.status()));

            // Record policy match metrics
            if (result.policyName() != null) {
                metrics.recordPolicyMatch(result.policyName());
            } else if (result.status() == OrchestratorResult.Status.NO_POLICY) {
                metrics.recordNoMatchingPolicy();
            }

            TriggerResponse response = TriggerResponse.from(result);

            return switch (result.status()) {
                case SUCCESS, PARTIAL_FAILURE, NO_OPERATIONS -> Response.ok(response).build();
                case NO_POLICY ->
                        Response.status(Response.Status.NOT_FOUND).entity(response).build();
                case FAILED ->
                        Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                .entity(response)
                                .build();
                case RUNNING -> Response.accepted(response).build();
            };
        } catch (IllegalArgumentException e) {
            metrics.decrementRunningOperations();
            LOG.warn("Invalid trigger request: {}", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse(e.getMessage()))
                    .build();
        } catch (Exception e) {
            metrics.decrementRunningOperations();
            LOG.error("Failed to trigger maintenance", e);
            return Response.serverError()
                    .entity(new ErrorResponse("Internal error: " + e.getMessage()))
                    .build();
        }
    }

    record ErrorResponse(String error) {}
}
