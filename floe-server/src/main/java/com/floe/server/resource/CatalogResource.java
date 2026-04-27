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
import com.floe.core.catalog.CatalogConfig;
import com.floe.core.catalog.CatalogConfigStore;
import com.floe.server.api.ErrorResponse;
import com.floe.server.auth.Secured;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST resource for catalog configuration.
 *
 * <p>Exposes non-sensitive catalog configuration for display in the UI. Credentials are NEVER
 * exposed through this API.
 */
@Path("/api/v1/catalog")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CatalogResource {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogResource.class);

    @Inject CatalogConfigStore catalogConfigStore;

    /**
     * Get the currently active catalog configuration.
     *
     * @return 200 OK with the active catalog config (excluding credentials), or 404 Not Found
     */
    @GET
    @Secured(Permission.READ_TABLES)
    public Response getActiveCatalog() {
        LOG.debug("Getting active catalog config");

        return catalogConfigStore
                .findActive()
                .map(config -> Response.ok(CatalogConfigResponse.from(config)).build())
                .orElseGet(
                        () ->
                                Response.status(Response.Status.NOT_FOUND)
                                        .entity(ErrorResponse.notFound("Catalog", "active"))
                                        .build());
    }

    /** Response DTO for catalog configuration. Contains only non-sensitive information. */
    public record CatalogConfigResponse(
            UUID id,
            String name,
            String type,
            String uri,
            String warehouse,
            Map<String, String> properties,
            Instant createdAt,
            Instant updatedAt,
            boolean active) {
        public static CatalogConfigResponse from(CatalogConfig config) {
            return new CatalogConfigResponse(
                    config.id(),
                    config.name(),
                    config.type(),
                    config.uri(),
                    config.warehouse(),
                    config.properties(),
                    config.createdAt(),
                    config.updatedAt(),
                    config.active());
        }
    }
}
