package com.floe.server.resource;

import com.floe.core.auth.*;
import com.floe.server.api.*;
import com.floe.server.auth.*;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST resource for managing API keys. */
@Path("/api/v1/auth/keys")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AuthResource {

    private static final Logger LOG = LoggerFactory.getLogger(AuthResource.class);

    @Inject ApiKeyStore apiKeyStore;

    @Context SecurityContext securityContext;

    /**
     * Create a new API key.
     *
     * @param request the API key creation request
     * @return 201 Created with the new API key (plaintext key only shown once), or 409 Conflict
     */
    @POST
    @Secured(Permission.MANAGE_API_KEYS)
    public Response create(@Valid ApiKeyRequest request) {
        LOG.info("Creating API key: {}", request.name());

        if (apiKeyStore.existsByName(request.name())) {
            LOG.warn("API key with name '{}' already exists", request.name());
            return Response.status(Response.Status.CONFLICT)
                    .entity(
                            ErrorResponse.conflict(
                                    "API key with name '" + request.name() + "' already exists"))
                    .build();
        }

        try {
            // Generate a new key
            String plaintextKey = ApiKeyGenerator.generateKey();
            String keyHash = ApiKeyGenerator.hashKey(plaintextKey);

            // Get the creating user's ID
            String createdBy = null;
            if (securityContext.getUserPrincipal() instanceof FloePrincipal principal) {
                createdBy = principal.userId();
            }

            ApiKey apiKey =
                    ApiKey.builder()
                            .keyHash(keyHash)
                            .name(request.name())
                            .role(request.role())
                            .expiresAt(request.expiresAt())
                            .createdBy(createdBy)
                            .build();

            apiKeyStore.save(apiKey);

            LOG.info(
                    "Created API key: {} ({}) with role {}",
                    apiKey.name(),
                    apiKey.id(),
                    apiKey.role());

            // Return the plaintext key - this is the only time it's available
            return Response.created(URI.create("/api/v1/auth/keys/" + apiKey.id()))
                    .entity(CreateApiKeyResponse.from(plaintextKey, apiKey))
                    .build();
        } catch (IllegalArgumentException e) {
            LOG.warn("Invalid API key request: {}", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ErrorResponse.validation(e.getMessage()))
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to create API key", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * List all API keys.
     *
     * @param enabledOnly if true, only return enabled API keys
     * @return 200 OK with list of API keys (without plaintext key values)
     */
    @GET
    @Secured(Permission.MANAGE_API_KEYS)
    public Response list(@QueryParam("enabled") Boolean enabledOnly) {
        LOG.debug("Listing API keys, enabledOnly={}", enabledOnly);

        try {
            List<ApiKey> keys;
            if (Boolean.TRUE.equals(enabledOnly)) {
                keys = apiKeyStore.listEnabled();
            } else {
                keys = apiKeyStore.listAll();
            }

            List<ApiKeyResponse> responses = keys.stream().map(ApiKeyResponse::from).toList();

            return Response.ok(Map.of("keys", responses, "total", responses.size())).build();
        } catch (Exception e) {
            LOG.error("Failed to list API keys", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * Get an API key by ID.
     *
     * @param id the API key ID
     * @return 200 OK with the API key details, or 404 Not Found
     */
    @GET
    @Path("/{id}")
    @Secured(Permission.MANAGE_API_KEYS)
    public Response getById(@PathParam("id") String id) {
        LOG.debug("Getting API key: {}", id);

        Optional<ApiKey> keyOpt = apiKeyStore.findById(id);

        if (keyOpt.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("API key", id))
                    .build();
        }

        return Response.ok(ApiKeyResponse.from(keyOpt.get())).build();
    }

    /**
     * Update an API key.
     *
     * @param id the API key ID
     * @param request the update request with fields to modify
     * @return 200 OK with the updated API key, 404 Not Found, or 409 Conflict
     */
    @PUT
    @Path("/{id}")
    @Secured(Permission.MANAGE_API_KEYS)
    public Response update(@PathParam("id") String id, @Valid UpdateApiKeyRequest request) {
        LOG.info("Updating API key: {}", id);

        Optional<ApiKey> existingOpt = apiKeyStore.findById(id);

        if (existingOpt.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("API key", id))
                    .build();
        }

        ApiKey existing = existingOpt.get();

        // Check if name is being changed to an existing name
        if (request.name() != null && !request.name().equals(existing.name())) {
            if (apiKeyStore.existsByName(request.name())) {
                return Response.status(Response.Status.CONFLICT)
                        .entity(
                                ErrorResponse.conflict(
                                        "API key with name '"
                                                + request.name()
                                                + "' already exists"))
                        .build();
            }
        }

        try {
            ApiKey updated = existing;

            if (request.name() != null) {
                updated = updated.withName(request.name());
            }
            if (request.role() != null) {
                updated = updated.withRole(request.role());
            }
            if (request.enabled() != null) {
                updated = updated.withEnabled(request.enabled());
            }

            apiKeyStore.save(updated);

            LOG.info("Updated API key: {} ({})", updated.name(), updated.id());

            return Response.ok(ApiKeyResponse.from(updated)).build();
        } catch (IllegalArgumentException e) {
            LOG.warn("Invalid update request: {}", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ErrorResponse.validation(e.getMessage()))
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to update API key", e);
            return Response.serverError().entity(ErrorResponse.internal(e)).build();
        }
    }

    /**
     * Delete (revoke) an API key.
     *
     * @param id the API key ID
     * @return 204 No Content on success, 400 Bad Request if deleting own key, or 404 Not Found
     */
    @DELETE
    @Path("/{id}")
    @Secured(Permission.MANAGE_API_KEYS)
    public Response delete(@PathParam("id") String id) {
        LOG.info("Deleting API key: {}", id);

        // Prevent deleting your own key
        if (securityContext.getUserPrincipal() instanceof FloePrincipal principal) {
            if (principal.userId().equals(id)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(ErrorResponse.validation("Cannot delete your own API key"))
                        .build();
            }
        }

        if (!apiKeyStore.existsById(id)) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("API key", id))
                    .build();
        }

        if (apiKeyStore.deleteById(id)) {
            LOG.info("Deleted API key: {}", id);
            return Response.noContent().build();
        } else {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(ErrorResponse.notFound("API key", id))
                    .build();
        }
    }

    /**
     * Get information about the current API key (who am I).
     *
     * @return 200 OK with the current user's API key or principal info, or 401 Unauthorized
     */
    @GET
    @Path("/me")
    @Secured(Permission.READ_POLICIES) // Any authenticated user can access
    public Response me() {
        if (securityContext.getUserPrincipal() instanceof FloePrincipal principal) {
            // If authenticated via API key, fetch and return the full API key details
            if ("API_KEY".equals(principal.authenticationMethod())) {
                Optional<ApiKey> apiKeyOpt = apiKeyStore.findById(principal.userId());
                if (apiKeyOpt.isPresent()) {
                    return Response.ok(ApiKeyResponse.from(apiKeyOpt.get())).build();
                }
            }

            // For other auth methods (OIDC, etc.), return basic principal info
            return Response.ok(
                            Map.of(
                                    "userId",
                                    principal.userId(),
                                    "username",
                                    principal.username(),
                                    "roles",
                                    principal.roles(),
                                    "authMethod",
                                    principal.authenticationMethod()))
                    .build();
        }

        return Response.status(Response.Status.UNAUTHORIZED)
                .entity(ErrorResponse.unauthorized("Not authenticated"))
                .build();
    }
}
