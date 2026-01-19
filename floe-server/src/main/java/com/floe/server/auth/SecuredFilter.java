package com.floe.server.auth;

import com.floe.core.auth.AuthorizationProvider;
import com.floe.core.auth.AuthorizationProvider.AuthorizationRequest;
import com.floe.core.auth.AuthorizationProvider.AuthorizationResult;
import com.floe.core.auth.FloePrincipal;
import com.floe.core.auth.Permission;
import com.floe.server.config.FloeConfig;
import com.floe.server.config.FloeConfigProvider;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.ext.Provider;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JAX-RS filter that enforces @Secured annotation permissions.
 *
 * <p>Intercepts requests to endpoints annotated with @Secured and checks that the authenticated
 * user has at least one of the required permissions.
 *
 * <p>Returns 401 if not authenticated, 403 if not authorized.
 */
@Provider
@Secured({})
@Priority(Priorities.AUTHORIZATION)
public class SecuredFilter implements ContainerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(SecuredFilter.class);

    @Context ResourceInfo resourceInfo;

    @Context SecurityContext securityContext;

    // Lazy-initialized to avoid CDI issues at construction time
    private AuthorizationProvider authorizationProvider;
    private FloeConfig floeConfig;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        LOG.debug("SecuredFilter invoked for: {}", requestContext.getUriInfo().getPath());
        // Get config first (minimal CDI lookup)
        if (floeConfig == null) {
            FloeConfigProvider provider = CDI.current().select(FloeConfigProvider.class).get();
            floeConfig = provider.get();
        }

        // Skip if auth is disabled - before other CDI lookups
        if (!floeConfig.auth().enabled()) {
            return;
        }

        // Only lookup AuthorizationProvider if auth is enabled
        if (authorizationProvider == null) {
            authorizationProvider = CDI.current().select(AuthorizationProvider.class).get();
        }

        // Get @Secured annotation from method or class
        Permission[] requiredPermissions = getRequiredPermissions();
        if (requiredPermissions == null || requiredPermissions.length == 0) {
            return; // No permissions required
        }

        // Check authentication
        if (securityContext.getUserPrincipal() == null) {
            LOG.debug(
                    "Unauthenticated request to secured endpoint: {}",
                    requestContext.getUriInfo().getPath());
            requestContext.abortWith(
                    Response.status(Response.Status.UNAUTHORIZED)
                            .entity("{\"error\":\"Authentication required\"}")
                            .type("application/json")
                            .build());
            return;
        }

        // Get FloePrincipal from security context
        FloePrincipal principal = getFloePrincipal(requestContext);
        if (principal == null) {
            LOG.warn("Authenticated but no FloePrincipal found");
            requestContext.abortWith(
                    Response.status(Response.Status.FORBIDDEN)
                            .entity("{\"error\":\"Invalid authentication state\"}")
                            .type("application/json")
                            .build());
            return;
        }

        // Check if user has at least one of the required permissions
        String resource = requestContext.getUriInfo().getPath();
        String method = requestContext.getMethod();

        for (Permission permission : requiredPermissions) {
            String action = permissionToAction(permission);
            AuthorizationRequest authzRequest =
                    AuthorizationRequest.of(principal.userId(), action, "endpoint", resource);

            AuthorizationResult result = authorizationProvider.authorize(authzRequest);
            if (result.allowed()) {
                LOG.debug(
                        "Authorized: user={} permission={} resource={}",
                        principal.username(),
                        permission,
                        resource);
                return; // Allowed - user has this permission
            }
        }

        // None of the required permissions matched
        LOG.info(
                "Access denied: user={} required={} resource={} method={}",
                principal.username(),
                requiredPermissions,
                resource,
                method);
        requestContext.abortWith(
                Response.status(Response.Status.FORBIDDEN)
                        .entity("{\"error\":\"Insufficient permissions\"}")
                        .type("application/json")
                        .build());
    }

    /** Gets required permissions from @Secured annotation on method or class. */
    private Permission[] getRequiredPermissions() {
        Method method = resourceInfo.getResourceMethod();
        if (method != null) {
            Secured secured = method.getAnnotation(Secured.class);
            if (secured != null) {
                return secured.value();
            }
        }

        Class<?> resourceClass = resourceInfo.getResourceClass();
        if (resourceClass != null) {
            Secured secured = resourceClass.getAnnotation(Secured.class);
            if (secured != null) {
                return secured.value();
            }
        }

        return null;
    }

    /** Extracts FloePrincipal from Quarkus SecurityIdentity. */
    private FloePrincipal getFloePrincipal(ContainerRequestContext requestContext) {
        try {
            SecurityIdentity identity = CDI.current().select(SecurityIdentity.class).get();
            Object principal = identity.getAttribute("floePrincipal");
            if (principal instanceof FloePrincipal) {
                return (FloePrincipal) principal;
            }
        } catch (Exception e) {
            LOG.debug("Could not get FloePrincipal from SecurityIdentity: {}", e.getMessage());
        }
        return null;
    }

    /** Maps Permission enum to action string for AuthorizationProvider. */
    private String permissionToAction(Permission permission) {
        return switch (permission) {
            case READ_POLICIES -> "read:policies";
            case WRITE_POLICIES -> "write:policies";
            case DELETE_POLICIES -> "delete:policies";
            case READ_TABLES -> "read:tables";
            case READ_OPERATIONS -> "read:operations";
            case TRIGGER_MAINTENANCE -> "trigger:maintenance";
            case MANAGE_API_KEYS -> "manage:api-keys";
        };
    }
}
