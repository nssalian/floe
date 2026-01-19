package com.floe.core.auth;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default AuthorizationProvider using built-in Role/Permission system.
 *
 * <p>This implementation uses Floe's internal RBAC with three roles:
 *
 * <ul>
 *   <li>ADMIN - Full access to all resources
 *   <li>OPERATOR - Can trigger maintenance, read all data
 *   <li>VIEWER - Read-only access
 * </ul>
 */
public record RbacAuthorizationProvider(Function<String, Optional<Role>> roleResolver)
        implements AuthorizationProvider {

    private static final Logger LOG = LoggerFactory.getLogger(RbacAuthorizationProvider.class);

    /**
     * Create an RBAC provider with a role resolver function.
     *
     * @param roleResolver function to resolve a subject to their role
     */
    public RbacAuthorizationProvider {}

    /**
     * Create an RBAC provider that always returns a fixed role.
     *
     * @param defaultRole the role to use for all subjects
     */
    public RbacAuthorizationProvider(Role defaultRole) {
        this(subject -> Optional.of(defaultRole));
    }

    @Override
    public AuthorizationResult authorize(AuthorizationRequest request) {
        Optional<Role> roleOpt = roleResolver.apply(request.subject());

        if (roleOpt.isEmpty()) {
            LOG.debug("No role found for subject: {}", request.subject());
            return AuthorizationResult.denied("no role assigned");
        }

        Role role = roleOpt.get();
        Permission requiredPermission = mapActionToPermission(request.action());

        if (requiredPermission == null) {
            LOG.warn("Unknown action: {}", request.action());
            return AuthorizationResult.denied("unknown action: " + request.action());
        }

        if (role.hasPermission(requiredPermission)) {
            LOG.debug(
                    "Authorized: subject={} role={} action={} resource={}:{}",
                    request.subject(),
                    role,
                    request.action(),
                    request.resourceType(),
                    request.resourceId());
            return AuthorizationResult.allowed(
                    "role " + role.name() + " has " + requiredPermission);
        }

        LOG.debug(
                "Denied: subject={} role={} action={} resource={}:{} (missing {})",
                request.subject(),
                role,
                request.action(),
                request.resourceType(),
                request.resourceId(),
                requiredPermission);
        return AuthorizationResult.denied(
                "role " + role.name() + " lacks permission " + requiredPermission);
    }

    /** Map an action string to a Permission. */
    private Permission mapActionToPermission(String action) {
        return switch (action.toLowerCase(Locale.ROOT)) {
            case "read:policies", "read_policies" -> Permission.READ_POLICIES;
            case "write:policies", "write_policies" -> Permission.WRITE_POLICIES;
            case "delete:policies", "delete_policies" -> Permission.DELETE_POLICIES;
            case "read:tables", "read_tables" -> Permission.READ_TABLES;
            case "read:operations", "read_operations" -> Permission.READ_OPERATIONS;
            case "trigger:maintenance", "trigger_maintenance" -> Permission.TRIGGER_MAINTENANCE;
            case "manage:api-keys", "manage_api_keys" -> Permission.MANAGE_API_KEYS;
            default -> null;
        };
    }
}
