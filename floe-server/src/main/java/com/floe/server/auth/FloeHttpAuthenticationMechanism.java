package com.floe.server.auth;

import com.floe.core.auth.*;
import com.floe.server.config.FloeConfig;
import com.floe.server.metrics.FloeMetrics;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quarkus Security HttpAuthenticationMechanism implementation for Floe.
 *
 * <p>Uses IdentityProvider abstraction (OidcIdentityProvider, ApiKeyIdentityProvider) - Returns
 * FloePrincipal as unified identity - Supports priority-based provider selection
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class FloeHttpAuthenticationMechanism implements HttpAuthenticationMechanism {

    private static final Logger LOG =
            LoggerFactory.getLogger(FloeHttpAuthenticationMechanism.class);

    private final List<IdentityProvider> floeIdentityProviders;
    private final AuditLogger auditLogger;
    private final FloeMetrics metrics;
    private final FloeConfig config;

    @Inject
    FloeHttpAuthenticationMechanism(
            Instance<IdentityProvider> identityProviders,
            AuditLogger auditLogger,
            FloeMetrics metrics,
            FloeConfig config) {
        this.floeIdentityProviders =
                StreamSupport.stream(identityProviders.spliterator(), false).toList();
        this.auditLogger = auditLogger;
        this.metrics = metrics;
        this.config = config;
    }

    // Package-private constructor for testing
    FloeHttpAuthenticationMechanism(
            List<IdentityProvider> floeIdentityProviders,
            AuditLogger auditLogger,
            FloeMetrics metrics,
            FloeConfig config) {
        this.floeIdentityProviders = floeIdentityProviders;
        this.auditLogger = auditLogger;
        this.metrics = metrics;
        this.config = config;
    }

    @Override
    public Uni<SecurityIdentity> authenticate(
            RoutingContext context, IdentityProviderManager identityProviderManager) {
        // Skip auth if disabled
        if (!config.auth().enabled()) {
            return Uni.createFrom().optional(Optional.empty());
        }

        // Create authentication request from Vert.x context
        com.floe.core.auth.AuthenticationRequest authRequest =
                new VertxAuthenticationRequest(context);

        // Get sorted providers (by priority, highest first)
        List<IdentityProvider> sortedProviders =
                floeIdentityProviders.stream()
                        .sorted(Comparator.comparingInt(IdentityProvider::priority).reversed())
                        .toList();

        if (sortedProviders.isEmpty()) {
            LOG.warn("No Floe identity providers registered");
            return Uni.createFrom().optional(Optional.empty());
        }

        // Try providers in priority order
        for (IdentityProvider provider : sortedProviders) {
            if (!provider.supports(authRequest)) {
                continue;
            }

            try {
                Optional<FloePrincipal> principal = provider.authenticate(authRequest);

                if (principal.isPresent()) {
                    FloePrincipal p = principal.get();

                    // Log successful authentication
                    auditLogger.logAccess(
                            AuditEvent.REQUEST_AUTHORIZED, p, authRequest.getPath(), "ALLOWED");

                    // Record auth success metric
                    metrics.recordAuthSuccess(provider.getClass().getSimpleName());

                    LOG.debug(
                            "Authenticated user: {} ({}) via {} with roles: {}",
                            p.username(),
                            p.userId(),
                            provider.getClass().getSimpleName(),
                            p.roles());

                    // Convert FloePrincipal to Quarkus SecurityIdentity
                    SecurityIdentity identity = buildSecurityIdentity(p);
                    return Uni.createFrom().item(identity);
                }
            } catch (Exception e) {
                LOG.warn(
                        "Provider {} failed to authenticate: {}",
                        provider.getClass().getSimpleName(),
                        e.getMessage());
                auditLogger.log(
                        AuditEvent.AUTH_ERROR,
                        provider.getClass().getSimpleName(),
                        e.getMessage(),
                        authRequest.getClientIp());
            }
        }

        // No provider could authenticate - return empty (anonymous)
        metrics.recordAuthFailure("none", "no_valid_credentials");
        return Uni.createFrom().optional(Optional.empty());
    }

    @Override
    public Uni<ChallengeData> getChallenge(RoutingContext context) {
        // Return WWW-Authenticate header with supported schemes
        List<String> schemes =
                floeIdentityProviders.stream()
                        .map(IdentityProvider::getScheme)
                        .distinct()
                        .collect(Collectors.toList());

        String wwwAuthenticate = schemes.isEmpty() ? "Bearer, ApiKey" : String.join(", ", schemes);

        return Uni.createFrom().item(new ChallengeData(401, "WWW-Authenticate", wwwAuthenticate));
    }

    @Override
    public Set<Class<? extends AuthenticationRequest>> getCredentialTypes() {
        return Set.of();
    }

    /** Builds a Quarkus SecurityIdentity from a FloePrincipal. */
    private SecurityIdentity buildSecurityIdentity(FloePrincipal principal) {
        QuarkusSecurityIdentity.Builder builder =
                QuarkusSecurityIdentity.builder()
                        .setPrincipal(new QuarkusPrincipal(principal.username()));

        // Add roles
        for (Role role : principal.roles()) {
            builder.addRole(role.name());
            // Also add permissions derived from roles
            for (Permission permission : role.permissions()) {
                builder.addRole("PERMISSION_" + permission.name());
            }
        }

        // Add FloePrincipal as an attribute for downstream access
        builder.addAttribute("floePrincipal", principal);

        return builder.build();
    }

    /**
     * Vert.x-based AuthenticationRequest implementation. Implements the floe-core
     * AuthenticationRequest interface.
     */
    private static class VertxAuthenticationRequest
            implements com.floe.core.auth.AuthenticationRequest {

        private final RoutingContext context;

        VertxAuthenticationRequest(RoutingContext context) {
            this.context = context;
        }

        @Override
        public Optional<String> getBearerToken() {
            String authHeader = context.request().getHeader("Authorization");
            if (authHeader != null && authHeader.toLowerCase(Locale.ROOT).startsWith("bearer ")) {
                return Optional.of(authHeader.substring(7).trim());
            }
            return Optional.empty();
        }

        @Override
        public Optional<String> getApiKey(String headerName) {
            String value = context.request().getHeader(headerName);
            return Optional.ofNullable(value).filter(s -> !s.isBlank());
        }

        @Override
        public String getPath() {
            return context.request().path();
        }

        @Override
        public String getMethod() {
            return context.request().method().name();
        }

        @Override
        public String getClientIp() {
            // Try X-Forwarded-For first for proxied requests
            String xff = context.request().getHeader("X-Forwarded-For");
            if (xff != null && !xff.isBlank()) {
                return xff.split(",")[0].trim();
            }
            // Try X-Real-IP
            String realIp = context.request().getHeader("X-Real-IP");
            if (realIp != null && !realIp.isBlank()) {
                return realIp.trim();
            }
            return context.request().remoteAddress() != null
                    ? context.request().remoteAddress().host()
                    : "unknown";
        }

        @Override
        public Optional<String> getHeader(String headerName) {
            return Optional.ofNullable(context.request().getHeader(headerName));
        }
    }
}
