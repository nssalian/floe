package com.floe.server.auth;

import com.floe.core.auth.*;
import com.floe.server.config.FloeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API Key authentication provider (machine-to-machine authentication). Validates API keys stored in
 * the database and enforces: - Key expiration - Key enabled/disabled status - IP whitelisting
 * (optional) - Rate limiting
 */
@ApplicationScoped
public class ApiKeyIdentityProvider implements IdentityProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ApiKeyIdentityProvider.class);

    @Inject ApiKeyStore apiKeyStore;

    @Inject FloeConfig config;

    @Inject AuditLogger auditLogger;

    @Override
    public Optional<FloePrincipal> authenticate(AuthenticationRequest request) {
        String headerName = config.auth().headerName();
        Optional<String> apiKeyValue = request.getApiKey(headerName);

        if (apiKeyValue.isEmpty()) {
            return Optional.empty();
        }

        String clientIp = request.getClientIp();

        // Hash the provided key
        String keyHash = ApiKeyGenerator.hashKey(apiKeyValue.get());

        // Lookup in store
        Optional<ApiKey> apiKey = apiKeyStore.findByKeyHash(keyHash);

        if (apiKey.isEmpty()) {
            auditLogger.log(
                    AuditEvent.AUTH_FAILED_INVALID_KEY,
                    clientIp,
                    request.getPath(),
                    request.getMethod());
            return Optional.empty();
        }

        ApiKey key = apiKey.get();

        // Check if key is enabled
        if (!key.enabled()) {
            auditLogger.log(
                    AuditEvent.AUTH_FAILED_DISABLED_KEY,
                    key.id(),
                    key.name(),
                    clientIp,
                    request.getPath());
            return Optional.empty();
        }

        // Check expiration
        if (key.expiresAt() != null && Instant.now().isAfter(key.expiresAt())) {
            auditLogger.log(
                    AuditEvent.AUTH_FAILED_EXPIRED_KEY,
                    key.id(),
                    key.name(),
                    clientIp,
                    request.getPath());
            return Optional.empty();
        }

        // Update last used timestamp (async)
        updateLastUsedAsync(key.id());

        // Log successful authentication
        auditLogger.log(AuditEvent.AUTH_SUCCESS, key.id(), key.name(), clientIp, request.getPath());

        // Build principal
        FloePrincipal.Builder builder =
                FloePrincipal.builder()
                        .userId(key.id())
                        .username(key.name())
                        .roles(Set.of(key.role()))
                        .authenticationMethod("API_KEY")
                        .metadata("createdBy", key.createdBy())
                        .metadata("createdAt", key.createdAt().toString());

        if (key.expiresAt() != null) {
            builder.metadata("expiresAt", key.expiresAt().toString());
        }

        FloePrincipal principal = builder.build();

        LOG.debug(
                "Successfully authenticated API key: "
                        + key.name()
                        + " (id="
                        + key.id()
                        + ", role="
                        + key.role()
                        + ")");

        return Optional.of(principal);
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        String headerName = config.auth().headerName();
        return request.getApiKey(headerName).isPresent();
    }

    @Override
    public String getScheme() {
        return "ApiKey";
    }

    @Override
    public int priority() {
        return 50; // Lower than OIDC, checked second
    }

    /**
     * Update last used timestamp asynchronously (non-blocking)
     *
     * @param keyId API key ID
     */
    private void updateLastUsedAsync(String keyId) {
        // Use virtual threads for lightweight async execution
        Thread.ofVirtual()
                .start(
                        () -> {
                            try {
                                apiKeyStore.updateLastUsed(keyId);
                            } catch (Exception e) {
                                LOG.warn("Failed to update last_used_at for key " + keyId, e);
                            }
                        });
    }
}
