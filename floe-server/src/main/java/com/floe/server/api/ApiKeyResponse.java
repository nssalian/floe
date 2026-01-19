package com.floe.server.api;

import com.floe.core.auth.ApiKey;
import com.floe.core.auth.Role;
import java.time.Instant;
import java.util.Set;

/** Response containing API key information (without the key hash). */
public record ApiKeyResponse(
        String id,
        String name,
        Role role,
        Set<String> permissions,
        boolean enabled,
        Instant createdAt,
        Instant expiresAt,
        Instant lastUsedAt,
        String createdBy) {
    /** Create a response from an ApiKey. */
    public static ApiKeyResponse from(ApiKey apiKey) {
        Set<String> permissionNames =
                apiKey.role().permissions().stream()
                        .map(Enum::name)
                        .collect(java.util.stream.Collectors.toSet());

        return new ApiKeyResponse(
                apiKey.id(),
                apiKey.name(),
                apiKey.role(),
                permissionNames,
                apiKey.enabled(),
                apiKey.createdAt(),
                apiKey.expiresAt(),
                apiKey.lastUsedAt(),
                apiKey.createdBy());
    }
}
