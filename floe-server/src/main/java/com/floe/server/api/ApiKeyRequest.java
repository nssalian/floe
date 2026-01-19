package com.floe.server.api;

import com.floe.core.auth.Role;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.time.Instant;

/**
 * Request to create a new API key.
 *
 * @param name key name for identification
 * @param role role for permissions
 * @param expiresAt optional expiration time
 */
public record ApiKeyRequest(
        @NotBlank(message = "Name is required")
                @Size(min = 1, max = 255, message = "Name must be between 1 and 255 characters")
                String name,
        Role role,
        Instant expiresAt) {
    public ApiKeyRequest {
        if (role == null) {
            role = Role.VIEWER;
        }
    }
}
