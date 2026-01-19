package com.floe.server.api;

import com.floe.core.auth.Role;
import jakarta.validation.constraints.Size;

/**
 * Request to update an existing API key.
 *
 * @param name new key name
 * @param role new role
 * @param enabled whether key is active
 */
public record UpdateApiKeyRequest(
        @Size(min = 1, max = 255, message = "Name must be between 1 and 255 characters")
                String name,
        Role role,
        Boolean enabled) {}
