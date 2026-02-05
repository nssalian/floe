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
