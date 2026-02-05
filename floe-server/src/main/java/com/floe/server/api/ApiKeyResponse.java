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
