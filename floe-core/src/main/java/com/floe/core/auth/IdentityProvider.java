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

package com.floe.core.auth;

import java.util.Optional;

/**
 * Abstraction for identity providers (OIDC, API Keys, SAML, etc.) Implementations are responsible
 * for validating credentials and returning a normalized principal.
 */
public interface IdentityProvider {

    /**
     * Authenticate a request and return the principal
     *
     * @param request The HTTP request context
     * @return Authenticated principal if valid, empty if authentication fails
     */
    Optional<FloePrincipal> authenticate(AuthenticationRequest request);

    /**
     * Check if this provider can handle the given request (e.g., has X-API-Key header vs
     * Authorization: Bearer)
     *
     * @param request The HTTP request context
     * @return true if this provider should handle the request
     */
    boolean supports(AuthenticationRequest request);

    /**
     * Get the authentication scheme name (for WWW-Authenticate header)
     *
     * @return Scheme name (e.g., "Bearer", "ApiKey")
     */
    String getScheme();

    /**
     * Priority for provider selection (higher = checked first) Useful when multiple providers might
     * match
     *
     * @return Priority value (default: 0)
     */
    default int priority() {
        return 0;
    }
}
