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
 * Wrapper around HTTP request context that provides convenient methods for extracting
 * authentication credentials.
 */
public interface AuthenticationRequest {

    /**
     * Get bearer token from Authorization header
     *
     * @return Bearer token if present
     */
    Optional<String> getBearerToken();

    /**
     * Get API key from custom header
     *
     * @param headerName Name of the header containing the API key
     * @return API key if present
     */
    Optional<String> getApiKey(String headerName);

    /**
     * Get client IP address (for rate limiting / IP whitelisting)
     *
     * @return Client IP address
     */
    String getClientIp();

    /**
     * Get request path for audit logging
     *
     * @return Request path (e.g., "/api/v1/policies")
     */
    String getPath();

    /**
     * Get HTTP method
     *
     * @return HTTP method (e.g., "GET", "POST")
     */
    String getMethod();

    /**
     * Get any header value
     *
     * @param headerName Header name
     * @return Header value if present
     */
    Optional<String> getHeader(String headerName);
}
