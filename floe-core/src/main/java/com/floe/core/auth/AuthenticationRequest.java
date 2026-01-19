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
