package com.floe.server.api;

import com.floe.core.auth.ApiKey;

/** Response returned when creating a new API key. */
public record CreateApiKeyResponse(String key, ApiKeyResponse apiKey) {
    /** Create a response with the plaintext key and API key details. */
    public static CreateApiKeyResponse from(String plaintextKey, ApiKey apiKey) {
        return new CreateApiKeyResponse(plaintextKey, ApiKeyResponse.from(apiKey));
    }
}
