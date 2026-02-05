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

/** Response returned when creating a new API key. */
public record CreateApiKeyResponse(String key, ApiKeyResponse apiKey) {
    /** Create a response with the plaintext key and API key details. */
    public static CreateApiKeyResponse from(String plaintextKey, ApiKey apiKey) {
        return new CreateApiKeyResponse(plaintextKey, ApiKeyResponse.from(apiKey));
    }
}
