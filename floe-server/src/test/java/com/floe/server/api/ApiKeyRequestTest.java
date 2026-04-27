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

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.auth.Role;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ApiKeyRequestTest {

    @Test
    void shouldCreateWithAllFields() {
        Instant expiresAt = Instant.parse("2025-01-15T10:00:00Z");
        ApiKeyRequest request = new ApiKeyRequest("My API Key", Role.ADMIN, expiresAt);

        assertEquals("My API Key", request.name());
        assertEquals(Role.ADMIN, request.role());
        assertEquals(expiresAt, request.expiresAt());
    }

    @Test
    void shouldDefaultToViewerRoleWhenNullProvided() {
        ApiKeyRequest request = new ApiKeyRequest("My API Key", null, null);

        assertEquals("My API Key", request.name());
        assertEquals(Role.VIEWER, request.role());
        assertNull(request.expiresAt());
    }

    @Test
    void shouldPreserveExplicitRole() {
        ApiKeyRequest request = new ApiKeyRequest("Admin Key", Role.ADMIN, null);
        assertEquals(Role.ADMIN, request.role());

        ApiKeyRequest operatorRequest = new ApiKeyRequest("Operator Key", Role.OPERATOR, null);
        assertEquals(Role.OPERATOR, operatorRequest.role());
    }

    @Test
    void shouldAllowNullExpiresAt() {
        ApiKeyRequest request = new ApiKeyRequest("Never Expires", Role.VIEWER, null);

        assertNull(request.expiresAt());
    }

    @Test
    void recordShouldSupportEquality() {
        Instant expiresAt = Instant.parse("2025-01-15T10:00:00Z");
        ApiKeyRequest request1 = new ApiKeyRequest("Key", Role.ADMIN, expiresAt);
        ApiKeyRequest request2 = new ApiKeyRequest("Key", Role.ADMIN, expiresAt);

        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());
    }

    @Test
    void differentRequestsShouldNotBeEqual() {
        ApiKeyRequest request1 = new ApiKeyRequest("Key1", Role.ADMIN, null);
        ApiKeyRequest request2 = new ApiKeyRequest("Key2", Role.ADMIN, null);

        assertNotEquals(request1, request2);
    }

    @Test
    void recordShouldHaveToString() {
        ApiKeyRequest request = new ApiKeyRequest("My Key", Role.VIEWER, null);
        String toString = request.toString();

        assertTrue(toString.contains("ApiKeyRequest"));
        assertTrue(toString.contains("My Key"));
        assertTrue(toString.contains("VIEWER"));
    }
}
