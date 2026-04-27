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
import org.junit.jupiter.api.Test;

class UpdateApiKeyRequestTest {

    @Test
    void shouldCreateWithAllFields() {
        UpdateApiKeyRequest request = new UpdateApiKeyRequest("New Name", Role.OPERATOR, true);

        assertEquals("New Name", request.name());
        assertEquals(Role.OPERATOR, request.role());
        assertTrue(request.enabled());
    }

    @Test
    void shouldAllowNullFields() {
        UpdateApiKeyRequest request = new UpdateApiKeyRequest(null, null, null);

        assertNull(request.name());
        assertNull(request.role());
        assertNull(request.enabled());
    }

    @Test
    void shouldAllowPartialUpdates() {
        // Only update name
        UpdateApiKeyRequest nameOnly = new UpdateApiKeyRequest("New Name", null, null);
        assertEquals("New Name", nameOnly.name());
        assertNull(nameOnly.role());
        assertNull(nameOnly.enabled());

        // Only update role
        UpdateApiKeyRequest roleOnly = new UpdateApiKeyRequest(null, Role.ADMIN, null);
        assertNull(roleOnly.name());
        assertEquals(Role.ADMIN, roleOnly.role());
        assertNull(roleOnly.enabled());

        // Only update enabled
        UpdateApiKeyRequest enabledOnly = new UpdateApiKeyRequest(null, null, false);
        assertNull(enabledOnly.name());
        assertNull(enabledOnly.role());
        assertFalse(enabledOnly.enabled());
    }

    @Test
    void recordShouldSupportEquality() {
        UpdateApiKeyRequest request1 = new UpdateApiKeyRequest("Name", Role.VIEWER, true);
        UpdateApiKeyRequest request2 = new UpdateApiKeyRequest("Name", Role.VIEWER, true);

        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());
    }

    @Test
    void differentRequestsShouldNotBeEqual() {
        UpdateApiKeyRequest request1 = new UpdateApiKeyRequest("Name1", Role.VIEWER, true);
        UpdateApiKeyRequest request2 = new UpdateApiKeyRequest("Name2", Role.VIEWER, true);

        assertNotEquals(request1, request2);
    }

    @Test
    void recordShouldHaveToString() {
        UpdateApiKeyRequest request = new UpdateApiKeyRequest("My Key", Role.OPERATOR, false);
        String toString = request.toString();

        assertTrue(toString.contains("UpdateApiKeyRequest"));
        assertTrue(toString.contains("My Key"));
    }
}
