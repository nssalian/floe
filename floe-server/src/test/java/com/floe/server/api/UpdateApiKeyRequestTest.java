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
