package com.floe.server.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TriggerRequestTest {

    @Test
    void shouldCreateWithAllFields() {
        TriggerRequest request = new TriggerRequest("iceberg", "db", "orders", "daily-compact");

        assertEquals("iceberg", request.catalog());
        assertEquals("db", request.namespace());
        assertEquals("orders", request.table());
        assertEquals("daily-compact", request.policyName());
    }

    @Test
    void shouldCreateWithNullPolicyName() {
        TriggerRequest request = new TriggerRequest("iceberg", "db", "orders", null);

        assertEquals("iceberg", request.catalog());
        assertEquals("db", request.namespace());
        assertEquals("orders", request.table());
        assertNull(request.policyName());
    }

    @Test
    void hasSpecificPolicyShouldReturnTrueWhenPolicyNameIsSet() {
        TriggerRequest request = new TriggerRequest("iceberg", "db", "orders", "daily-compact");

        assertTrue(request.hasSpecificPolicy());
    }

    @Test
    void hasSpecificPolicyShouldReturnFalseWhenPolicyNameIsNull() {
        TriggerRequest request = new TriggerRequest("iceberg", "db", "orders", null);

        assertFalse(request.hasSpecificPolicy());
    }

    @Test
    void hasSpecificPolicyShouldReturnFalseWhenPolicyNameIsBlank() {
        TriggerRequest request1 = new TriggerRequest("iceberg", "db", "orders", "");
        TriggerRequest request2 = new TriggerRequest("iceberg", "db", "orders", "   ");

        assertFalse(request1.hasSpecificPolicy());
        assertFalse(request2.hasSpecificPolicy());
    }

    @Test
    void recordShouldSupportEquality() {
        TriggerRequest request1 = new TriggerRequest("iceberg", "db", "orders", "policy");
        TriggerRequest request2 = new TriggerRequest("iceberg", "db", "orders", "policy");

        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());
    }

    @Test
    void differentRequestsShouldNotBeEqual() {
        TriggerRequest request1 = new TriggerRequest("iceberg", "db", "orders", "policy1");
        TriggerRequest request2 = new TriggerRequest("iceberg", "db", "orders", "policy2");

        assertNotEquals(request1, request2);
    }

    @Test
    void recordShouldHaveToString() {
        TriggerRequest request = new TriggerRequest("iceberg", "db", "orders", "policy");
        String toString = request.toString();

        assertTrue(toString.contains("TriggerRequest"));
        assertTrue(toString.contains("iceberg"));
        assertTrue(toString.contains("db"));
        assertTrue(toString.contains("orders"));
    }
}
