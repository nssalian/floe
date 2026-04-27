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
