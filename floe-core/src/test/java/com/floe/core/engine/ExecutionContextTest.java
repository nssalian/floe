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

package com.floe.core.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExecutionContextTest {

    @Test
    @DisplayName("should create ExecutionContext with builder")
    void shouldCreateWithBuilder() {
        ExecutionContext context =
                ExecutionContext.builder("exec-123")
                        .catalogProperties(Map.of("catalog.name", "iceberg"))
                        .engineProperties(Map.of("spark.executor.memory", "4g"))
                        .callbackUrl("http://callback.example.com")
                        .timeoutSeconds(7200)
                        .priority(10)
                        .build();

        assertEquals("exec-123", context.executionId());
        assertEquals(Map.of("catalog.name", "iceberg"), context.catalogProperties());
        assertEquals(Map.of("spark.executor.memory", "4g"), context.engineProperties());
        assertEquals(Optional.of("http://callback.example.com"), context.callbackUrl());
        assertEquals(7200, context.timeoutSeconds());
        assertEquals(10, context.priority());
    }

    @Test
    @DisplayName("builder should use default values when not set")
    void builderShouldUseDefaults() {
        ExecutionContext context = ExecutionContext.builder("exec-456").build();

        assertEquals("exec-456", context.executionId());
        assertTrue(context.catalogProperties().isEmpty());
        assertTrue(context.engineProperties().isEmpty());
        assertEquals(Optional.empty(), context.callbackUrl());
        assertEquals(3600, context.timeoutSeconds()); // 1 hour default
        assertEquals(5, context.priority()); // Medium priority default
    }

    @Test
    @DisplayName("should throw exception for null executionId")
    void shouldThrowExceptionForNullExecutionId() {
        assertThrows(IllegalArgumentException.class, () -> ExecutionContext.builder(null).build());
    }

    @Test
    @DisplayName("should throw exception for blank executionId")
    void shouldThrowExceptionForBlankExecutionId() {
        assertThrows(IllegalArgumentException.class, () -> ExecutionContext.builder("").build());
        assertThrows(IllegalArgumentException.class, () -> ExecutionContext.builder("   ").build());
    }

    @Test
    @DisplayName("getCatalogProperty should return value when present")
    void getCatalogPropertyShouldReturnValueWhenPresent() {
        ExecutionContext context =
                ExecutionContext.builder("exec-123")
                        .catalogProperties(Map.of("catalog.name", "iceberg"))
                        .build();

        assertEquals("iceberg", context.getCatalogProperty("catalog.name", "default"));
    }

    @Test
    @DisplayName("getCatalogProperty should return default when absent")
    void getCatalogPropertyShouldReturnDefaultWhenAbsent() {
        ExecutionContext context = ExecutionContext.builder("exec-123").build();

        assertEquals("default", context.getCatalogProperty("catalog.name", "default"));
    }

    @Test
    @DisplayName("getEngineProperty should return value when present")
    void getEnginePropertyShouldReturnValueWhenPresent() {
        ExecutionContext context =
                ExecutionContext.builder("exec-123")
                        .engineProperties(Map.of("spark.executor.memory", "4g"))
                        .build();

        assertEquals("4g", context.getEngineProperty("spark.executor.memory", "2g"));
    }

    @Test
    @DisplayName("getEngineProperty should return default when absent")
    void getEnginePropertyShouldReturnDefaultWhenAbsent() {
        ExecutionContext context = ExecutionContext.builder("exec-123").build();

        assertEquals("2g", context.getEngineProperty("spark.executor.memory", "2g"));
    }

    @Test
    @DisplayName("callbackUrl should handle null value")
    void callbackUrlShouldHandleNullValue() {
        ExecutionContext context = ExecutionContext.builder("exec-123").callbackUrl(null).build();

        assertEquals(Optional.empty(), context.callbackUrl());
    }

    @Test
    @DisplayName("properties should be immutable")
    void propertiesShouldBeImmutable() {
        Map<String, String> catalogProps = new java.util.HashMap<>();
        catalogProps.put("key", "value");

        ExecutionContext context =
                ExecutionContext.builder("exec-123").catalogProperties(catalogProps).build();

        // Original map modification should not affect the context
        catalogProps.put("newKey", "newValue");
        assertFalse(context.catalogProperties().containsKey("newKey"));

        // Context properties should be immutable
        assertThrows(
                UnsupportedOperationException.class,
                () -> context.catalogProperties().put("anotherKey", "anotherValue"));
    }

    @Test
    @DisplayName("record should support equality and hashCode")
    void recordShouldSupportEquality() {
        ExecutionContext context1 =
                ExecutionContext.builder("exec-123")
                        .catalogProperties(Map.of("key", "value"))
                        .timeoutSeconds(3600)
                        .priority(5)
                        .build();

        ExecutionContext context2 =
                ExecutionContext.builder("exec-123")
                        .catalogProperties(Map.of("key", "value"))
                        .timeoutSeconds(3600)
                        .priority(5)
                        .build();

        assertEquals(context1, context2);
        assertEquals(context1.hashCode(), context2.hashCode());
    }

    @Test
    @DisplayName("different contexts should not be equal")
    void differentContextsShouldNotBeEqual() {
        ExecutionContext context1 = ExecutionContext.builder("exec-123").build();
        ExecutionContext context2 = ExecutionContext.builder("exec-456").build();

        assertNotEquals(context1, context2);
    }

    @Test
    @DisplayName("record should have meaningful toString")
    void recordShouldHaveToString() {
        ExecutionContext context = ExecutionContext.builder("exec-123").build();
        String toString = context.toString();

        assertTrue(toString.contains("ExecutionContext"));
        assertTrue(toString.contains("exec-123"));
    }
}
