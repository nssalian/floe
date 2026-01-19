package com.floe.core.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EngineTypeTest {

    @Test
    @DisplayName("should have expected number of types")
    void shouldHaveExpectedNumberOfTypes() {
        assertEquals(2, EngineType.values().length);
    }

    @Test
    @DisplayName("Spark should have correct display name and description")
    void sparkShouldHaveCorrectDisplayNameAndDescription() {
        assertEquals("Apache Spark", EngineType.SPARK.getDisplayName());
        assertEquals("Full Iceberg maintenance support", EngineType.SPARK.getDescription());
    }

    @Test
    @DisplayName("Trino should have correct display name and description")
    void trinoShouldHaveCorrectDisplayNameAndDescription() {
        assertEquals("Trino", EngineType.TRINO.getDisplayName());
        assertEquals("OPTIMIZE and expire_snapshots via SQL", EngineType.TRINO.getDescription());
    }

    @Test
    @DisplayName("should convert from string to enum")
    void shouldConvertFromString() {
        assertEquals(EngineType.SPARK, EngineType.valueOf("SPARK"));
        assertEquals(EngineType.TRINO, EngineType.valueOf("TRINO"));
    }

    @Test
    @DisplayName("should throw exception for invalid value")
    void shouldThrowExceptionForInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> EngineType.valueOf("INVALID"));
    }

    @Test
    @DisplayName("should return correct name")
    void shouldReturnCorrectName() {
        assertEquals("SPARK", EngineType.SPARK.name());
        assertEquals("TRINO", EngineType.TRINO.name());
    }

    @Test
    @DisplayName("should return correct ordinal")
    void shouldReturnCorrectOrdinal() {
        assertEquals(0, EngineType.SPARK.ordinal());
        assertEquals(1, EngineType.TRINO.ordinal());
    }
}
