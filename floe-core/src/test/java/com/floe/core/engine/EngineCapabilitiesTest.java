package com.floe.core.engine;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.maintenance.MaintenanceOperation;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EngineCapabilitiesTest {

    @Test
    @DisplayName("Default capabilities should have no features enabled")
    void defaultCapabilitiesShouldHaveNoFeatures() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();

        assertTrue(capabilities.supportedOperations().isEmpty());
        assertFalse(capabilities.supportsAsync());
        assertFalse(capabilities.supportsPartitionFiltering());
        assertFalse(capabilities.supportsIncrementalProcessing());
        assertEquals(10, capabilities.maxConcurrentMaintenanceOperations());
    }

    @Test
    @DisplayName("Spark capabilities should have full operation support")
    void sparkCapabilitiesShouldHaveFullOperationSupport() {
        EngineCapabilities capabilities = EngineCapabilities.spark();

        assertTrue(capabilities.supportsAsync());
        assertTrue(capabilities.supportsPartitionFiltering());
        assertTrue(capabilities.supportsIncrementalProcessing());
        assertEquals(100, capabilities.maxConcurrentMaintenanceOperations());

        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.REWRITE_DATA_FILES));
        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS));
        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.REWRITE_MANIFESTS));
        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.ORPHAN_CLEANUP));
    }

    @Test
    @DisplayName("Trino capabilities should have full operation support")
    void trinoCapabilitiesShouldHaveFullOperationSupport() {
        EngineCapabilities capabilities = EngineCapabilities.trino();

        assertFalse(capabilities.supportsAsync());
        assertFalse(capabilities.supportsPartitionFiltering());
        assertFalse(capabilities.supportsIncrementalProcessing());
        assertEquals(10, capabilities.maxConcurrentMaintenanceOperations());

        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.REWRITE_DATA_FILES));
        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS));
        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.ORPHAN_CLEANUP));
        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.REWRITE_MANIFESTS));
    }

    @Test
    @DisplayName("isOperationSupported should return false for unsupported operation")
    void isOperationSupportedShouldReturnFalseForUnsupportedOperation() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();

        assertFalse(
                capabilities.isOperationSupported(MaintenanceOperation.Type.REWRITE_DATA_FILES));
        assertFalse(capabilities.isOperationSupported(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS));
    }

    @Test
    @DisplayName("Builder should create custom capabilities")
    void builderShouldCreateCustomCapabilities() {
        EngineCapabilities capabilities =
                EngineCapabilities.builder()
                        .supportedOperations(
                                Set.of(
                                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS))
                        .supportsAsync(false)
                        .supportsPartitionFiltering(true)
                        .supportsIncrementalProcessing(false)
                        .maxConcurrentOperations(50)
                        .build();

        assertFalse(capabilities.supportsAsync());
        assertTrue(capabilities.supportsPartitionFiltering());
        assertFalse(capabilities.supportsIncrementalProcessing());
        assertEquals(50, capabilities.maxConcurrentMaintenanceOperations());

        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.REWRITE_DATA_FILES));
        assertTrue(capabilities.isOperationSupported(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS));
        assertFalse(capabilities.isOperationSupported(MaintenanceOperation.Type.REWRITE_MANIFESTS));
    }

    @Test
    @DisplayName("Builder defaults should match default capabilities")
    void builderDefaultsShouldMatchDefaultCapabilities() {
        EngineCapabilities capabilities = EngineCapabilities.builder().build();

        assertTrue(capabilities.supportedOperations().isEmpty());
        assertFalse(capabilities.supportsAsync());
        assertFalse(capabilities.supportsPartitionFiltering());
        assertFalse(capabilities.supportsIncrementalProcessing());
        assertEquals(10, capabilities.maxConcurrentMaintenanceOperations());
    }

    @Test
    @DisplayName("EngineCapabilities with same features should be equal")
    void differentCapabilitiesShouldNotBeEqual() {
        EngineCapabilities spark = EngineCapabilities.spark();
        EngineCapabilities trino = EngineCapabilities.trino();

        assertNotEquals(spark, trino);
    }
}
