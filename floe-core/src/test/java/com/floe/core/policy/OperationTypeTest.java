package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class OperationTypeTest {

    @Test
    void shouldHaveExpectedNumberOfOperationTypes() {
        assertEquals(4, OperationType.values().length);
    }

    @Test
    void shouldHaveAllExpectedOperationTypes() {
        assertNotNull(OperationType.REWRITE_DATA_FILES);
        assertNotNull(OperationType.EXPIRE_SNAPSHOTS);
        assertNotNull(OperationType.ORPHAN_CLEANUP);
        assertNotNull(OperationType.REWRITE_MANIFESTS);
    }

    @Test
    void shouldConvertFromString() {
        assertEquals(OperationType.REWRITE_DATA_FILES, OperationType.valueOf("REWRITE_DATA_FILES"));
        assertEquals(OperationType.EXPIRE_SNAPSHOTS, OperationType.valueOf("EXPIRE_SNAPSHOTS"));
        assertEquals(OperationType.ORPHAN_CLEANUP, OperationType.valueOf("ORPHAN_CLEANUP"));
        assertEquals(OperationType.REWRITE_MANIFESTS, OperationType.valueOf("REWRITE_MANIFESTS"));
    }

    @Test
    void shouldThrowExceptionForInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> OperationType.valueOf("INVALID"));
        assertThrows(IllegalArgumentException.class, () -> OperationType.valueOf("COMPACT"));
    }

    @Test
    void shouldReturnCorrectName() {
        assertEquals("REWRITE_DATA_FILES", OperationType.REWRITE_DATA_FILES.name());
        assertEquals("EXPIRE_SNAPSHOTS", OperationType.EXPIRE_SNAPSHOTS.name());
        assertEquals("ORPHAN_CLEANUP", OperationType.ORPHAN_CLEANUP.name());
        assertEquals("REWRITE_MANIFESTS", OperationType.REWRITE_MANIFESTS.name());
    }

    @Test
    void shouldReturnCorrectOrdinal() {
        assertEquals(0, OperationType.REWRITE_DATA_FILES.ordinal());
        assertEquals(1, OperationType.EXPIRE_SNAPSHOTS.ordinal());
        assertEquals(2, OperationType.ORPHAN_CLEANUP.ordinal());
        assertEquals(3, OperationType.REWRITE_MANIFESTS.ordinal());
    }
}
