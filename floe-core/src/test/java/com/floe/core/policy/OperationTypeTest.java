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
