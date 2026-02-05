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

package com.floe.core.health;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class HealthIssueTest {

    @Test
    void createWithConstructor() {
        HealthIssue issue =
                new HealthIssue(
                        HealthIssue.Type.TOO_MANY_SMALL_FILES,
                        HealthIssue.Severity.WARNING,
                        "30% of files are under 100MB");

        assertEquals(HealthIssue.Type.TOO_MANY_SMALL_FILES, issue.type());
        assertEquals(HealthIssue.Severity.WARNING, issue.severity());
        assertEquals("30% of files are under 100MB", issue.message());
    }

    @Test
    void createInfoIssue() {
        HealthIssue issue = HealthIssue.info(HealthIssue.Type.TABLE_EMPTY, "Table has no data");

        assertEquals(HealthIssue.Severity.INFO, issue.severity());
        assertFalse(issue.isCritical());
        assertFalse(issue.isWarningOrWorse());
    }

    @Test
    void createWarningIssue() {
        HealthIssue issue =
                HealthIssue.warning(HealthIssue.Type.TOO_MANY_SMALL_FILES, "25% small files");

        assertEquals(HealthIssue.Severity.WARNING, issue.severity());
        assertFalse(issue.isCritical());
        assertTrue(issue.isWarningOrWorse());
    }

    @Test
    void createCriticalIssue() {
        HealthIssue issue =
                HealthIssue.critical(
                        HealthIssue.Type.TOO_MANY_SNAPSHOTS, "1000 snapshots accumulated");

        assertEquals(HealthIssue.Severity.CRITICAL, issue.severity());
        assertTrue(issue.isCritical());
        assertTrue(issue.isWarningOrWorse());
    }

    @Test
    void allTypesExist() {
        // Verify all expected types are defined
        assertNotNull(HealthIssue.Type.TOO_MANY_SMALL_FILES);
        assertNotNull(HealthIssue.Type.TOO_MANY_LARGE_FILES);
        assertNotNull(HealthIssue.Type.HIGH_FILE_COUNT);
        assertNotNull(HealthIssue.Type.TOO_MANY_SNAPSHOTS);
        assertNotNull(HealthIssue.Type.OLD_SNAPSHOTS);
        assertNotNull(HealthIssue.Type.TOO_MANY_DELETE_FILES);
        assertNotNull(HealthIssue.Type.HIGH_DELETE_FILE_RATIO);
        assertNotNull(HealthIssue.Type.TOO_MANY_MANIFESTS);
        assertNotNull(HealthIssue.Type.LARGE_MANIFEST_LIST);
        assertNotNull(HealthIssue.Type.TOO_MANY_PARTITIONS);
        assertNotNull(HealthIssue.Type.PARTITION_SKEW);
        assertNotNull(HealthIssue.Type.TABLE_EMPTY);
        assertNotNull(HealthIssue.Type.STALE_METADATA);
    }

    @Test
    void allSeveritiesExist() {
        assertNotNull(HealthIssue.Severity.INFO);
        assertNotNull(HealthIssue.Severity.WARNING);
        assertNotNull(HealthIssue.Severity.CRITICAL);
    }
}
