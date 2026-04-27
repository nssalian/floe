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

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RewriteDataFilesConfigTest {

    @Test
    void defaultsShouldHaveExpectedValues() {
        RewriteDataFilesConfig config = RewriteDataFilesConfig.defaults();

        assertEquals("BINPACK", config.strategy());
        assertNull(config.sortOrder());
        assertNull(config.zOrderColumns());
        assertNull(config.targetFileSizeBytes());
        assertNull(config.maxFileGroupSizeBytes());
        assertNull(config.maxConcurrentFileGroupRewrites());
        assertNull(config.partialProgressEnabled());
        assertNull(config.partialProgressMaxCommits());
        assertNull(config.partialProgressMaxFailedCommits());
        assertNull(config.filter());
        assertNull(config.rewriteJobOrder());
        assertNull(config.useStartingSequenceNumber());
        assertNull(config.removeDanglingDeletes());
        assertNull(config.outputSpecId());
    }

    @Test
    void builderShouldCreateConfigWithAllFields() {
        RewriteDataFilesConfig config =
                RewriteDataFilesConfig.builder()
                        .strategy("SORT")
                        .sortOrder(List.of("col1", "col2"))
                        .zOrderColumns(List.of("col3"))
                        .targetFileSizeBytes(512L * 1024 * 1024)
                        .maxFileGroupSizeBytes(10L * 1024 * 1024 * 1024)
                        .maxConcurrentFileGroupRewrites(4)
                        .partialProgressEnabled(true)
                        .partialProgressMaxCommits(5)
                        .partialProgressMaxFailedCommits(3)
                        .filter("date > '2025-01-01'")
                        .rewriteJobOrder("FILES_DESC")
                        .useStartingSequenceNumber(true)
                        .removeDanglingDeletes(true)
                        .outputSpecId(1)
                        .build();

        assertEquals("SORT", config.strategy());
        assertEquals(List.of("col1", "col2"), config.sortOrder());
        assertEquals(List.of("col3"), config.zOrderColumns());
        assertEquals(512L * 1024 * 1024, config.targetFileSizeBytes());
        assertEquals(10L * 1024 * 1024 * 1024, config.maxFileGroupSizeBytes());
        assertEquals(4, config.maxConcurrentFileGroupRewrites());
        assertTrue(config.partialProgressEnabled());
        assertEquals(5, config.partialProgressMaxCommits());
        assertEquals(3, config.partialProgressMaxFailedCommits());
        assertEquals("date > '2025-01-01'", config.filter());
        assertEquals("FILES_DESC", config.rewriteJobOrder());
        assertTrue(config.useStartingSequenceNumber());
        assertTrue(config.removeDanglingDeletes());
        assertEquals(1, config.outputSpecId());
    }

    @Test
    void builderShouldUseDefaultStrategy() {
        RewriteDataFilesConfig config = RewriteDataFilesConfig.builder().build();

        assertEquals("BINPACK", config.strategy());
    }

    @Test
    void toOptionsMapShouldIncludeOnlyNonNullValues() {
        RewriteDataFilesConfig config =
                RewriteDataFilesConfig.builder()
                        .targetFileSizeBytes(512L * 1024 * 1024)
                        .partialProgressEnabled(true)
                        .build();

        Map<String, String> options = config.toOptionsMap();

        assertEquals("536870912", options.get("target-file-size-bytes"));
        assertEquals("true", options.get("partial-progress.enabled"));
        assertFalse(options.containsKey("max-file-group-size-bytes"));
        assertFalse(options.containsKey("partial-progress.max-failed-commits"));
    }

    @Test
    void toOptionsMapShouldIncludeAllSetValues() {
        RewriteDataFilesConfig config =
                RewriteDataFilesConfig.builder()
                        .targetFileSizeBytes(512L * 1024 * 1024)
                        .maxFileGroupSizeBytes(10L * 1024 * 1024 * 1024)
                        .maxConcurrentFileGroupRewrites(4)
                        .partialProgressEnabled(true)
                        .partialProgressMaxCommits(5)
                        .partialProgressMaxFailedCommits(3)
                        .rewriteJobOrder("FILES_DESC")
                        .useStartingSequenceNumber(true)
                        .removeDanglingDeletes(false)
                        .outputSpecId(2)
                        .build();

        Map<String, String> options = config.toOptionsMap();

        assertEquals("536870912", options.get("target-file-size-bytes"));
        assertEquals("10737418240", options.get("max-file-group-size-bytes"));
        assertEquals("4", options.get("max-concurrent-file-group-rewrites"));
        assertEquals("true", options.get("partial-progress.enabled"));
        assertEquals("5", options.get("partial-progress.max-commits"));
        assertEquals("3", options.get("partial-progress.max-failed-commits"));
        assertEquals("FILES_DESC", options.get("rewrite-job-order"));
        assertEquals("true", options.get("use-starting-sequence-number"));
        assertEquals("false", options.get("remove-dangling-deletes"));
        assertEquals("2", options.get("output-spec-id"));
    }

    @Test
    void toOptionsMapShouldBeEmptyForDefaults() {
        RewriteDataFilesConfig config = RewriteDataFilesConfig.builder().build();

        Map<String, String> options = config.toOptionsMap();

        assertTrue(options.isEmpty());
    }

    @Test
    void recordShouldSupportEquality() {
        RewriteDataFilesConfig config1 =
                RewriteDataFilesConfig.builder()
                        .strategy("BINPACK")
                        .targetFileSizeBytes(512L * 1024 * 1024)
                        .build();
        RewriteDataFilesConfig config2 =
                RewriteDataFilesConfig.builder()
                        .strategy("BINPACK")
                        .targetFileSizeBytes(512L * 1024 * 1024)
                        .build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    void differentConfigsShouldNotBeEqual() {
        RewriteDataFilesConfig config1 =
                RewriteDataFilesConfig.builder().strategy("BINPACK").build();
        RewriteDataFilesConfig config2 = RewriteDataFilesConfig.builder().strategy("SORT").build();

        assertNotEquals(config1, config2);
    }
}
