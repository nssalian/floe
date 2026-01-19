package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.policy.RewriteManifestsConfig.ManifestFilterConfig;
import com.floe.core.policy.RewriteManifestsConfig.ManifestFilterConfig.PartitionFieldSummaryConfig;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("RewriteManifestsConfig")
class RewriteManifestsConfigTest {

    @Test
    void defaultsShouldHaveNullValues() {
        RewriteManifestsConfig config = RewriteManifestsConfig.defaults();

        assertNull(config.specId());
        assertNull(config.stagingLocation());
        assertNull(config.sortBy());
    }

    @Test
    void builderShouldCreateConfigWithAllFields() {
        RewriteManifestsConfig config =
                RewriteManifestsConfig.builder()
                        .specId(1)
                        .stagingLocation("/data/staging")
                        .sortBy(List.of("date", "region"))
                        .build();

        assertEquals(1, config.specId());
        assertEquals("/data/staging", config.stagingLocation());
        assertEquals(List.of("date", "region"), config.sortBy());
    }

    @Test
    void builderShouldAllowPartialConfiguration() {
        RewriteManifestsConfig config = RewriteManifestsConfig.builder().specId(2).build();

        assertEquals(2, config.specId());
        assertNull(config.stagingLocation());
        assertNull(config.sortBy());
    }

    @Test
    void builderShouldDefaultToNullValues() {
        RewriteManifestsConfig config = RewriteManifestsConfig.builder().build();

        assertNull(config.specId());
        assertNull(config.stagingLocation());
        assertNull(config.sortBy());
    }

    @Test
    void recordShouldSupportEquality() {
        RewriteManifestsConfig config1 =
                RewriteManifestsConfig.builder().specId(1).stagingLocation("/tmp").build();
        RewriteManifestsConfig config2 =
                RewriteManifestsConfig.builder().specId(1).stagingLocation("/tmp").build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    void differentConfigsShouldNotBeEqual() {
        RewriteManifestsConfig config1 = RewriteManifestsConfig.builder().specId(1).build();
        RewriteManifestsConfig config2 = RewriteManifestsConfig.builder().specId(2).build();

        assertNotEquals(config1, config2);
    }

    @Test
    void shouldCreateWithRecordConstructor() {
        RewriteManifestsConfig config =
                new RewriteManifestsConfig(1, "/staging", List.of("date"), null);

        assertEquals(1, config.specId());
        assertEquals("/staging", config.stagingLocation());
        assertEquals(List.of("date"), config.sortBy());
    }

    @Test
    void recordShouldHaveToString() {
        RewriteManifestsConfig config = RewriteManifestsConfig.builder().specId(1).build();
        String toString = config.toString();

        assertTrue(toString.contains("RewriteManifestsConfig"));
        assertTrue(toString.contains("1"));
    }

    @Test
    void sortByShouldBeImmutable() {
        List<String> fields = List.of("date", "region");
        RewriteManifestsConfig config = RewriteManifestsConfig.builder().sortBy(fields).build();

        assertEquals(fields, config.sortBy());
    }

    @Test
    void builderShouldSetRewriteIf() {
        ManifestFilterConfig filter =
                ManifestFilterConfig.builder().specId(1).content("DATA").build();

        RewriteManifestsConfig config = RewriteManifestsConfig.builder().rewriteIf(filter).build();

        assertNotNull(config.rewriteIf());
        assertEquals(1, config.rewriteIf().specId());
        assertEquals("DATA", config.rewriteIf().content());
    }

    @Test
    void shouldCreateConfigWithAllFieldsIncludingRewriteIf() {
        ManifestFilterConfig filter =
                ManifestFilterConfig.builder()
                        .path("/data/manifest.avro")
                        .length(1024L)
                        .specId(0)
                        .content("DATA")
                        .sequenceNumber(100L)
                        .minSequenceNumber(50L)
                        .snapshotId(999L)
                        .addedFilesCount(10)
                        .existingFilesCount(5)
                        .deletedFilesCount(2)
                        .addedRowsCount(1000L)
                        .existingRowsCount(500L)
                        .deletedRowsCount(200L)
                        .firstRowId(1L)
                        .keyMetadata("0a1b2c")
                        .build();

        RewriteManifestsConfig config =
                RewriteManifestsConfig.builder()
                        .specId(1)
                        .stagingLocation("/staging")
                        .sortBy(List.of("date"))
                        .rewriteIf(filter)
                        .build();

        assertEquals(1, config.specId());
        assertEquals("/staging", config.stagingLocation());
        assertEquals(List.of("date"), config.sortBy());
        assertNotNull(config.rewriteIf());
    }

    @Nested
    @DisplayName("ManifestFilterConfig")
    class ManifestFilterConfigTests {

        @Test
        @DisplayName("empty() should create filter with all null values")
        void emptyShouldCreateFilterWithAllNullValues() {
            ManifestFilterConfig filter = ManifestFilterConfig.empty();

            assertNull(filter.path());
            assertNull(filter.length());
            assertNull(filter.specId());
            assertNull(filter.content());
            assertNull(filter.sequenceNumber());
            assertNull(filter.minSequenceNumber());
            assertNull(filter.snapshotId());
            assertNull(filter.addedFilesCount());
            assertNull(filter.existingFilesCount());
            assertNull(filter.deletedFilesCount());
            assertNull(filter.addedRowsCount());
            assertNull(filter.existingRowsCount());
            assertNull(filter.deletedRowsCount());
            assertNull(filter.firstRowId());
            assertNull(filter.keyMetadata());
            assertNull(filter.partitionSummaries());
        }

        @Test
        @DisplayName("builder should set path field")
        void builderShouldSetPath() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().path("/data/manifest-001.avro").build();

            assertEquals("/data/manifest-001.avro", filter.path());
        }

        @Test
        @DisplayName("builder should set length field")
        void builderShouldSetLength() {
            ManifestFilterConfig filter = ManifestFilterConfig.builder().length(2048L).build();

            assertEquals(2048L, filter.length());
        }

        @Test
        @DisplayName("builder should set specId field")
        void builderShouldSetSpecId() {
            ManifestFilterConfig filter = ManifestFilterConfig.builder().specId(2).build();

            assertEquals(2, filter.specId());
        }

        @Test
        @DisplayName("builder should set content field (DATA or DELETES)")
        void builderShouldSetContent() {
            ManifestFilterConfig dataFilter =
                    ManifestFilterConfig.builder().content("DATA").build();
            ManifestFilterConfig deletesFilter =
                    ManifestFilterConfig.builder().content("DELETES").build();

            assertEquals("DATA", dataFilter.content());
            assertEquals("DELETES", deletesFilter.content());
        }

        @Test
        @DisplayName("builder should set sequenceNumber field")
        void builderShouldSetSequenceNumber() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().sequenceNumber(12345L).build();

            assertEquals(12345L, filter.sequenceNumber());
        }

        @Test
        @DisplayName("builder should set minSequenceNumber field")
        void builderShouldSetMinSequenceNumber() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().minSequenceNumber(100L).build();

            assertEquals(100L, filter.minSequenceNumber());
        }

        @Test
        @DisplayName("builder should set snapshotId field")
        void builderShouldSetSnapshotId() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().snapshotId(9876543210L).build();

            assertEquals(9876543210L, filter.snapshotId());
        }

        @Test
        @DisplayName("builder should set addedFilesCount field")
        void builderShouldSetAddedFilesCount() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().addedFilesCount(42).build();

            assertEquals(42, filter.addedFilesCount());
        }

        @Test
        @DisplayName("builder should set existingFilesCount field")
        void builderShouldSetExistingFilesCount() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().existingFilesCount(100).build();

            assertEquals(100, filter.existingFilesCount());
        }

        @Test
        @DisplayName("builder should set deletedFilesCount field")
        void builderShouldSetDeletedFilesCount() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().deletedFilesCount(5).build();

            assertEquals(5, filter.deletedFilesCount());
        }

        @Test
        @DisplayName("builder should set addedRowsCount field")
        void builderShouldSetAddedRowsCount() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().addedRowsCount(100000L).build();

            assertEquals(100000L, filter.addedRowsCount());
        }

        @Test
        @DisplayName("builder should set existingRowsCount field")
        void builderShouldSetExistingRowsCount() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().existingRowsCount(500000L).build();

            assertEquals(500000L, filter.existingRowsCount());
        }

        @Test
        @DisplayName("builder should set deletedRowsCount field")
        void builderShouldSetDeletedRowsCount() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().deletedRowsCount(1000L).build();

            assertEquals(1000L, filter.deletedRowsCount());
        }

        @Test
        @DisplayName("builder should set firstRowId field")
        void builderShouldSetFirstRowId() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().firstRowId(12345678L).build();

            assertEquals(12345678L, filter.firstRowId());
        }

        @Test
        @DisplayName("builder should set keyMetadata field (hex encoded)")
        void builderShouldSetKeyMetadata() {
            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().keyMetadata("0a1b2c3d4e5f").build();

            assertEquals("0a1b2c3d4e5f", filter.keyMetadata());
        }

        @Test
        @DisplayName("builder should set partitionSummaries field")
        void builderShouldSetPartitionSummaries() {
            List<PartitionFieldSummaryConfig> summaries =
                    List.of(
                            new PartitionFieldSummaryConfig(true, false, "00", "ff"),
                            new PartitionFieldSummaryConfig(false, null, null, null));

            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder().partitionSummaries(summaries).build();

            assertNotNull(filter.partitionSummaries());
            assertEquals(2, filter.partitionSummaries().size());
            assertTrue(filter.partitionSummaries().get(0).containsNull());
            assertFalse(filter.partitionSummaries().get(1).containsNull());
        }

        @Test
        @DisplayName("builder should set all 16 ManifestFile fields")
        void builderShouldSetAllFields() {
            List<PartitionFieldSummaryConfig> summaries =
                    List.of(PartitionFieldSummaryConfig.of(true));

            ManifestFilterConfig filter =
                    ManifestFilterConfig.builder()
                            .path("/data/manifest.avro")
                            .length(4096L)
                            .specId(0)
                            .content("DATA")
                            .sequenceNumber(1L)
                            .minSequenceNumber(1L)
                            .snapshotId(123456789L)
                            .addedFilesCount(10)
                            .existingFilesCount(20)
                            .deletedFilesCount(5)
                            .addedRowsCount(10000L)
                            .existingRowsCount(20000L)
                            .deletedRowsCount(5000L)
                            .firstRowId(1L)
                            .keyMetadata("deadbeef")
                            .partitionSummaries(summaries)
                            .build();

            assertEquals("/data/manifest.avro", filter.path());
            assertEquals(4096L, filter.length());
            assertEquals(0, filter.specId());
            assertEquals("DATA", filter.content());
            assertEquals(1L, filter.sequenceNumber());
            assertEquals(1L, filter.minSequenceNumber());
            assertEquals(123456789L, filter.snapshotId());
            assertEquals(10, filter.addedFilesCount());
            assertEquals(20, filter.existingFilesCount());
            assertEquals(5, filter.deletedFilesCount());
            assertEquals(10000L, filter.addedRowsCount());
            assertEquals(20000L, filter.existingRowsCount());
            assertEquals(5000L, filter.deletedRowsCount());
            assertEquals(1L, filter.firstRowId());
            assertEquals("deadbeef", filter.keyMetadata());
            assertEquals(1, filter.partitionSummaries().size());
        }

        @Test
        @DisplayName("record should support equality")
        void recordShouldSupportEquality() {
            ManifestFilterConfig filter1 =
                    ManifestFilterConfig.builder().specId(1).content("DATA").build();
            ManifestFilterConfig filter2 =
                    ManifestFilterConfig.builder().specId(1).content("DATA").build();

            assertEquals(filter1, filter2);
            assertEquals(filter1.hashCode(), filter2.hashCode());
        }

        @Test
        @DisplayName("different filters should not be equal")
        void differentFiltersShouldNotBeEqual() {
            ManifestFilterConfig filter1 = ManifestFilterConfig.builder().content("DATA").build();
            ManifestFilterConfig filter2 =
                    ManifestFilterConfig.builder().content("DELETES").build();

            assertNotEquals(filter1, filter2);
        }

        @Test
        @DisplayName("should create with record constructor")
        void shouldCreateWithRecordConstructor() {
            ManifestFilterConfig filter =
                    new ManifestFilterConfig(
                            "/path", 1024L, 0, "DATA", 1L, 1L, 999L, 10, 5, 2, 1000L, 500L, 200L,
                            1L, "abc123", null);

            assertEquals("/path", filter.path());
            assertEquals(1024L, filter.length());
            assertEquals(0, filter.specId());
            assertEquals("DATA", filter.content());
        }
    }

    @Nested
    @DisplayName("PartitionFieldSummaryConfig")
    class PartitionFieldSummaryConfigTests {

        @Test
        @DisplayName("of() should create summary with containsNull only")
        void ofShouldCreateSummaryWithContainsNullOnly() {
            PartitionFieldSummaryConfig summary = PartitionFieldSummaryConfig.of(true);

            assertTrue(summary.containsNull());
            assertNull(summary.containsNan());
            assertNull(summary.lowerBound());
            assertNull(summary.upperBound());
        }

        @Test
        @DisplayName("should create summary with all fields")
        void shouldCreateSummaryWithAllFields() {
            PartitionFieldSummaryConfig summary =
                    new PartitionFieldSummaryConfig(true, false, "00000000", "ffffffff");

            assertTrue(summary.containsNull());
            assertFalse(summary.containsNan());
            assertEquals("00000000", summary.lowerBound());
            assertEquals("ffffffff", summary.upperBound());
        }

        @Test
        @DisplayName("should handle null optional fields")
        void shouldHandleNullOptionalFields() {
            PartitionFieldSummaryConfig summary =
                    new PartitionFieldSummaryConfig(false, null, null, null);

            assertFalse(summary.containsNull());
            assertNull(summary.containsNan());
            assertNull(summary.lowerBound());
            assertNull(summary.upperBound());
        }

        @Test
        @DisplayName("record should support equality")
        void recordShouldSupportEquality() {
            PartitionFieldSummaryConfig summary1 =
                    new PartitionFieldSummaryConfig(true, false, "00", "ff");
            PartitionFieldSummaryConfig summary2 =
                    new PartitionFieldSummaryConfig(true, false, "00", "ff");

            assertEquals(summary1, summary2);
            assertEquals(summary1.hashCode(), summary2.hashCode());
        }

        @Test
        @DisplayName("different summaries should not be equal")
        void differentSummariesShouldNotBeEqual() {
            PartitionFieldSummaryConfig summary1 = PartitionFieldSummaryConfig.of(true);
            PartitionFieldSummaryConfig summary2 = PartitionFieldSummaryConfig.of(false);

            assertNotEquals(summary1, summary2);
        }

        @Test
        @DisplayName("should have meaningful toString")
        void shouldHaveMeaningfulToString() {
            PartitionFieldSummaryConfig summary =
                    new PartitionFieldSummaryConfig(true, false, "00", "ff");

            String toString = summary.toString();

            assertTrue(toString.contains("PartitionFieldSummaryConfig"));
            assertTrue(toString.contains("true"));
        }
    }
}
