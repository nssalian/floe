package com.floe.core.catalog;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TableMetadataTest {

    private static final TableIdentifier TEST_ID =
            new TableIdentifier("catalog", "database", "table");
    private static final Instant NOW = Instant.now();
    private static final Instant EARLIER = NOW.minusSeconds(3600);

    private TableMetadata createTestMetadata() {
        return TableMetadata.builder()
                .identifier(TEST_ID)
                .location("/path/to/table")
                .snapshotCount(5)
                .currentSnapshotId(123456789L)
                .currentSnapshotTimestamp(NOW)
                .oldestSnapshotTimestamp(EARLIER)
                .dataFileCount(100)
                .totalDataFileSizeBytes(1073741824L) // 1 GB
                .deleteFileCount(5)
                .positionDeleteFileCount(3)
                .equalityDeleteFileCount(2)
                .totalRecordCount(1000000L)
                .manifestCount(10)
                .totalManifestSizeBytes(10485760L) // 10 MB
                .formatVersion(2)
                .partitionSpec("date (day), region (identity)")
                .sortOrder("timestamp desc nulls_last")
                .properties(Map.of("key1", "value1", "key2", "value2"))
                .lastModified(NOW)
                .build();
    }

    @Test
    public void shouldCreateTableMetadata() {
        TableMetadata metadata = createTestMetadata();

        assertEquals(TEST_ID, metadata.identifier());
        assertEquals("/path/to/table", metadata.location());
        assertEquals(5, metadata.snapshotCount());
        assertEquals(123456789L, metadata.currentSnapshotId());
        assertEquals(NOW, metadata.currentSnapshotTimestamp());
        assertEquals(EARLIER, metadata.oldestSnapshotTimestamp());
        assertEquals(100, metadata.dataFileCount());
        assertEquals(1073741824L, metadata.totalDataFileSizeBytes());
        assertEquals(5, metadata.deleteFileCount());
        assertEquals(3, metadata.positionDeleteFileCount());
        assertEquals(2, metadata.equalityDeleteFileCount());
        assertEquals(1000000L, metadata.totalRecordCount());
        assertEquals(10, metadata.manifestCount());
        assertEquals(10485760L, metadata.totalManifestSizeBytes());
        assertEquals(2, metadata.formatVersion());
        assertEquals("date (day), region (identity)", metadata.partitionSpec());
        assertEquals("timestamp desc nulls_last", metadata.sortOrder());
    }

    @Test
    public void shouldCalculateAverageDataFileSizeMb() {
        TableMetadata metadata = createTestMetadata();

        double averageSizeMb = metadata.averageDataFileSizeMb();
        assertEquals(10.24, averageSizeMb, 0.01); // 1 GB / 100 files = 10.24 MB
    }

    @Test
    public void shouldReturnZeroForAverageWhenNoFiles() {
        TableMetadata metadata =
                TableMetadata.builder()
                        .identifier(TEST_ID)
                        .location("/path/to/table")
                        .snapshotCount(0)
                        .currentSnapshotId(-1)
                        .dataFileCount(0)
                        .totalDataFileSizeBytes(0L)
                        .formatVersion(1)
                        .partitionSpec("unpartitioned")
                        .sortOrder("unsorted")
                        .properties(Map.of())
                        .build();

        assertEquals(0.0, metadata.averageDataFileSizeMb());
    }

    @Test
    public void shouldCalculateTotalDataSizeGb() {
        TableMetadata metadata =
                TableMetadata.builder()
                        .identifier(TEST_ID)
                        .location("/path/to/table")
                        .snapshotCount(5)
                        .currentSnapshotId(123456789L)
                        .currentSnapshotTimestamp(NOW)
                        .oldestSnapshotTimestamp(EARLIER)
                        .dataFileCount(100)
                        .totalDataFileSizeBytes(2147483648L) // 2 GB
                        .totalRecordCount(500000L)
                        .manifestCount(10)
                        .totalManifestSizeBytes(10485760L)
                        .formatVersion(2)
                        .partitionSpec("unpartitioned")
                        .sortOrder("unsorted")
                        .properties(Map.of())
                        .lastModified(NOW)
                        .build();

        double totalSizeGb = metadata.totalDataSizeGb();
        assertEquals(2.0, totalSizeGb, 0.01);
    }

    @Test
    public void shouldGetPropertyWithDefault() {
        TableMetadata metadata = createTestMetadata();

        assertEquals("value1", metadata.getProperty("key1", "default"));
        assertEquals("default", metadata.getProperty("nonExistent", "default"));
    }

    @Test
    public void shouldCheckIfEmpty() {
        TableMetadata emptyMetadata =
                TableMetadata.builder()
                        .identifier(TEST_ID)
                        .location("/path/to/table")
                        .snapshotCount(1)
                        .currentSnapshotId(123456789L)
                        .currentSnapshotTimestamp(NOW)
                        .oldestSnapshotTimestamp(NOW)
                        .dataFileCount(0) // no data files
                        .totalDataFileSizeBytes(0L)
                        .formatVersion(1)
                        .partitionSpec("unpartitioned")
                        .sortOrder("unsorted")
                        .properties(Map.of())
                        .lastModified(NOW)
                        .build();

        assertTrue(emptyMetadata.isEmpty());
        assertFalse(createTestMetadata().isEmpty());
    }

    @Test
    public void shouldCheckHasDeleteFiles() {
        TableMetadata withDeletes = createTestMetadata();
        assertTrue(withDeletes.hasDeleteFiles());

        TableMetadata noDeletes =
                TableMetadata.builder()
                        .identifier(TEST_ID)
                        .location("/path/to/table")
                        .snapshotCount(5)
                        .currentSnapshotId(123456789L)
                        .currentSnapshotTimestamp(NOW)
                        .oldestSnapshotTimestamp(EARLIER)
                        .dataFileCount(100)
                        .totalDataFileSizeBytes(1073741824L)
                        .deleteFileCount(0) // no delete files
                        .totalRecordCount(1000000L)
                        .manifestCount(10)
                        .totalManifestSizeBytes(10485760L)
                        .formatVersion(2)
                        .partitionSpec("unpartitioned")
                        .sortOrder("unsorted")
                        .properties(Map.of())
                        .lastModified(NOW)
                        .build();
        assertFalse(noDeletes.hasDeleteFiles());
    }

    @Test
    public void shouldCheckIsFormatV2() {
        TableMetadata v2 = createTestMetadata(); // formatVersion = 2
        assertTrue(v2.isFormatV2());

        TableMetadata v1 =
                TableMetadata.builder()
                        .identifier(TEST_ID)
                        .location("/path/to/table")
                        .snapshotCount(5)
                        .currentSnapshotId(123456789L)
                        .currentSnapshotTimestamp(NOW)
                        .oldestSnapshotTimestamp(EARLIER)
                        .dataFileCount(100)
                        .totalDataFileSizeBytes(1073741824L)
                        .totalRecordCount(1000000L)
                        .manifestCount(10)
                        .totalManifestSizeBytes(10485760L)
                        .formatVersion(1) // formatVersion = 1
                        .partitionSpec("unpartitioned")
                        .sortOrder("unsorted")
                        .properties(Map.of())
                        .lastModified(NOW)
                        .build();
        assertFalse(v1.isFormatV2());
    }

    @Test
    public void shouldCheckIsPartitioned() {
        TableMetadata partitioned = createTestMetadata();
        assertTrue(partitioned.isPartitioned());

        TableMetadata unpartitioned =
                TableMetadata.builder()
                        .identifier(TEST_ID)
                        .location("/path/to/table")
                        .snapshotCount(5)
                        .currentSnapshotId(123456789L)
                        .currentSnapshotTimestamp(NOW)
                        .oldestSnapshotTimestamp(EARLIER)
                        .dataFileCount(100)
                        .totalDataFileSizeBytes(1073741824L)
                        .totalRecordCount(1000000L)
                        .manifestCount(10)
                        .totalManifestSizeBytes(10485760L)
                        .formatVersion(2)
                        .partitionSpec("unpartitioned")
                        .sortOrder("unsorted")
                        .properties(Map.of())
                        .lastModified(NOW)
                        .build();
        assertFalse(unpartitioned.isPartitioned());

        TableMetadata emptySpec =
                TableMetadata.builder()
                        .identifier(TEST_ID)
                        .location("/path/to/table")
                        .snapshotCount(5)
                        .currentSnapshotId(123456789L)
                        .currentSnapshotTimestamp(NOW)
                        .oldestSnapshotTimestamp(EARLIER)
                        .dataFileCount(100)
                        .totalDataFileSizeBytes(1073741824L)
                        .totalRecordCount(1000000L)
                        .manifestCount(10)
                        .totalManifestSizeBytes(10485760L)
                        .formatVersion(2)
                        .partitionSpec("[]")
                        .sortOrder("unsorted")
                        .properties(Map.of())
                        .lastModified(NOW)
                        .build();
        assertFalse(emptySpec.isPartitioned());
    }

    @Test
    public void shouldCheckIsSorted() {
        TableMetadata sorted = createTestMetadata();
        assertTrue(sorted.isSorted());

        TableMetadata unsorted =
                TableMetadata.builder()
                        .identifier(TEST_ID)
                        .location("/path/to/table")
                        .snapshotCount(5)
                        .currentSnapshotId(123456789L)
                        .currentSnapshotTimestamp(NOW)
                        .oldestSnapshotTimestamp(EARLIER)
                        .dataFileCount(100)
                        .totalDataFileSizeBytes(1073741824L)
                        .totalRecordCount(1000000L)
                        .manifestCount(10)
                        .totalManifestSizeBytes(10485760L)
                        .formatVersion(2)
                        .partitionSpec("date (day)")
                        .sortOrder("unsorted")
                        .properties(Map.of())
                        .lastModified(NOW)
                        .build();
        assertFalse(unsorted.isSorted());
    }
}
