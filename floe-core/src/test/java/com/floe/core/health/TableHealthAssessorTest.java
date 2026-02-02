package com.floe.core.health;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.TableIdentifier;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableHealthAssessorTest {

    private static final TableIdentifier TABLE_ID = new TableIdentifier("catalog", "db", "events");

    private TableHealthAssessor assessor;
    private Table mockTable;
    private Snapshot mockSnapshot;
    private FileIO mockFileIO;

    @BeforeEach
    void setUp() {
        assessor = new TableHealthAssessor(HealthThresholds.defaults());
        mockTable = mock(Table.class);
        mockSnapshot = mock(Snapshot.class);
        mockFileIO = mock(FileIO.class);

        when(mockTable.io()).thenReturn(mockFileIO);
    }

    @Test
    void assessEmptyTable() {
        when(mockTable.snapshots()).thenReturn(List.of());
        when(mockTable.currentSnapshot()).thenReturn(null);

        HealthReport report = assessor.assess(TABLE_ID, mockTable);

        assertEquals(TABLE_ID, report.tableIdentifier());
        assertEquals(0, report.snapshotCount());
        assertEquals(0, report.dataFileCount());
        assertTrue(report.isEmpty());
        assertTrue(report.hasIssues());
        assertEquals(1, report.getIssuesOfType(HealthIssue.Type.TABLE_EMPTY).size());
    }

    @Test
    void assessTableWithSnapshots() {
        Instant now = Instant.now();
        Instant fiveDaysAgo = now.minus(5, ChronoUnit.DAYS);
        Instant twoDaysAgo = now.minus(2, ChronoUnit.DAYS);

        Snapshot oldSnapshot = mock(Snapshot.class);
        Snapshot newSnapshot = mock(Snapshot.class);
        when(oldSnapshot.timestampMillis()).thenReturn(fiveDaysAgo.toEpochMilli());
        when(newSnapshot.timestampMillis()).thenReturn(twoDaysAgo.toEpochMilli());

        when(mockTable.snapshots()).thenReturn(List.of(oldSnapshot, newSnapshot));
        when(mockTable.currentSnapshot()).thenReturn(null);

        HealthReport report = assessor.assess(TABLE_ID, mockTable);

        assertEquals(2, report.snapshotCount());
        assertTrue(report.oldestSnapshotTimestamp().isPresent());
        assertTrue(report.newestSnapshotTimestamp().isPresent());
        assertEquals(5, report.oldestSnapshotAge().get().toDays());
    }

    @Test
    void assessTableWithSmallFiles() {
        setupTableWithFiles(100, 10 * 1024 * 1024); // 100 files, 10MB each

        HealthReport report = assessor.assess(TABLE_ID, mockTable);

        assertEquals(100, report.dataFileCount());
        assertEquals(100, report.smallFileCount()); // All files are small
        assertTrue(report.hasIssues());
        assertTrue(report.hasIssuesOfSeverity(HealthIssue.Severity.CRITICAL));
        assertFalse(report.getIssuesOfType(HealthIssue.Type.TOO_MANY_SMALL_FILES).isEmpty());
    }

    @Test
    void assessTableWithHealthyFiles() {
        setupTableWithFiles(50, 256 * 1024 * 1024); // 50 files, 256MB each

        HealthReport report = assessor.assess(TABLE_ID, mockTable);

        assertEquals(50, report.dataFileCount());
        assertEquals(0, report.smallFileCount());
        assertFalse(report.hasIssuesOfSeverity(HealthIssue.Severity.CRITICAL));
    }

    @Test
    void assessTableWithTooManySnapshots() {
        // Create 150 snapshots (above warning threshold of 100)
        List<Snapshot> snapshots = createMockSnapshots(150);
        when(mockTable.snapshots()).thenReturn(snapshots);

        // Need to set up files too, otherwise we get TABLE_EMPTY and return early
        setupTableWithFiles(10, 256 * 1024 * 1024); // 10 healthy files

        // Override snapshots after setupTableWithFiles
        when(mockTable.snapshots()).thenReturn(snapshots);

        HealthReport report = assessor.assess(TABLE_ID, mockTable);

        assertEquals(150, report.snapshotCount());
        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.TOO_MANY_SNAPSHOTS).isEmpty());
    }

    @Test
    void assessTableWithOldSnapshots() {
        Instant now = Instant.now();
        Instant thirtyDaysAgo = now.minus(30, ChronoUnit.DAYS);

        Snapshot oldSnapshot = mock(Snapshot.class);
        when(oldSnapshot.timestampMillis()).thenReturn(thirtyDaysAgo.toEpochMilli());

        // Need to set up files too
        setupTableWithFiles(10, 256 * 1024 * 1024);

        // Override snapshots after setupTableWithFiles
        when(mockTable.snapshots()).thenReturn(List.of(oldSnapshot));

        HealthReport report = assessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.OLD_SNAPSHOTS).isEmpty());
    }

    @Test
    void assessWithCustomThresholds() {
        HealthThresholds strict =
                HealthThresholds.builder()
                        .smallFileSizeBytes(200 * 1024 * 1024) // 200MB threshold
                        .smallFilePercentWarning(10.0)
                        .smallFilePercentCritical(20.0)
                        .build();

        TableHealthAssessor strictAssessor = new TableHealthAssessor(strict);
        setupTableWithFiles(100, 150 * 1024 * 1024); // 100 files, 150MB each

        HealthReport report = strictAssessor.assess(TABLE_ID, mockTable);

        // All files are under 200MB threshold, so all are "small"
        assertEquals(100, report.smallFileCount());
        assertTrue(report.hasIssuesOfSeverity(HealthIssue.Severity.CRITICAL));
    }

    @Test
    void reportIncludesAssessmentTimestamp() {
        when(mockTable.snapshots()).thenReturn(List.of());
        when(mockTable.currentSnapshot()).thenReturn(null);

        Instant before = Instant.now();
        HealthReport report = assessor.assess(TABLE_ID, mockTable);
        Instant after = Instant.now();

        assertNotNull(report.assessedAt());
        assertFalse(report.assessedAt().isBefore(before));
        assertFalse(report.assessedAt().isAfter(after));
    }

    // Helper methods

    private void setupTableWithFiles(int fileCount, long fileSizeBytes) {
        when(mockTable.snapshots()).thenReturn(List.of(mockSnapshot));
        when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
        when(mockSnapshot.timestampMillis()).thenReturn(Instant.now().toEpochMilli());
        when(mockSnapshot.allManifests(mockFileIO)).thenReturn(List.of());

        // Create mock scan
        var mockScan = mock(org.apache.iceberg.TableScan.class);
        when(mockTable.newScan()).thenReturn(mockScan);

        List<FileScanTask> tasks = createMockFileScanTasks(fileCount, fileSizeBytes);
        CloseableIterable<FileScanTask> iterable = mockCloseableIterable(tasks);
        when(mockScan.planFiles()).thenReturn(iterable);
    }

    private List<FileScanTask> createMockFileScanTasks(int count, long fileSizeBytes) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(
                        i -> {
                            FileScanTask task = mock(FileScanTask.class);
                            DataFile dataFile = mock(DataFile.class);
                            when(dataFile.fileSizeInBytes()).thenReturn(fileSizeBytes);
                            when(dataFile.partition()).thenReturn(null);
                            when(task.file()).thenReturn(dataFile);
                            when(task.deletes()).thenReturn(List.of());
                            return task;
                        })
                .toList();
    }

    private List<Snapshot> createMockSnapshots(int count) {
        Instant now = Instant.now();
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(
                        i -> {
                            Snapshot snapshot = mock(Snapshot.class);
                            when(snapshot.timestampMillis())
                                    .thenReturn(now.minus(i, ChronoUnit.HOURS).toEpochMilli());
                            return snapshot;
                        })
                .toList();
    }

    @SuppressWarnings("unchecked")
    private <T> CloseableIterable<T> mockCloseableIterable(List<T> items) {
        CloseableIterable<T> iterable = mock(CloseableIterable.class);
        CloseableIterator<T> iterator = mock(CloseableIterator.class);

        Iterator<T> realIterator = items.iterator();
        when(iterator.hasNext()).thenAnswer(inv -> realIterator.hasNext());
        when(iterator.next()).thenAnswer(inv -> realIterator.next());

        when(iterable.iterator()).thenReturn(iterator);
        return iterable;
    }

    @Test
    void assessHighDeleteFileRatioWarning() {
        // Set up table with 10% delete file ratio (warning threshold)
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .deleteFileRatioWarning(0.10)
                        .deleteFileRatioCritical(0.25)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        setupTableWithFilesAndDeletes(100, 256 * 1024 * 1024, 15); // 15% ratio

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.HIGH_DELETE_FILE_RATIO).isEmpty());
        assertEquals(
                HealthIssue.Severity.WARNING,
                report.getIssuesOfType(HealthIssue.Type.HIGH_DELETE_FILE_RATIO).get(0).severity());
    }

    @Test
    void assessHighDeleteFileRatioCritical() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .deleteFileRatioWarning(0.10)
                        .deleteFileRatioCritical(0.25)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        setupTableWithFilesAndDeletes(100, 256 * 1024 * 1024, 30); // 30% ratio

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.HIGH_DELETE_FILE_RATIO).isEmpty());
        assertEquals(
                HealthIssue.Severity.CRITICAL,
                report.getIssuesOfType(HealthIssue.Type.HIGH_DELETE_FILE_RATIO).get(0).severity());
    }

    @Test
    void assessPartitionSkewWarning() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .partitionSkewWarning(5.0)
                        .partitionSkewCritical(10.0)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        setupTableWithPartitionSkew(30, 1, 5, 256 * 1024 * 1024);

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertFalse(report.getIssuesOfType(HealthIssue.Type.PARTITION_SKEW).isEmpty());
        assertEquals(
                HealthIssue.Severity.WARNING,
                report.getIssuesOfType(HealthIssue.Type.PARTITION_SKEW).get(0).severity());
    }

    @Test
    void assessPartitionSkewCritical() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .partitionSkewWarning(5.0)
                        .partitionSkewCritical(10.0)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        setupTableWithPartitionSkew(100, 1, 50, 256 * 1024 * 1024);

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertFalse(report.getIssuesOfType(HealthIssue.Type.PARTITION_SKEW).isEmpty());
        assertEquals(
                HealthIssue.Severity.CRITICAL,
                report.getIssuesOfType(HealthIssue.Type.PARTITION_SKEW).get(0).severity());
    }

    @Test
    void assessHighDeleteFileRatioNoIssue() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .deleteFileRatioWarning(0.10)
                        .deleteFileRatioCritical(0.25)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        setupTableWithFilesAndDeletes(100, 256 * 1024 * 1024, 5); // 5% ratio

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.getIssuesOfType(HealthIssue.Type.HIGH_DELETE_FILE_RATIO).isEmpty());
    }

    @Test
    void assessTooManyPartitionsWarning() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .partitionCountWarning(100)
                        .partitionCountCritical(500)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        setupTableWithPartitions(200); // 200 partitions > 100 warning

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.TOO_MANY_PARTITIONS).isEmpty());
        assertEquals(
                HealthIssue.Severity.WARNING,
                report.getIssuesOfType(HealthIssue.Type.TOO_MANY_PARTITIONS).get(0).severity());
    }

    @Test
    void assessTooManyPartitionsCritical() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .partitionCountWarning(100)
                        .partitionCountCritical(500)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        setupTableWithPartitions(600); // 600 partitions > 500 critical

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.TOO_MANY_PARTITIONS).isEmpty());
        assertEquals(
                HealthIssue.Severity.CRITICAL,
                report.getIssuesOfType(HealthIssue.Type.TOO_MANY_PARTITIONS).get(0).severity());
    }

    @Test
    void assessLargeManifestListWarning() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .manifestSizeWarningBytes(100 * 1024 * 1024)
                        .manifestSizeCriticalBytes(500 * 1024 * 1024)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        setupTableWithManifestSize(150 * 1024 * 1024); // 150MB > 100MB warning

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.LARGE_MANIFEST_LIST).isEmpty());
        assertEquals(
                HealthIssue.Severity.WARNING,
                report.getIssuesOfType(HealthIssue.Type.LARGE_MANIFEST_LIST).get(0).severity());
    }

    @Test
    void assessLargeManifestListCritical() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .manifestSizeWarningBytes(100 * 1024 * 1024)
                        .manifestSizeCriticalBytes(500 * 1024 * 1024)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        setupTableWithManifestSize(600 * 1024 * 1024); // 600MB > 500MB critical

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.LARGE_MANIFEST_LIST).isEmpty());
        assertEquals(
                HealthIssue.Severity.CRITICAL,
                report.getIssuesOfType(HealthIssue.Type.LARGE_MANIFEST_LIST).get(0).severity());
    }

    @Test
    void assessStaleMetadataWarning() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .staleMetadataWarningDays(7)
                        .staleMetadataCriticalDays(30)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        // Set up table with newest snapshot 10 days ago
        Instant tenDaysAgo = Instant.now().minus(10, ChronoUnit.DAYS);
        setupTableWithNewestSnapshot(tenDaysAgo);

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.STALE_METADATA).isEmpty());
        assertEquals(
                HealthIssue.Severity.WARNING,
                report.getIssuesOfType(HealthIssue.Type.STALE_METADATA).get(0).severity());
    }

    @Test
    void assessStaleMetadataCritical() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .staleMetadataWarningDays(7)
                        .staleMetadataCriticalDays(30)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        // Set up table with newest snapshot 35 days ago
        Instant thirtyFiveDaysAgo = Instant.now().minus(35, ChronoUnit.DAYS);
        setupTableWithNewestSnapshot(thirtyFiveDaysAgo);

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.STALE_METADATA).isEmpty());
        assertEquals(
                HealthIssue.Severity.CRITICAL,
                report.getIssuesOfType(HealthIssue.Type.STALE_METADATA).get(0).severity());
    }

    @Test
    void assessMultipleIssuesDetected() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .deleteFileRatioWarning(0.10)
                        .deleteFileRatioCritical(0.25)
                        .partitionCountWarning(100)
                        .partitionCountCritical(500)
                        .manifestSizeWarningBytes(100 * 1024 * 1024)
                        .manifestSizeCriticalBytes(500 * 1024 * 1024)
                        .build();
        TableHealthAssessor customAssessor = new TableHealthAssessor(thresholds);

        // Setup table with multiple issues
        setupTableWithMultipleIssues(
                100, // file count
                256 * 1024 * 1024, // file size
                30, // delete files (30% ratio)
                200, // partitions
                150 * 1024 * 1024 // manifest size
                );

        HealthReport report = customAssessor.assess(TABLE_ID, mockTable);

        assertTrue(report.hasIssues());
        // Should detect multiple issue types
        assertFalse(report.getIssuesOfType(HealthIssue.Type.HIGH_DELETE_FILE_RATIO).isEmpty());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.TOO_MANY_PARTITIONS).isEmpty());
        assertFalse(report.getIssuesOfType(HealthIssue.Type.LARGE_MANIFEST_LIST).isEmpty());
    }

    @Test
    void assessHealthyTableNoIssues() {
        setupTableWithFiles(50, 256 * 1024 * 1024); // 50 healthy files

        HealthReport report = assessor.assess(TABLE_ID, mockTable);

        // Should have no critical or warning issues for a well-maintained table
        assertFalse(report.hasIssuesOfSeverity(HealthIssue.Severity.CRITICAL));
        assertFalse(report.hasIssuesOfSeverity(HealthIssue.Severity.WARNING));
    }

    // Additional helper methods for new tests

    private void setupTableWithFilesAndDeletes(int fileCount, long fileSizeBytes, int deleteFiles) {
        when(mockTable.snapshots()).thenReturn(List.of(mockSnapshot));
        when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
        when(mockSnapshot.timestampMillis()).thenReturn(Instant.now().toEpochMilli());
        when(mockSnapshot.allManifests(mockFileIO)).thenReturn(List.of());

        var mockScan = mock(org.apache.iceberg.TableScan.class);
        when(mockTable.newScan()).thenReturn(mockScan);

        List<FileScanTask> tasks =
                createMockFileScanTasksWithDeletes(fileCount, fileSizeBytes, deleteFiles);
        CloseableIterable<FileScanTask> iterable = mockCloseableIterable(tasks);
        when(mockScan.planFiles()).thenReturn(iterable);
    }

    private List<FileScanTask> createMockFileScanTasksWithDeletes(
            int dataFileCount, long fileSizeBytes, int deleteFileCount) {
        int deletesPerDataFile = deleteFileCount / Math.max(dataFileCount, 1);
        int remainingDeletes = deleteFileCount % Math.max(dataFileCount, 1);

        return java.util.stream.IntStream.range(0, dataFileCount)
                .mapToObj(
                        i -> {
                            FileScanTask task = mock(FileScanTask.class);
                            DataFile dataFile = mock(DataFile.class);
                            when(dataFile.fileSizeInBytes()).thenReturn(fileSizeBytes);
                            when(dataFile.partition()).thenReturn(null);
                            when(task.file()).thenReturn(dataFile);

                            // Distribute delete files across data files
                            int numDeletes = deletesPerDataFile + (i < remainingDeletes ? 1 : 0);
                            List<DeleteFile> deletes =
                                    java.util.stream.IntStream.range(0, numDeletes)
                                            .mapToObj(
                                                    j -> {
                                                        DeleteFile deleteFile =
                                                                mock(DeleteFile.class);
                                                        when(deleteFile.content())
                                                                .thenReturn(
                                                                        org.apache.iceberg
                                                                                .FileContent
                                                                                .POSITION_DELETES);
                                                        return deleteFile;
                                                    })
                                            .toList();
                            when(task.deletes()).thenReturn(deletes);
                            return task;
                        })
                .toList();
    }

    private void setupTableWithPartitions(int partitionCount) {
        when(mockTable.snapshots()).thenReturn(List.of(mockSnapshot));
        when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
        when(mockSnapshot.timestampMillis()).thenReturn(Instant.now().toEpochMilli());
        when(mockSnapshot.allManifests(mockFileIO)).thenReturn(List.of());

        var mockScan = mock(org.apache.iceberg.TableScan.class);
        when(mockTable.newScan()).thenReturn(mockScan);

        List<FileScanTask> tasks = createMockFileScanTasksWithPartitions(partitionCount);
        CloseableIterable<FileScanTask> iterable = mockCloseableIterable(tasks);
        when(mockScan.planFiles()).thenReturn(iterable);
    }

    private void setupTableWithPartitionSkew(
            int largePartitionCount,
            int smallPartitionCount,
            int smallPartitions,
            long fileSizeBytes) {
        when(mockTable.snapshots()).thenReturn(List.of(mockSnapshot));
        when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
        when(mockSnapshot.timestampMillis()).thenReturn(Instant.now().toEpochMilli());
        when(mockSnapshot.allManifests(mockFileIO)).thenReturn(List.of());

        var mockScan = mock(org.apache.iceberg.TableScan.class);
        when(mockTable.newScan()).thenReturn(mockScan);

        List<FileScanTask> tasks =
                createMockFileScanTasksWithPartitionSkew(
                        largePartitionCount, smallPartitionCount, smallPartitions, fileSizeBytes);
        CloseableIterable<FileScanTask> iterable = mockCloseableIterable(tasks);
        when(mockScan.planFiles()).thenReturn(iterable);
    }

    private List<FileScanTask> createMockFileScanTasksWithPartitions(int partitionCount) {
        // Create one file per partition for simplicity
        return java.util.stream.IntStream.range(0, partitionCount)
                .mapToObj(
                        i -> {
                            FileScanTask task = mock(FileScanTask.class);
                            DataFile dataFile = mock(DataFile.class);
                            when(dataFile.fileSizeInBytes()).thenReturn(256L * 1024 * 1024);
                            // Mock partition with unique string representation
                            org.apache.iceberg.StructLike partition =
                                    mock(org.apache.iceberg.StructLike.class);
                            when(partition.toString()).thenReturn("partition-" + i);
                            when(dataFile.partition()).thenReturn(partition);
                            when(task.file()).thenReturn(dataFile);
                            when(task.deletes()).thenReturn(List.of());
                            return task;
                        })
                .toList();
    }

    private List<FileScanTask> createMockFileScanTasksWithPartitionSkew(
            int largePartitionCount,
            int smallPartitionCount,
            int smallPartitions,
            long fileSizeBytes) {
        List<FileScanTask> tasks = new java.util.ArrayList<>();

        tasks.addAll(
                createMockTasksForPartition("partition-large", largePartitionCount, fileSizeBytes));
        for (int i = 1; i <= smallPartitions; i++) {
            tasks.addAll(
                    createMockTasksForPartition(
                            "partition-" + i, smallPartitionCount, fileSizeBytes));
        }

        return tasks;
    }

    private List<FileScanTask> createMockTasksForPartition(
            String partitionKey, int count, long fileSizeBytes) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(
                        i -> {
                            FileScanTask task = mock(FileScanTask.class);
                            DataFile dataFile = mock(DataFile.class);
                            when(dataFile.fileSizeInBytes()).thenReturn(fileSizeBytes);
                            org.apache.iceberg.StructLike partition =
                                    mock(org.apache.iceberg.StructLike.class);
                            when(partition.toString()).thenReturn(partitionKey);
                            when(dataFile.partition()).thenReturn(partition);
                            when(task.file()).thenReturn(dataFile);
                            when(task.deletes()).thenReturn(List.of());
                            return task;
                        })
                .toList();
    }

    private void setupTableWithManifestSize(long totalManifestSizeBytes) {
        when(mockTable.snapshots()).thenReturn(List.of(mockSnapshot));
        when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
        when(mockSnapshot.timestampMillis()).thenReturn(Instant.now().toEpochMilli());

        // Create manifests with the specified total size
        int manifestCount = 10;
        long sizePerManifest = totalManifestSizeBytes / manifestCount;

        List<org.apache.iceberg.ManifestFile> manifests =
                java.util.stream.IntStream.range(0, manifestCount)
                        .mapToObj(
                                i -> {
                                    org.apache.iceberg.ManifestFile manifest =
                                            mock(org.apache.iceberg.ManifestFile.class);
                                    when(manifest.length()).thenReturn(sizePerManifest);
                                    return manifest;
                                })
                        .toList();
        when(mockSnapshot.allManifests(mockFileIO)).thenReturn(manifests);

        var mockScan = mock(org.apache.iceberg.TableScan.class);
        when(mockTable.newScan()).thenReturn(mockScan);

        // Need at least one data file to avoid TABLE_EMPTY issue
        List<FileScanTask> tasks = createMockFileScanTasks(10, 256 * 1024 * 1024);
        CloseableIterable<FileScanTask> iterable = mockCloseableIterable(tasks);
        when(mockScan.planFiles()).thenReturn(iterable);
    }

    private void setupTableWithNewestSnapshot(Instant newestSnapshotTime) {
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.timestampMillis()).thenReturn(newestSnapshotTime.toEpochMilli());

        when(mockTable.snapshots()).thenReturn(List.of(snapshot));
        when(mockTable.currentSnapshot()).thenReturn(snapshot);
        when(snapshot.allManifests(mockFileIO)).thenReturn(List.of());

        var mockScan = mock(org.apache.iceberg.TableScan.class);
        when(mockTable.newScan()).thenReturn(mockScan);

        List<FileScanTask> tasks = createMockFileScanTasks(10, 256 * 1024 * 1024);
        CloseableIterable<FileScanTask> iterable = mockCloseableIterable(tasks);
        when(mockScan.planFiles()).thenReturn(iterable);
    }

    private void setupTableWithMultipleIssues(
            int fileCount,
            long fileSizeBytes,
            int deleteFiles,
            int partitionCount,
            long totalManifestSizeBytes) {
        when(mockTable.snapshots()).thenReturn(List.of(mockSnapshot));
        when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
        when(mockSnapshot.timestampMillis()).thenReturn(Instant.now().toEpochMilli());

        // Setup manifests
        int manifestCount = 10;
        long sizePerManifest = totalManifestSizeBytes / manifestCount;
        List<org.apache.iceberg.ManifestFile> manifests =
                java.util.stream.IntStream.range(0, manifestCount)
                        .mapToObj(
                                i -> {
                                    org.apache.iceberg.ManifestFile manifest =
                                            mock(org.apache.iceberg.ManifestFile.class);
                                    when(manifest.length()).thenReturn(sizePerManifest);
                                    return manifest;
                                })
                        .toList();
        when(mockSnapshot.allManifests(mockFileIO)).thenReturn(manifests);

        var mockScan = mock(org.apache.iceberg.TableScan.class);
        when(mockTable.newScan()).thenReturn(mockScan);

        // Create files with partitions and deletes
        int deletesPerFile = deleteFiles / Math.max(fileCount, 1);
        int remainingDeletes = deleteFiles % Math.max(fileCount, 1);

        List<FileScanTask> tasks =
                java.util.stream.IntStream.range(0, fileCount)
                        .mapToObj(
                                i -> {
                                    FileScanTask task = mock(FileScanTask.class);
                                    DataFile dataFile = mock(DataFile.class);
                                    when(dataFile.fileSizeInBytes()).thenReturn(fileSizeBytes);

                                    // Partition
                                    int partitionIndex = i % partitionCount;
                                    org.apache.iceberg.StructLike partition =
                                            mock(org.apache.iceberg.StructLike.class);
                                    when(partition.toString())
                                            .thenReturn("partition-" + partitionIndex);
                                    when(dataFile.partition()).thenReturn(partition);
                                    when(task.file()).thenReturn(dataFile);

                                    // Delete files
                                    int numDeletes =
                                            deletesPerFile + (i < remainingDeletes ? 1 : 0);
                                    List<DeleteFile> deletes =
                                            java.util.stream.IntStream.range(0, numDeletes)
                                                    .mapToObj(
                                                            j -> {
                                                                DeleteFile deleteFile =
                                                                        mock(DeleteFile.class);
                                                                when(deleteFile.content())
                                                                        .thenReturn(
                                                                                org.apache.iceberg
                                                                                        .FileContent
                                                                                        .POSITION_DELETES);
                                                                return deleteFile;
                                                            })
                                                    .toList();
                                    when(task.deletes()).thenReturn(deletes);
                                    return task;
                                })
                        .toList();

        CloseableIterable<FileScanTask> iterable = mockCloseableIterable(tasks);
        when(mockScan.planFiles()).thenReturn(iterable);
    }
}
