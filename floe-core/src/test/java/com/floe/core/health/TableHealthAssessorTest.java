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
}
