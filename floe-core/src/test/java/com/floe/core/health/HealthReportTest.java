package com.floe.core.health;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.catalog.TableIdentifier;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class HealthReportTest {

    private static final TableIdentifier TABLE_ID = new TableIdentifier("catalog", "db", "table");

    @Test
    void buildBasicReport() {
        HealthReport report =
                HealthReport.builder(TABLE_ID)
                        .snapshotCount(5)
                        .dataFileCount(100)
                        .totalDataSizeBytes(1024 * 1024 * 1024L) // 1 GB
                        .build();

        assertEquals(TABLE_ID, report.tableIdentifier());
        assertEquals(5, report.snapshotCount());
        assertEquals(100, report.dataFileCount());
        assertEquals(1.0, report.totalDataSizeGb(), 0.01);
    }

    @Test
    void calculateAvgFileSizeMb() {
        HealthReport report =
                HealthReport.builder(TABLE_ID)
                        .avgFileSizeBytes(256 * 1024 * 1024) // 256 MB
                        .build();

        assertEquals(256.0, report.avgFileSizeMb(), 0.01);
    }

    @Test
    void calculateOldestSnapshotAge() {
        Instant now = Instant.now();
        Instant threeDaysAgo = now.minus(3, ChronoUnit.DAYS);

        HealthReport report =
                HealthReport.builder(TABLE_ID)
                        .assessedAt(now)
                        .oldestSnapshotTimestamp(threeDaysAgo)
                        .build();

        Optional<Duration> age = report.oldestSnapshotAge();
        assertTrue(age.isPresent());
        assertEquals(3, age.get().toDays());
    }

    @Test
    void oldestSnapshotAgeEmptyWhenNoSnapshots() {
        HealthReport report = HealthReport.builder(TABLE_ID).build();

        assertTrue(report.oldestSnapshotAge().isEmpty());
    }

    @Test
    void calculateSmallFilePercentage() {
        HealthReport report =
                HealthReport.builder(TABLE_ID).dataFileCount(100).smallFileCount(30).build();

        assertEquals(30.0, report.smallFilePercentage(), 0.01);
    }

    @Test
    void smallFilePercentageZeroWhenEmpty() {
        HealthReport report =
                HealthReport.builder(TABLE_ID).dataFileCount(0).smallFileCount(0).build();

        assertEquals(0.0, report.smallFilePercentage(), 0.01);
    }

    @Test
    void needsCompactionWhenSmallFilesExceedThreshold() {
        HealthReport report =
                HealthReport.builder(TABLE_ID).dataFileCount(100).smallFileCount(30).build();

        assertTrue(report.needsCompaction(25.0));
        assertFalse(report.needsCompaction(35.0));
    }

    @Test
    void isEmptyWhenNoDataFiles() {
        HealthReport empty = HealthReport.builder(TABLE_ID).dataFileCount(0).build();

        HealthReport notEmpty = HealthReport.builder(TABLE_ID).dataFileCount(10).build();

        assertTrue(empty.isEmpty());
        assertFalse(notEmpty.isEmpty());
    }

    @Test
    void hasIssuesWhenIssuesPresent() {
        HealthReport withIssues =
                HealthReport.builder(TABLE_ID)
                        .issues(
                                List.of(
                                        new HealthIssue(
                                                HealthIssue.Type.TOO_MANY_SMALL_FILES,
                                                HealthIssue.Severity.WARNING,
                                                "30% small files")))
                        .build();

        HealthReport noIssues = HealthReport.builder(TABLE_ID).issues(List.of()).build();

        assertTrue(withIssues.hasIssues());
        assertFalse(noIssues.hasIssues());
    }

    @Test
    void hasIssuesOfSeverity() {
        HealthReport report =
                HealthReport.builder(TABLE_ID)
                        .issues(
                                List.of(
                                        new HealthIssue(
                                                HealthIssue.Type.TOO_MANY_SMALL_FILES,
                                                HealthIssue.Severity.WARNING,
                                                "30% small files"),
                                        new HealthIssue(
                                                HealthIssue.Type.TOO_MANY_SNAPSHOTS,
                                                HealthIssue.Severity.CRITICAL,
                                                "500 snapshots")))
                        .build();

        assertTrue(report.hasIssuesOfSeverity(HealthIssue.Severity.WARNING));
        assertTrue(report.hasIssuesOfSeverity(HealthIssue.Severity.CRITICAL));
        assertFalse(report.hasIssuesOfSeverity(HealthIssue.Severity.INFO));
    }

    @Test
    void getIssuesByType() {
        HealthIssue smallFiles =
                new HealthIssue(
                        HealthIssue.Type.TOO_MANY_SMALL_FILES,
                        HealthIssue.Severity.WARNING,
                        "30% small files");
        HealthIssue snapshots =
                new HealthIssue(
                        HealthIssue.Type.TOO_MANY_SNAPSHOTS,
                        HealthIssue.Severity.CRITICAL,
                        "500 snapshots");

        HealthReport report =
                HealthReport.builder(TABLE_ID).issues(List.of(smallFiles, snapshots)).build();

        List<HealthIssue> smallFileIssues =
                report.getIssuesOfType(HealthIssue.Type.TOO_MANY_SMALL_FILES);
        assertEquals(1, smallFileIssues.size());
        assertEquals(smallFiles, smallFileIssues.get(0));
    }

    @Test
    void deleteFileMetrics() {
        HealthReport report =
                HealthReport.builder(TABLE_ID)
                        .deleteFileCount(50)
                        .positionDeleteFileCount(30)
                        .equalityDeleteFileCount(20)
                        .build();

        assertEquals(50, report.deleteFileCount());
        assertEquals(30, report.positionDeleteFileCount());
        assertEquals(20, report.equalityDeleteFileCount());
    }
}
