package com.floe.core.health;

import com.floe.core.catalog.TableIdentifier;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assesses the health of an Iceberg table and produces a HealthReport.
 *
 * <p>Analyzes snapshot history, file sizes, manifest counts, and other metrics to identify
 * potential issues that may require maintenance operations.
 */
public class TableHealthAssessor {

    private static final Logger LOG = LoggerFactory.getLogger(TableHealthAssessor.class);

    private final HealthThresholds thresholds;

    public TableHealthAssessor() {
        this(HealthThresholds.defaults());
    }

    public TableHealthAssessor(HealthThresholds thresholds) {
        this.thresholds = thresholds;
    }

    /**
     * Assess the health of a table and produce a report.
     *
     * @param tableId the table identifier
     * @param table the Iceberg table to assess
     * @return a health report with metrics and identified issues
     */
    public HealthReport assess(TableIdentifier tableId, Table table) {
        LOG.debug("Assessing health of table {}", tableId);
        Instant now = Instant.now();

        HealthReport.Builder builder = HealthReport.builder(tableId).assessedAt(now);

        // Collect snapshot metrics
        SnapshotMetrics snapshotMetrics = collectSnapshotMetrics(table);
        builder.snapshotCount(snapshotMetrics.count)
                .oldestSnapshotTimestamp(snapshotMetrics.oldest)
                .newestSnapshotTimestamp(snapshotMetrics.newest);

        // Collect file metrics
        FileMetrics fileMetrics = collectFileMetrics(table);
        builder.dataFileCount(fileMetrics.dataFileCount)
                .totalDataSizeBytes(fileMetrics.totalDataSizeBytes)
                .minFileSizeBytes(fileMetrics.minFileSizeBytes)
                .maxFileSizeBytes(fileMetrics.maxFileSizeBytes)
                .avgFileSizeBytes(fileMetrics.avgFileSizeBytes)
                .smallFileCount(fileMetrics.smallFileCount)
                .largeFileCount(fileMetrics.largeFileCount)
                .deleteFileCount(fileMetrics.deleteFileCount)
                .positionDeleteFileCount(fileMetrics.positionDeleteFileCount)
                .equalityDeleteFileCount(fileMetrics.equalityDeleteFileCount)
                .partitionCount(fileMetrics.partitionCount);

        // Collect manifest metrics
        ManifestMetrics manifestMetrics = collectManifestMetrics(table);
        builder.manifestCount(manifestMetrics.count)
                .totalManifestSizeBytes(manifestMetrics.totalSizeBytes);

        // Identify issues
        List<HealthIssue> issues =
                identifyIssues(snapshotMetrics, fileMetrics, manifestMetrics, now);
        builder.issues(issues);

        HealthReport report = builder.build();
        LOG.debug("Health assessment complete for {}: {} issues found", tableId, issues.size());
        return report;
    }

    private SnapshotMetrics collectSnapshotMetrics(Table table) {
        int count = 0;
        Instant oldest = null;
        Instant newest = null;

        for (Snapshot snapshot : table.snapshots()) {
            count++;
            Instant ts = Instant.ofEpochMilli(snapshot.timestampMillis());
            if (oldest == null || ts.isBefore(oldest)) {
                oldest = ts;
            }
            if (newest == null || ts.isAfter(newest)) {
                newest = ts;
            }
        }

        return new SnapshotMetrics(count, oldest, newest);
    }

    private FileMetrics collectFileMetrics(Table table) {
        int dataFileCount = 0;
        long totalDataSizeBytes = 0;
        long minFileSizeBytes = Long.MAX_VALUE;
        long maxFileSizeBytes = 0;
        int smallFileCount = 0;
        int largeFileCount = 0;
        int deleteFileCount = 0;
        int positionDeleteFileCount = 0;
        int equalityDeleteFileCount = 0;
        Set<String> partitions = new HashSet<>();

        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            return new FileMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        try (var taskIterator = table.newScan().planFiles().iterator()) {
            while (taskIterator.hasNext()) {
                FileScanTask task = taskIterator.next();
                DataFile dataFile = task.file();

                dataFileCount++;
                long fileSize = dataFile.fileSizeInBytes();
                totalDataSizeBytes += fileSize;

                if (fileSize < minFileSizeBytes) {
                    minFileSizeBytes = fileSize;
                }
                if (fileSize > maxFileSizeBytes) {
                    maxFileSizeBytes = fileSize;
                }
                if (fileSize < thresholds.smallFileSizeBytes()) {
                    smallFileCount++;
                }
                if (fileSize > thresholds.largeFileSizeBytes()) {
                    largeFileCount++;
                }

                // Track partition
                if (dataFile.partition() != null) {
                    partitions.add(dataFile.partition().toString());
                }

                // Count delete files
                for (DeleteFile deleteFile : task.deletes()) {
                    deleteFileCount++;
                    switch (deleteFile.content()) {
                        case POSITION_DELETES -> positionDeleteFileCount++;
                        case EQUALITY_DELETES -> equalityDeleteFileCount++;
                        default -> {}
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Error scanning files for table: {}", e.getMessage());
        }

        double avgFileSizeBytes =
                dataFileCount > 0 ? (double) totalDataSizeBytes / dataFileCount : 0;

        if (dataFileCount == 0) {
            minFileSizeBytes = 0;
        }

        return new FileMetrics(
                dataFileCount,
                totalDataSizeBytes,
                minFileSizeBytes,
                maxFileSizeBytes,
                avgFileSizeBytes,
                smallFileCount,
                largeFileCount,
                deleteFileCount,
                positionDeleteFileCount,
                equalityDeleteFileCount,
                partitions.size());
    }

    private ManifestMetrics collectManifestMetrics(Table table) {
        int count = 0;
        long totalSizeBytes = 0;

        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            return new ManifestMetrics(0, 0);
        }

        try {
            for (ManifestFile manifest : currentSnapshot.allManifests(table.io())) {
                count++;
                totalSizeBytes += manifest.length();
            }
        } catch (Exception e) {
            LOG.warn("Error reading manifests: {}", e.getMessage());
        }

        return new ManifestMetrics(count, totalSizeBytes);
    }

    private List<HealthIssue> identifyIssues(
            SnapshotMetrics snapshots, FileMetrics files, ManifestMetrics manifests, Instant now) {
        List<HealthIssue> issues = new ArrayList<>();

        // Empty table
        if (files.dataFileCount == 0) {
            issues.add(HealthIssue.info(HealthIssue.Type.TABLE_EMPTY, "Table has no data files"));
            return issues;
        }

        // Small files
        if (files.dataFileCount > 0) {
            double smallFilePercent = (files.smallFileCount * 100.0) / files.dataFileCount;
            String formatted =
                    String.format(
                            "%.1f%% of files are under %dMB (%d files)",
                            smallFilePercent,
                            thresholds.smallFileSizeBytes() / (1024 * 1024),
                            files.smallFileCount);
            if (smallFilePercent >= thresholds.smallFilePercentCritical()) {
                issues.add(HealthIssue.critical(HealthIssue.Type.TOO_MANY_SMALL_FILES, formatted));
            } else if (smallFilePercent >= thresholds.smallFilePercentWarning()) {
                issues.add(HealthIssue.warning(HealthIssue.Type.TOO_MANY_SMALL_FILES, formatted));
            }
        }

        // Large files
        if (files.largeFileCount > 0) {
            double largeFilePercent = (files.largeFileCount * 100.0) / files.dataFileCount;
            if (largeFilePercent >= thresholds.largeFilePercentWarning()) {
                issues.add(
                        HealthIssue.warning(
                                HealthIssue.Type.TOO_MANY_LARGE_FILES,
                                String.format(
                                        "%.1f%% of files are over %dMB (%d files)",
                                        largeFilePercent,
                                        thresholds.largeFileSizeBytes() / (1024 * 1024),
                                        files.largeFileCount)));
            }
        }

        // High file count
        if (files.dataFileCount >= thresholds.fileCountCritical()) {
            issues.add(
                    HealthIssue.critical(
                            HealthIssue.Type.HIGH_FILE_COUNT,
                            String.format("Table has %d data files", files.dataFileCount)));
        } else if (files.dataFileCount >= thresholds.fileCountWarning()) {
            issues.add(
                    HealthIssue.warning(
                            HealthIssue.Type.HIGH_FILE_COUNT,
                            String.format("Table has %d data files", files.dataFileCount)));
        }

        // Too many snapshots
        if (snapshots.count >= thresholds.snapshotCountCritical()) {
            issues.add(
                    HealthIssue.critical(
                            HealthIssue.Type.TOO_MANY_SNAPSHOTS,
                            String.format("Table has %d snapshots", snapshots.count)));
        } else if (snapshots.count >= thresholds.snapshotCountWarning()) {
            issues.add(
                    HealthIssue.warning(
                            HealthIssue.Type.TOO_MANY_SNAPSHOTS,
                            String.format("Table has %d snapshots", snapshots.count)));
        }

        // Old snapshots
        if (snapshots.oldest != null) {
            long daysOld = ChronoUnit.DAYS.between(snapshots.oldest, now);
            if (daysOld >= thresholds.snapshotAgeCriticalDays()) {
                issues.add(
                        HealthIssue.critical(
                                HealthIssue.Type.OLD_SNAPSHOTS,
                                String.format("Oldest snapshot is %d days old", daysOld)));
            } else if (daysOld >= thresholds.snapshotAgeWarningDays()) {
                issues.add(
                        HealthIssue.warning(
                                HealthIssue.Type.OLD_SNAPSHOTS,
                                String.format("Oldest snapshot is %d days old", daysOld)));
            }
        }

        // Delete files
        if (files.deleteFileCount >= thresholds.deleteFileCountCritical()) {
            issues.add(
                    HealthIssue.critical(
                            HealthIssue.Type.TOO_MANY_DELETE_FILES,
                            String.format("Table has %d delete files", files.deleteFileCount)));
        } else if (files.deleteFileCount >= thresholds.deleteFileCountWarning()) {
            issues.add(
                    HealthIssue.warning(
                            HealthIssue.Type.TOO_MANY_DELETE_FILES,
                            String.format("Table has %d delete files", files.deleteFileCount)));
        }

        // Too many manifests
        if (manifests.count >= thresholds.manifestCountCritical()) {
            issues.add(
                    HealthIssue.critical(
                            HealthIssue.Type.TOO_MANY_MANIFESTS,
                            String.format("Table has %d manifests", manifests.count)));
        } else if (manifests.count >= thresholds.manifestCountWarning()) {
            issues.add(
                    HealthIssue.warning(
                            HealthIssue.Type.TOO_MANY_MANIFESTS,
                            String.format("Table has %d manifests", manifests.count)));
        }

        return issues;
    }

    /** Snapshot statistics for a table. */
    private record SnapshotMetrics(int count, Instant oldest, Instant newest) {}

    /** File statistics for a table including data files and delete files. */
    private record FileMetrics(
            int dataFileCount,
            long totalDataSizeBytes,
            long minFileSizeBytes,
            long maxFileSizeBytes,
            double avgFileSizeBytes,
            int smallFileCount,
            int largeFileCount,
            int deleteFileCount,
            int positionDeleteFileCount,
            int equalityDeleteFileCount,
            int partitionCount) {}

    /** Manifest statistics for a table. */
    private record ManifestMetrics(int count, long totalSizeBytes) {}
}
