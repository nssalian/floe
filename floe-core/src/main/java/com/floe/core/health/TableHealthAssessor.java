package com.floe.core.health;

import com.floe.core.catalog.TableIdentifier;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.*;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
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
    private static final int DEFAULT_SAMPLE_LIMIT = 10_000;

    private final HealthThresholds thresholds;
    private final ScanMode scanMode;
    private final int sampleLimit;

    public TableHealthAssessor() {
        this(HealthThresholds.defaults(), ScanMode.SCAN, DEFAULT_SAMPLE_LIMIT);
    }

    public TableHealthAssessor(HealthThresholds thresholds) {
        this(thresholds, ScanMode.SCAN, DEFAULT_SAMPLE_LIMIT);
    }

    public TableHealthAssessor(HealthThresholds thresholds, ScanMode scanMode, int sampleLimit) {
        this.thresholds = thresholds;
        this.scanMode = scanMode != null ? scanMode : ScanMode.SCAN;
        this.sampleLimit = sampleLimit > 0 ? sampleLimit : DEFAULT_SAMPLE_LIMIT;
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
        SnapshotMetrics snapshotMetrics =
                scanMode == ScanMode.METADATA
                        ? collectSnapshotMetricsFromMetadata(table)
                        : collectSnapshotMetrics(table);
        builder.snapshotCount(snapshotMetrics.count)
                .oldestSnapshotTimestamp(snapshotMetrics.oldest)
                .newestSnapshotTimestamp(snapshotMetrics.newest);

        // Collect file metrics
        FileMetrics fileMetrics = collectFileMetrics(table, scanMode, sampleLimit);
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
                .partitionCount(fileMetrics.partitionCount)
                .partitionSkew(fileMetrics.partitionSkew());

        // Collect manifest metrics
        ManifestMetrics manifestMetrics =
                scanMode == ScanMode.METADATA
                        ? collectManifestMetricsFromMetadata(table)
                        : collectManifestMetrics(table);
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

    private static class DataFileMetrics {

        private final int dataFileCount;
        private final long totalDataSizeBytes;
        private final long minFileSizeBytes;
        private final long maxFileSizeBytes;
        private final int smallFileCount;
        private final int largeFileCount;

        private DataFileMetrics(
                int dataFileCount,
                long totalDataSizeBytes,
                long minFileSizeBytes,
                long maxFileSizeBytes,
                int smallFileCount,
                int largeFileCount) {
            this.dataFileCount = dataFileCount;
            this.totalDataSizeBytes = totalDataSizeBytes;
            this.minFileSizeBytes = minFileSizeBytes;
            this.maxFileSizeBytes = maxFileSizeBytes;
            this.smallFileCount = smallFileCount;
            this.largeFileCount = largeFileCount;
        }
    }

    private static class DeleteFileMetrics {

        private final int deleteFileCount;
        private final int positionDeleteFileCount;
        private final int equalityDeleteFileCount;

        private DeleteFileMetrics(
                int deleteFileCount, int positionDeleteFileCount, int equalityDeleteFileCount) {
            this.deleteFileCount = deleteFileCount;
            this.positionDeleteFileCount = positionDeleteFileCount;
            this.equalityDeleteFileCount = equalityDeleteFileCount;
        }
    }

    private SnapshotMetrics collectSnapshotMetricsFromMetadata(Table table) {
        int[] count = new int[] {0};
        Instant[] oldest = new Instant[] {null};
        Instant[] newest = new Instant[] {null};

        try {
            Table snapshotsTable =
                    MetadataTableUtils.createMetadataTableInstance(
                            table, MetadataTableType.SNAPSHOTS);
            Schema schema = snapshotsTable.schema();
            int committedAtPos = fieldPosition(schema, "committed_at");
            if (committedAtPos < 0) {
                return new SnapshotMetrics(0, null, null);
            }

            forEachMetadataRow(
                    snapshotsTable,
                    row -> {
                        Instant committedAt = toInstant(row.get(committedAtPos, Object.class));
                        if (committedAt == null) {
                            return;
                        }
                        count[0]++;
                        if (oldest[0] == null || committedAt.isBefore(oldest[0])) {
                            oldest[0] = committedAt;
                        }
                        if (newest[0] == null || committedAt.isAfter(newest[0])) {
                            newest[0] = committedAt;
                        }
                    });
        } catch (Exception e) {
            LOG.warn("Failed to read snapshots metadata table: {}", e.getMessage());
            return collectSnapshotMetrics(table);
        }

        return new SnapshotMetrics(count[0], oldest[0], newest[0]);
    }

    private FileMetrics collectFileMetrics(Table table, ScanMode mode, int sampleLimit) {
        return switch (mode) {
            case METADATA -> collectFileMetricsFromMetadata(table);
            case SAMPLE -> collectFileMetricsByScan(table, sampleLimit);
            case SCAN -> collectFileMetricsByScan(table, -1);
        };
    }

    private FileMetrics collectFileMetricsFromMetadata(Table table) {
        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            return new FileMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, Map.of());
        }

        Map<String, Integer> partitionFileCounts = collectPartitionFileCounts(table);
        Map<String, Integer> filesPerPartition =
                partitionFileCounts.isEmpty()
                        ? new HashMap<>()
                        : new HashMap<>(partitionFileCounts);

        try {
            return collectFileMetricsFromDataAndDeleteTables(table, filesPerPartition);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to read data/delete metadata tables, falling back to files table: {}",
                    e.getMessage());
        }

        try {
            return collectFileMetricsFromFilesTable(table, filesPerPartition);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to read files metadata table, falling back to snapshot summary: {}",
                    e.getMessage());
            return collectFileMetricsFromSnapshotSummary(currentSnapshot);
        }
    }

    private FileMetrics collectFileMetricsFromDataAndDeleteTables(
            Table table, Map<String, Integer> filesPerPartition) {
        DataFileMetrics dataMetrics = collectDataFileMetricsFromMetadata(table, filesPerPartition);
        DeleteFileMetrics deleteMetrics = collectDeleteFileMetricsFromMetadata(table);

        double avgFileSizeBytes =
                dataMetrics.dataFileCount > 0
                        ? (double) dataMetrics.totalDataSizeBytes / dataMetrics.dataFileCount
                        : 0;

        return new FileMetrics(
                dataMetrics.dataFileCount,
                dataMetrics.totalDataSizeBytes,
                dataMetrics.minFileSizeBytes,
                dataMetrics.maxFileSizeBytes,
                avgFileSizeBytes,
                dataMetrics.smallFileCount,
                dataMetrics.largeFileCount,
                deleteMetrics.deleteFileCount,
                deleteMetrics.positionDeleteFileCount,
                deleteMetrics.equalityDeleteFileCount,
                filesPerPartition.size(),
                filesPerPartition);
    }

    private DataFileMetrics collectDataFileMetricsFromMetadata(
            Table table, Map<String, Integer> filesPerPartition) {
        Table dataFilesTable =
                MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.DATA_FILES);
        Schema schema = dataFilesTable.schema();
        int fileSizePos = fieldPosition(schema, "file_size_in_bytes");
        int partitionPos = fieldPosition(schema, "partition");
        if (fileSizePos < 0) {
            throw new IllegalStateException("Missing file_size_in_bytes in data files table");
        }

        int[] dataFileCount = new int[] {0};
        long[] totalDataSizeBytes = new long[] {0};
        long[] minFileSizeBytes = new long[] {Long.MAX_VALUE};
        long[] maxFileSizeBytes = new long[] {0};
        int[] smallFileCount = new int[] {0};
        int[] largeFileCount = new int[] {0};
        boolean collectPartitionCounts =
                filesPerPartition.isEmpty()
                        || filesPerPartition.values().stream().allMatch(count -> count == 0);
        if (collectPartitionCounts && !filesPerPartition.isEmpty()) {
            filesPerPartition.clear();
        }

        forEachMetadataRow(
                dataFilesTable,
                row -> {
                    long fileSize = getLong(row, fileSizePos, 0L);
                    dataFileCount[0]++;
                    totalDataSizeBytes[0] += fileSize;
                    if (fileSize < minFileSizeBytes[0]) {
                        minFileSizeBytes[0] = fileSize;
                    }
                    if (fileSize > maxFileSizeBytes[0]) {
                        maxFileSizeBytes[0] = fileSize;
                    }
                    if (fileSize < thresholds.smallFileSizeBytes()) {
                        smallFileCount[0]++;
                    }
                    if (fileSize > thresholds.largeFileSizeBytes()) {
                        largeFileCount[0]++;
                    }

                    if (collectPartitionCounts && partitionPos >= 0) {
                        StructLike partition = row.get(partitionPos, StructLike.class);
                        if (partition != null) {
                            String partitionKey = partition.toString();
                            filesPerPartition.merge(partitionKey, 1, Integer::sum);
                        }
                    }
                });

        if (dataFileCount[0] == 0) {
            minFileSizeBytes[0] = 0;
        }

        return new DataFileMetrics(
                dataFileCount[0],
                totalDataSizeBytes[0],
                minFileSizeBytes[0],
                maxFileSizeBytes[0],
                smallFileCount[0],
                largeFileCount[0]);
    }

    private DeleteFileMetrics collectDeleteFileMetricsFromMetadata(Table table) {
        try {
            Table deleteFilesTable =
                    MetadataTableUtils.createMetadataTableInstance(
                            table, MetadataTableType.DELETE_FILES);
            Schema schema = deleteFilesTable.schema();
            int contentPos = fieldPosition(schema, "content");

            int[] deleteFileCount = new int[] {0};
            int[] positionDeleteFileCount = new int[] {0};
            int[] equalityDeleteFileCount = new int[] {0};

            forEachMetadataRow(
                    deleteFilesTable,
                    row -> {
                        deleteFileCount[0]++;
                        if (contentPos >= 0) {
                            int contentId = getInt(row, contentPos, 0);
                            if (contentId == 1) {
                                positionDeleteFileCount[0]++;
                            } else if (contentId == 2) {
                                equalityDeleteFileCount[0]++;
                            }
                        }
                    });

            return new DeleteFileMetrics(
                    deleteFileCount[0], positionDeleteFileCount[0], equalityDeleteFileCount[0]);
        } catch (Exception e) {
            return collectDeleteFileMetricsFromFilesTable(table);
        }
    }

    private DeleteFileMetrics collectDeleteFileMetricsFromFilesTable(Table table) {
        Table filesTable =
                MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES);
        Schema schema = filesTable.schema();
        int contentPos = fieldPosition(schema, "content");
        if (contentPos < 0) {
            return new DeleteFileMetrics(0, 0, 0);
        }

        int[] deleteFileCount = new int[] {0};
        int[] positionDeleteFileCount = new int[] {0};
        int[] equalityDeleteFileCount = new int[] {0};

        forEachMetadataRow(
                filesTable,
                row -> {
                    int contentId = getInt(row, contentPos, 0);
                    if (contentId == 0) {
                        return;
                    }
                    deleteFileCount[0]++;
                    if (contentId == 1) {
                        positionDeleteFileCount[0]++;
                    } else if (contentId == 2) {
                        equalityDeleteFileCount[0]++;
                    }
                });

        return new DeleteFileMetrics(
                deleteFileCount[0], positionDeleteFileCount[0], equalityDeleteFileCount[0]);
    }

    private FileMetrics collectFileMetricsFromFilesTable(
            Table table, Map<String, Integer> filesPerPartition) {
        Table filesTable =
                MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES);
        Schema schema = filesTable.schema();
        int contentPos = fieldPosition(schema, "content");
        int fileSizePos = fieldPosition(schema, "file_size_in_bytes");
        int partitionPos = fieldPosition(schema, "partition");

        int[] dataFileCount = new int[] {0};
        long[] totalDataSizeBytes = new long[] {0};
        long[] minFileSizeBytes = new long[] {Long.MAX_VALUE};
        long[] maxFileSizeBytes = new long[] {0};
        int[] smallFileCount = new int[] {0};
        int[] largeFileCount = new int[] {0};
        int[] deleteFileCount = new int[] {0};
        int[] positionDeleteFileCount = new int[] {0};
        int[] equalityDeleteFileCount = new int[] {0};
        boolean collectPartitionCounts =
                filesPerPartition.isEmpty()
                        || filesPerPartition.values().stream().allMatch(count -> count == 0);
        if (collectPartitionCounts && !filesPerPartition.isEmpty()) {
            filesPerPartition.clear();
        }

        forEachMetadataRow(
                filesTable,
                row -> {
                    int contentId = getInt(row, contentPos, 0);
                    long fileSize = getLong(row, fileSizePos, 0L);
                    if (contentId == 0) {
                        dataFileCount[0]++;
                        totalDataSizeBytes[0] += fileSize;
                        if (fileSize < minFileSizeBytes[0]) {
                            minFileSizeBytes[0] = fileSize;
                        }
                        if (fileSize > maxFileSizeBytes[0]) {
                            maxFileSizeBytes[0] = fileSize;
                        }
                        if (fileSize < thresholds.smallFileSizeBytes()) {
                            smallFileCount[0]++;
                        }
                        if (fileSize > thresholds.largeFileSizeBytes()) {
                            largeFileCount[0]++;
                        }

                        if (collectPartitionCounts && partitionPos >= 0) {
                            StructLike partition = row.get(partitionPos, StructLike.class);
                            if (partition != null) {
                                String partitionKey = partition.toString();
                                filesPerPartition.merge(partitionKey, 1, Integer::sum);
                            }
                        }
                    } else {
                        deleteFileCount[0]++;
                        if (contentId == 1) {
                            positionDeleteFileCount[0]++;
                        } else if (contentId == 2) {
                            equalityDeleteFileCount[0]++;
                        }
                    }
                });

        if (dataFileCount[0] == 0) {
            minFileSizeBytes[0] = 0;
        }

        double avgFileSizeBytes =
                dataFileCount[0] > 0 ? (double) totalDataSizeBytes[0] / dataFileCount[0] : 0;

        return new FileMetrics(
                dataFileCount[0],
                totalDataSizeBytes[0],
                minFileSizeBytes[0],
                maxFileSizeBytes[0],
                avgFileSizeBytes,
                smallFileCount[0],
                largeFileCount[0],
                deleteFileCount[0],
                positionDeleteFileCount[0],
                equalityDeleteFileCount[0],
                filesPerPartition.size(),
                filesPerPartition);
    }

    private FileMetrics collectFileMetricsFromSnapshotSummary(Snapshot snapshot) {
        Map<String, String> summary = snapshot.summary();
        int dataFileCount = parseIntSummary(summary, "total-data-files");
        long totalDataSizeBytes =
                parseLongSummary(summary, "total-file-size", "total-data-files-size");
        int deleteFileCount = parseIntSummary(summary, "total-delete-files");
        int positionDeleteFileCount = parseIntSummary(summary, "total-position-deletes");
        int equalityDeleteFileCount = parseIntSummary(summary, "total-equality-deletes");

        double avgFileSizeBytes =
                dataFileCount > 0 ? (double) totalDataSizeBytes / dataFileCount : 0;

        return new FileMetrics(
                dataFileCount,
                totalDataSizeBytes,
                0,
                0,
                avgFileSizeBytes,
                0,
                0,
                deleteFileCount,
                positionDeleteFileCount,
                equalityDeleteFileCount,
                0,
                Map.of());
    }

    private FileMetrics collectFileMetricsByScan(Table table, int sampleLimit) {
        int dataFileCount = 0;
        long totalDataSizeBytes = 0;
        long minFileSizeBytes = Long.MAX_VALUE;
        long maxFileSizeBytes = 0;
        int smallFileCount = 0;
        int largeFileCount = 0;
        int deleteFileCount = 0;
        int positionDeleteFileCount = 0;
        int equalityDeleteFileCount = 0;
        Map<String, Integer> filesPerPartition = new HashMap<>();

        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            return new FileMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, Map.of());
        }

        int sampledFiles = 0;
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

                // Track files per partition for skew detection
                if (dataFile.partition() != null) {
                    String partitionKey = dataFile.partition().toString();
                    filesPerPartition.merge(partitionKey, 1, Integer::sum);
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

                sampledFiles++;
                if (sampleLimit > 0 && sampledFiles >= sampleLimit) {
                    break;
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
                filesPerPartition.size(),
                filesPerPartition);
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

    private ManifestMetrics collectManifestMetricsFromMetadata(Table table) {
        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            return new ManifestMetrics(0, 0);
        }

        long[] totalSizeBytes = new long[] {0};
        int[] count = new int[] {0};

        try {
            Table manifestsTable =
                    MetadataTableUtils.createMetadataTableInstance(
                            table, MetadataTableType.MANIFESTS);
            Schema schema = manifestsTable.schema();
            int lengthPos = fieldPosition(schema, "length");
            if (lengthPos < 0) {
                return new ManifestMetrics(0, 0);
            }

            forEachMetadataRow(
                    manifestsTable,
                    row -> {
                        long length = getLong(row, lengthPos, 0L);
                        totalSizeBytes[0] += length;
                        count[0]++;
                    });
        } catch (Exception e) {
            LOG.warn("Failed to read manifests metadata table: {}", e.getMessage());
            return collectManifestMetrics(table);
        }

        return new ManifestMetrics(count[0], totalSizeBytes[0]);
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
            if (largeFilePercent >= thresholds.largeFilePercentCritical()) {
                issues.add(
                        HealthIssue.critical(
                                HealthIssue.Type.TOO_MANY_LARGE_FILES,
                                String.format(
                                        "%.1f%% of files are over %dMB (%d files)",
                                        largeFilePercent,
                                        thresholds.largeFileSizeBytes() / (1024 * 1024),
                                        files.largeFileCount)));
            } else if (largeFilePercent >= thresholds.largeFilePercentWarning()) {
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

        // High delete file ratio
        double deleteRatio = files.deleteFileRatio();
        if (deleteRatio >= thresholds.deleteFileRatioCritical()) {
            issues.add(
                    HealthIssue.critical(
                            HealthIssue.Type.HIGH_DELETE_FILE_RATIO,
                            String.format(
                                    "Delete file ratio is %.1f%% (%d delete files / %d data files)",
                                    deleteRatio * 100,
                                    files.deleteFileCount,
                                    files.dataFileCount)));
        } else if (deleteRatio >= thresholds.deleteFileRatioWarning()) {
            issues.add(
                    HealthIssue.warning(
                            HealthIssue.Type.HIGH_DELETE_FILE_RATIO,
                            String.format(
                                    "Delete file ratio is %.1f%% (%d delete files / %d data files)",
                                    deleteRatio * 100,
                                    files.deleteFileCount,
                                    files.dataFileCount)));
        }

        // Too many partitions
        if (files.partitionCount >= thresholds.partitionCountCritical()) {
            issues.add(
                    HealthIssue.critical(
                            HealthIssue.Type.TOO_MANY_PARTITIONS,
                            String.format("Table has %d partitions", files.partitionCount)));
        } else if (files.partitionCount >= thresholds.partitionCountWarning()) {
            issues.add(
                    HealthIssue.warning(
                            HealthIssue.Type.TOO_MANY_PARTITIONS,
                            String.format("Table has %d partitions", files.partitionCount)));
        }

        // Partition skew
        double skew = files.partitionSkew();
        if (skew >= thresholds.partitionSkewCritical()) {
            issues.add(
                    HealthIssue.critical(
                            HealthIssue.Type.PARTITION_SKEW,
                            String.format(
                                    "Partition skew is %.1fx (max/avg files per partition)",
                                    skew)));
        } else if (skew >= thresholds.partitionSkewWarning()) {
            issues.add(
                    HealthIssue.warning(
                            HealthIssue.Type.PARTITION_SKEW,
                            String.format(
                                    "Partition skew is %.1fx (max/avg files per partition)",
                                    skew)));
        }

        // Large manifest list (total manifest size)
        if (manifests.totalSizeBytes >= thresholds.manifestSizeCriticalBytes()) {
            issues.add(
                    HealthIssue.critical(
                            HealthIssue.Type.LARGE_MANIFEST_LIST,
                            String.format(
                                    "Total manifest size is %dMB",
                                    manifests.totalSizeBytes / (1024 * 1024))));
        } else if (manifests.totalSizeBytes >= thresholds.manifestSizeWarningBytes()) {
            issues.add(
                    HealthIssue.warning(
                            HealthIssue.Type.LARGE_MANIFEST_LIST,
                            String.format(
                                    "Total manifest size is %dMB",
                                    manifests.totalSizeBytes / (1024 * 1024))));
        }

        // Stale metadata (time since last write/newest snapshot)
        if (snapshots.newest != null) {
            long daysSinceWrite = ChronoUnit.DAYS.between(snapshots.newest, now);
            if (daysSinceWrite >= thresholds.staleMetadataCriticalDays()) {
                issues.add(
                        HealthIssue.critical(
                                HealthIssue.Type.STALE_METADATA,
                                String.format(
                                        "Table has not been written to in %d days",
                                        daysSinceWrite)));
            } else if (daysSinceWrite >= thresholds.staleMetadataWarningDays()) {
                issues.add(
                        HealthIssue.warning(
                                HealthIssue.Type.STALE_METADATA,
                                String.format(
                                        "Table has not been written to in %d days",
                                        daysSinceWrite)));
            }
        }

        return issues;
    }

    private Map<String, Integer> collectPartitionFileCounts(Table table) {
        try {
            Table partitionsTable =
                    MetadataTableUtils.createMetadataTableInstance(
                            table, MetadataTableType.PARTITIONS);
            Schema schema = partitionsTable.schema();
            int fileCountPos = fieldPosition(schema, "file_count");
            int partitionPos = fieldPosition(schema, "partition");
            Map<String, Integer> counts = new HashMap<>();

            forEachMetadataRow(
                    partitionsTable,
                    row -> {
                        int fileCount = getInt(row, fileCountPos, 0);
                        String key = "root";
                        if (partitionPos >= 0) {
                            StructLike partition = row.get(partitionPos, StructLike.class);
                            if (partition != null) {
                                key = partition.toString();
                            }
                        }
                        counts.put(key, fileCount);
                    });

            return counts;
        } catch (Exception e) {
            LOG.warn("Failed to read partitions metadata table: {}", e.getMessage());
            return Map.of();
        }
    }

    private void forEachMetadataRow(Table metadataTable, Consumer<StructLike> consumer) {
        try (CloseableIterable<FileScanTask> tasks = metadataTable.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (!task.isDataTask()) {
                    continue;
                }
                DataTask dataTask = task.asDataTask();
                try (CloseableIterable<StructLike> rows = dataTask.rows()) {
                    for (StructLike row : rows) {
                        consumer.accept(row);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to close metadata rows iterable", e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close metadata task iterable", e);
        }
    }

    private int fieldPosition(Schema schema, String fieldName) {
        if (schema == null || fieldName == null) {
            return -1;
        }
        Types.NestedField field = schema.findField(fieldName);
        if (field == null) {
            return -1;
        }
        return schema.columns().indexOf(field);
    }

    private int getInt(StructLike row, int pos, int defaultValue) {
        if (row == null || pos < 0) {
            return defaultValue;
        }
        Integer value = row.get(pos, Integer.class);
        return value != null ? value : defaultValue;
    }

    private long getLong(StructLike row, int pos, long defaultValue) {
        if (row == null || pos < 0) {
            return defaultValue;
        }
        Long value = row.get(pos, Long.class);
        return value != null ? value : defaultValue;
    }

    private Instant toInstant(Object value) {
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof Long epochValue) {
            long millis = toEpochMillis(epochValue);
            return Instant.ofEpochMilli(millis);
        }
        return null;
    }

    private long toEpochMillis(long epochValue) {
        if (epochValue > 100_000_000_000_000_000L) {
            // nanoseconds
            return epochValue / 1_000_000;
        }
        if (epochValue > 10_000_000_000_000L) {
            // microseconds
            return epochValue / 1000;
        }
        return epochValue;
    }

    private int parseIntSummary(Map<String, String> summary, String key) {
        if (summary == null || summary.get(key) == null) {
            return 0;
        }
        try {
            return Integer.parseInt(summary.get(key));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private long parseLongSummary(Map<String, String> summary, String... keys) {
        if (summary == null || keys == null) {
            return 0;
        }
        for (String key : keys) {
            if (key == null) {
                continue;
            }
            String value = summary.get(key);
            if (value == null) {
                continue;
            }
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
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
            int partitionCount,
            Map<String, Integer> filesPerPartition) {
        /** Computes the delete file ratio (deleteFileCount / dataFileCount). */
        double deleteFileRatio() {
            if (dataFileCount == 0) return 0.0;
            return (double) deleteFileCount / dataFileCount;
        }

        /**
         * Computes the partition skew as max/avg files per partition. Returns 0 if no partitions or
         * no meaningful data.
         */
        double partitionSkew() {
            if (filesPerPartition == null || filesPerPartition.isEmpty()) {
                return 0.0;
            }
            int maxFiles =
                    filesPerPartition.values().stream().mapToInt(Integer::intValue).max().orElse(0);
            double avgFiles =
                    filesPerPartition.values().stream()
                            .mapToInt(Integer::intValue)
                            .average()
                            .orElse(0);
            if (avgFiles == 0) return 0.0;
            return maxFiles / avgFiles;
        }
    }

    /** Manifest statistics for a table. */
    private record ManifestMetrics(int count, long totalSizeBytes) {}

    public enum ScanMode {
        METADATA,
        SCAN,
        SAMPLE;

        public static ScanMode fromString(String value) {
            if (value == null || value.isBlank()) {
                return SCAN;
            }
            return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
                case "metadata" -> METADATA;
                case "sample" -> SAMPLE;
                case "scan" -> SCAN;
                default -> SCAN;
            };
        }
    }
}
