package com.floe.server.api;

import com.floe.core.health.HealthIssue;
import com.floe.core.health.HealthReport;
import com.floe.core.health.HealthThresholds;
import java.util.List;

/** Response for health report. */
public record HealthReportResponse(
        String qualifiedName,
        String assessedAt,
        String healthStatus,
        int snapshotCount,
        int dataFileCount,
        long totalDataSizeBytes,
        String totalDataSizeFormatted,
        long minFileSizeBytes,
        long maxFileSizeBytes,
        double avgFileSizeBytes,
        String avgFileSizeFormatted,
        int smallFileCount,
        int largeFileCount,
        int deleteFileCount,
        int positionDeleteFileCount,
        int equalityDeleteFileCount,
        double deleteFileRatio,
        int manifestCount,
        long totalManifestSizeBytes,
        String totalManifestSizeFormatted,
        int partitionCount,
        Long oldestSnapshotAgeDays,
        Long newestSnapshotAgeDays,
        List<HealthIssueResponse> issues,
        HealthThresholdsResponse thresholds) {
    /**
     * Create a response from a HealthReport using default thresholds.
     *
     * @param report the health report
     * @return the response DTO
     */
    public static HealthReportResponse from(HealthReport report) {
        return from(report, HealthThresholds.defaults());
    }

    /**
     * Create a response from a HealthReport with specific thresholds.
     *
     * @param report the health report
     * @param thresholds the thresholds used for assessment
     * @return the response DTO
     */
    public static HealthReportResponse from(HealthReport report, HealthThresholds thresholds) {
        String status = "healthy";
        if (report.issues().stream().anyMatch(i -> i.severity() == HealthIssue.Severity.CRITICAL)) {
            status = "critical";
        } else if (report.issues().stream()
                .anyMatch(i -> i.severity() == HealthIssue.Severity.WARNING)) {
            status = "warning";
        }

        return new HealthReportResponse(
                report.tableIdentifier().toQualifiedName(),
                report.assessedAt().toString(),
                status,
                report.snapshotCount(),
                report.dataFileCount(),
                report.totalDataSizeBytes(),
                formatBytes(report.totalDataSizeBytes()),
                report.minFileSizeBytes(),
                report.maxFileSizeBytes(),
                report.avgFileSizeBytes(),
                formatBytes((long) report.avgFileSizeBytes()),
                report.smallFileCount(),
                report.largeFileCount(),
                report.deleteFileCount(),
                report.positionDeleteFileCount(),
                report.equalityDeleteFileCount(),
                report.deleteFileRatio(),
                report.manifestCount(),
                report.totalManifestSizeBytes(),
                formatBytes(report.totalManifestSizeBytes()),
                report.partitionCount(),
                report.oldestSnapshotAgeDays(),
                report.newestSnapshotAgeDays(),
                report.issues().stream().map(HealthIssueResponse::from).toList(),
                HealthThresholdsResponse.from(thresholds));
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
