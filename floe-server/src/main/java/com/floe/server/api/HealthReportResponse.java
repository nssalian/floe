package com.floe.server.api;

import com.floe.core.health.HealthIssue;
import com.floe.core.health.HealthReport;
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
        int smallFileCount,
        int largeFileCount,
        int deleteFileCount,
        int positionDeleteFileCount,
        int equalityDeleteFileCount,
        int manifestCount,
        int partitionCount,
        List<HealthIssueResponse> issues) {
    public static HealthReportResponse from(HealthReport report) {
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
                report.smallFileCount(),
                report.largeFileCount(),
                report.deleteFileCount(),
                report.positionDeleteFileCount(),
                report.equalityDeleteFileCount(),
                report.manifestCount(),
                report.partitionCount(),
                report.issues().stream().map(HealthIssueResponse::from).toList());
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
