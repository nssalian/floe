package com.floe.server.api;

import com.floe.core.catalog.TableMetadata;

/** Response for table details. */
public record TableDetailResponse(
        String catalog,
        String namespace,
        String name,
        String qualifiedName,
        String location,
        int snapshotCount,
        long currentSnapshotId,
        String currentSnapshotTimestamp,
        String oldestSnapshotTimestamp,
        int dataFileCount,
        long totalDataFileSizeBytes,
        String totalDataSizeFormatted,
        double averageFileSizeMb,
        int deleteFileCount,
        int positionDeleteFileCount,
        int equalityDeleteFileCount,
        long totalRecordCount,
        int manifestCount,
        long totalManifestSizeBytes,
        int formatVersion,
        String partitionSpec,
        String sortOrder,
        String lastModified) {
    public static TableDetailResponse from(TableMetadata meta) {
        return new TableDetailResponse(
                meta.identifier().catalog(),
                meta.identifier().namespace(),
                meta.identifier().table(),
                meta.identifier().toQualifiedName(),
                meta.location(),
                meta.snapshotCount(),
                meta.currentSnapshotId(),
                meta.currentSnapshotTimestamp() != null
                        ? meta.currentSnapshotTimestamp().toString()
                        : null,
                meta.oldestSnapshotTimestamp() != null
                        ? meta.oldestSnapshotTimestamp().toString()
                        : null,
                meta.dataFileCount(),
                meta.totalDataFileSizeBytes(),
                formatBytes(meta.totalDataFileSizeBytes()),
                meta.averageDataFileSizeMb(),
                meta.deleteFileCount(),
                meta.positionDeleteFileCount(),
                meta.equalityDeleteFileCount(),
                meta.totalRecordCount(),
                meta.manifestCount(),
                meta.totalManifestSizeBytes(),
                meta.formatVersion(),
                meta.partitionSpec(),
                meta.sortOrder(),
                meta.lastModified() != null ? meta.lastModified().toString() : null);
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
