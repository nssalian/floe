package com.floe.server.ui;

import com.floe.core.operation.NormalizedMetrics;
import com.floe.core.operation.OperationRecord;
import com.floe.core.operation.OperationStats;
import com.floe.core.operation.OperationStatus;
import com.floe.core.operation.OperationStore;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** UI resource for operations pages. */
@Path("/ui/operations")
@Produces(MediaType.TEXT_HTML)
public class OperationsUiResource extends UiResource {

    private static final Logger LOG = LoggerFactory.getLogger(OperationsUiResource.class);

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("MMM d, h:mm a").withZone(ZoneId.systemDefault());

    @Inject Template operations;

    @Inject
    @io.quarkus.qute.Location("operation-detail.html")
    Template operationDetail;

    @Inject
    @io.quarkus.qute.Location("operations-table-rows.html")
    Template operationsTableRows;

    @Inject
    @io.quarkus.qute.Location("operations-stats-row.html")
    Template operationsStatsRow;

    @Inject OperationStore operationStore;

    /** Main operations list page. */
    @GET
    public TemplateInstance index(@QueryParam("status") String status) {
        return page(operations, "/ui/operations").data("currentStatus", status);
    }

    /** Fragment: table rows for operations list (htmx partial). */
    @GET
    @Path("/list")
    public TemplateInstance listFragment(
            @QueryParam("status") String status,
            @QueryParam("limit") @DefaultValue("50") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        LOG.debug(
                "Fetching operations list: status={}, limit={}, offset={}", status, limit, offset);

        List<OperationRecord> records;
        long total;
        if (status != null && !status.isBlank()) {
            try {
                OperationStatus opStatus = OperationStatus.valueOf(status.toUpperCase(Locale.ROOT));
                records = operationStore.findByStatus(opStatus, limit, offset);
                total = operationStore.countByStatus(opStatus);
            } catch (IllegalArgumentException e) {
                records = operationStore.findRecent(limit, offset);
                total = operationStore.count();
            }
        } else {
            records = operationStore.findRecent(limit, offset);
            total = operationStore.count();
        }

        List<OperationView> views = records.stream().map(OperationView::from).toList();
        boolean hasMore = offset + views.size() < total;
        boolean hasPrev = offset > 0;
        int prevOffset = Math.max(0, offset - limit);

        return operationsTableRows
                .data("operations", views)
                .data("statusFilter", status)
                .data("offset", offset)
                .data("limit", limit)
                .data("totalCount", total)
                .data("hasMore", hasMore)
                .data("hasPrev", hasPrev)
                .data("nextOffset", offset + limit)
                .data("prevOffset", prevOffset);
    }

    /** Fragment: stats row (htmx partial). */
    @GET
    @Path("/stats-row")
    public TemplateInstance statsRowFragment() {
        LOG.debug("Fetching operations stats");

        OperationStats stats = operationStore.getStats(Duration.ofHours(24));
        return operationsStatsRow.data("stats", StatsView.from(stats));
    }

    /** Operation detail page. */
    @GET
    @Path("/{id}")
    public Response detail(@PathParam("id") String id) {
        LOG.debug("Viewing operation: {}", id);

        try {
            UUID uuid = UUID.fromString(id);
            Optional<OperationRecord> recordOpt = operationStore.findById(uuid);

            if (recordOpt.isEmpty()) {
                // Redirect to list with error
                return Response.seeOther(URI.create("/ui/operations")).build();
            }

            OperationDetailView view = OperationDetailView.from(recordOpt.get());
            TemplateInstance template = page(operationDetail, "/ui/operations").data("op", view);

            return Response.ok(template).build();
        } catch (IllegalArgumentException e) {
            return Response.seeOther(URI.create("/ui/operations")).build();
        }
    }

    // View models for templates

    /** View model for operations table rows. */
    public record OperationView(
            String id,
            String status,
            String tableName,
            String catalog,
            String namespace,
            String policyName,
            String engineType,
            String startedAtFormatted,
            String startedAtRelative,
            String durationFormatted) {
        public static OperationView from(OperationRecord record) {
            return new OperationView(
                    record.id().toString(),
                    record.status().name(),
                    record.tableName(),
                    record.catalog(),
                    record.namespace(),
                    record.policyName(),
                    formatEngineType(record.engineType()),
                    formatInstant(record.startedAt()),
                    formatRelative(record.startedAt()),
                    formatDuration(record.duration()));
        }
    }

    /** View model for operation detail page. */
    public record OperationDetailView(
            String id,
            String status,
            String tableName,
            String catalog,
            String namespace,
            String policyName,
            String policyId,
            String engineType,
            String executionId,
            String scheduleId,
            String policyVersion,
            String startedAtFormatted,
            String completedAtFormatted,
            String durationFormatted,
            String errorMessage,
            String completedAt,
            ResultsView results,
            AggregatedMetricsView aggregatedMetrics) {
        public static OperationDetailView from(OperationRecord record) {
            return new OperationDetailView(
                    record.id().toString(),
                    record.status().name(),
                    record.tableName(),
                    record.catalog(),
                    record.namespace(),
                    record.policyName(),
                    record.policyId() != null ? record.policyId().toString() : null,
                    formatEngineType(record.engineType()),
                    record.executionId(),
                    record.scheduleId(),
                    record.policyVersion(),
                    formatInstant(record.startedAt()),
                    record.completedAt() != null ? formatInstant(record.completedAt()) : null,
                    formatDuration(record.duration()),
                    record.errorMessage(),
                    record.completedAt() != null ? record.completedAt().toString() : null,
                    record.results() != null ? ResultsView.from(record.results()) : null,
                    AggregatedMetricsView.from(record.results()));
        }
    }

    /** View model for operation results. */
    public record ResultsView(
            List<SingleResultView> operations,
            int successCount,
            int failedCount,
            int skippedCount) {
        public static ResultsView from(com.floe.core.operation.OperationResults results) {
            List<SingleResultView> ops =
                    results.operations().stream().map(SingleResultView::from).toList();
            return new ResultsView(
                    ops,
                    (int) results.successCount(),
                    (int) results.failedCount(),
                    (int) results.skippedCount());
        }
    }

    /** View model for a single operation result. */
    public record SingleResultView(
            String operationType,
            String status,
            String durationFormatted,
            String metricsSummary,
            String errorMessage) {
        public static SingleResultView from(
                com.floe.core.operation.OperationResults.SingleOperationResult result) {
            return new SingleResultView(
                    formatOperationType(result.operationType().name()),
                    result.status(),
                    formatDurationMs(result.durationMs()),
                    buildMetricsSummary(result.metrics()),
                    result.errorMessage());
        }
    }

    public record AggregatedMetricsView(List<MetricView> metrics) {
        public static AggregatedMetricsView from(com.floe.core.operation.OperationResults results) {
            if (results == null || results.aggregatedMetrics() == null) {
                return null;
            }
            Map<String, Object> metrics = results.aggregatedMetrics();
            List<MetricView> views = new ArrayList<>();
            addMetricView(
                    views, "Files rewritten", metrics, NormalizedMetrics.FILES_REWRITTEN, false);
            addMetricView(
                    views, "Bytes rewritten", metrics, NormalizedMetrics.BYTES_REWRITTEN, true);
            addMetricView(
                    views,
                    "Manifests rewritten",
                    metrics,
                    NormalizedMetrics.MANIFESTS_REWRITTEN,
                    false);
            addMetricView(
                    views,
                    "Snapshots expired",
                    metrics,
                    NormalizedMetrics.SNAPSHOTS_EXPIRED,
                    false);
            addMetricView(
                    views,
                    "Delete files removed",
                    metrics,
                    NormalizedMetrics.DELETE_FILES_REMOVED,
                    false);
            addMetricView(
                    views,
                    "Orphan files removed",
                    metrics,
                    NormalizedMetrics.ORPHAN_FILES_REMOVED,
                    false);
            if (views.isEmpty()) {
                return null;
            }
            return new AggregatedMetricsView(views);
        }
    }

    public record MetricView(String label, String value) {}

    /** View model for stats. */
    public record StatsView(
            long totalOperations,
            long successCount,
            long runningCount,
            long withFailuresCount,
            String successRateFormatted) {
        public static StatsView from(OperationStats stats) {
            String rate =
                    stats.completedCount() > 0 ? String.format("%.0f", stats.successRate()) : "--";
            return new StatsView(
                    stats.totalOperations(),
                    stats.successCount(),
                    stats.runningCount(),
                    stats.withFailuresCount(),
                    rate);
        }
    }

    // Formatting helpers

    private static String formatInstant(Instant instant) {
        if (instant == null) return "-";
        return DATE_FORMAT.format(instant);
    }

    private static String formatRelative(Instant instant) {
        if (instant == null) return "";

        Duration ago = Duration.between(instant, Instant.now());
        long seconds = ago.getSeconds();

        if (seconds < 60) return "just now";
        if (seconds < 3600) return (seconds / 60) + "m ago";
        if (seconds < 86400) return (seconds / 3600) + "h ago";
        return (seconds / 86400) + "d ago";
    }

    private static String formatDuration(Duration duration) {
        if (duration == null) return "-";

        long millis = duration.toMillis();
        if (millis < 1000) return millis + "ms";
        if (millis < 60000) return String.format("%.1fs", millis / 1000.0);
        if (millis < 3600000) return String.format("%.1fm", millis / 60000.0);
        return String.format("%.1fh", millis / 3600000.0);
    }

    private static String formatDurationMs(long millis) {
        if (millis < 1000) return millis + "ms";
        if (millis < 60000) return String.format("%.1fs", millis / 1000.0);
        if (millis < 3600000) return String.format("%.1fm", millis / 60000.0);
        return String.format("%.1fh", millis / 3600000.0);
    }

    private static String formatOperationType(String type) {
        if (type == null || type.isBlank()) {
            return "";
        }
        return type.toUpperCase(Locale.ROOT).replace('_', ' ');
    }

    private static String formatEngineType(String engineType) {
        if (engineType == null || engineType.isBlank()) {
            return "-";
        }
        return engineType.toUpperCase(Locale.ROOT);
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static String formatNumber(long number) {
        if (number < 1_000) return String.valueOf(number);
        if (number < 1_000_000) return String.format("%.1fK", number / 1_000.0);
        if (number < 1_000_000_000) return String.format("%.1fM", number / 1_000_000.0);
        return String.format("%.2fB", number / 1_000_000_000.0);
    }

    private static Map<String, Object> extractMetrics(OperationRecord record) {
        if (record == null) {
            return null;
        }
        if (record.normalizedMetrics() != null) {
            return record.normalizedMetrics();
        }
        if (record.results() != null && record.results().aggregatedMetrics() != null) {
            return record.results().aggregatedMetrics();
        }
        return null;
    }

    private static long getLong(Map<String, Object> metrics, String key) {
        if (metrics == null || key == null) {
            return 0;
        }
        Object value = metrics.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text) {
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    private static String buildMetricsSummary(Map<String, Object> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return null;
        }
        List<String> parts = new ArrayList<>();

        addMetric(parts, "files", metrics, NormalizedMetrics.FILES_REWRITTEN, false);
        addMetric(parts, "bytes", metrics, NormalizedMetrics.BYTES_REWRITTEN, true);
        addMetric(parts, "manifests", metrics, NormalizedMetrics.MANIFESTS_REWRITTEN, false);
        addMetric(parts, "snapshots", metrics, NormalizedMetrics.SNAPSHOTS_EXPIRED, false);
        addMetric(parts, "delete files", metrics, NormalizedMetrics.DELETE_FILES_REMOVED, false);
        addMetric(parts, "orphan files", metrics, NormalizedMetrics.ORPHAN_FILES_REMOVED, false);

        return parts.isEmpty() ? null : String.join(" \u00b7 ", parts);
    }

    private static void addMetric(
            List<String> parts,
            String label,
            Map<String, Object> metrics,
            String key,
            boolean bytes) {
        if (metrics == null || key == null) {
            return;
        }
        if (!metrics.containsKey(key)) {
            return;
        }
        long value = getLong(metrics, key);
        String formatted = bytes ? formatBytes(value) : formatNumber(value);
        parts.add(label + ": " + formatted);
    }

    private static void addMetricView(
            List<MetricView> parts,
            String label,
            Map<String, Object> metrics,
            String key,
            boolean bytes) {
        if (metrics == null || key == null) {
            return;
        }
        if (!metrics.containsKey(key)) {
            return;
        }
        long value = getLong(metrics, key);
        String formatted = bytes ? formatBytes(value) : formatNumber(value);
        parts.add(new MetricView(label, formatted));
    }
}
