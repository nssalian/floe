package com.floe.server.ui;

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
import java.util.List;
import java.util.Locale;
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
            String startedAtFormatted,
            String completedAtFormatted,
            String durationFormatted,
            String errorMessage,
            String completedAt,
            ResultsView results) {
        public static OperationDetailView from(OperationRecord record) {
            return new OperationDetailView(
                    record.id().toString(),
                    record.status().name(),
                    record.tableName(),
                    record.catalog(),
                    record.namespace(),
                    record.policyName(),
                    record.policyId() != null ? record.policyId().toString() : null,
                    formatInstant(record.startedAt()),
                    record.completedAt() != null ? formatInstant(record.completedAt()) : null,
                    formatDuration(record.duration()),
                    record.errorMessage(),
                    record.completedAt() != null ? record.completedAt().toString() : null,
                    record.results() != null ? ResultsView.from(record.results()) : null);
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
            String operationType, String status, String durationFormatted, String errorMessage) {
        public static SingleResultView from(
                com.floe.core.operation.OperationResults.SingleOperationResult result) {
            return new SingleResultView(
                    formatOperationType(result.operationType().name()),
                    result.status(),
                    formatDurationMs(result.durationMs()),
                    result.errorMessage());
        }
    }

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
}
