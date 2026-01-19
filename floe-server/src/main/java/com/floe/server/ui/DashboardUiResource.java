package com.floe.server.ui;

import com.floe.core.operation.OperationRecord;
import com.floe.core.operation.OperationStore;
import com.floe.core.policy.PolicyStore;
import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** API resource for dashboard HTML fragments (htmx). */
@Path("/{root:ui|}")
@Produces(MediaType.TEXT_HTML)
public class DashboardUiResource extends UiResource {

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("MMM d, h:mm a").withZone(ZoneId.systemDefault());

    @Inject OperationStore operationStore;

    @Inject PolicyStore policyStore;

    @Inject Template dashboard;

    @GET
    public TemplateInstance dashboard() {
        return page(dashboard, "/ui/").data("lastUpdated", Instant.now().toString());
    }

    @CheckedTemplate(basePath = "")
    static class Templates {

        static native TemplateInstance statsGrid(
                long totalOperations, String successRate, long runningCount, long activePolicies);

        static native TemplateInstance recentOperations(List<RecentOperationView> operations);
    }

    @GET
    @Path("/stats")
    public TemplateInstance stats() {
        var stats = operationStore.getStats(Duration.ofHours(24));

        return Templates.statsGrid(
                stats.totalOperations(),
                String.format("%.0f", stats.successRate()),
                stats.runningCount(),
                policyStore.countEnabled());
    }

    @GET
    @Path("/recent-operations")
    public TemplateInstance recentOperations() {
        List<RecentOperationView> views =
                operationStore.findRecent(5, 0).stream().map(RecentOperationView::from).toList();
        return Templates.recentOperations(views);
    }

    /** View model for recent operations on dashboard. */
    public record RecentOperationView(
            String qualifiedTableName,
            String namespace,
            String tableName,
            String policyName,
            String status,
            String startedAt,
            String durationFormatted) {
        public static RecentOperationView from(OperationRecord record) {
            return new RecentOperationView(
                    record.qualifiedTableName(),
                    record.namespace(),
                    record.tableName(),
                    record.policyName() != null ? record.policyName() : "-",
                    record.status().name(),
                    formatInstant(record.startedAt()),
                    formatDuration(record.duration()));
        }
    }

    private static String formatInstant(Instant instant) {
        if (instant == null) return "-";
        return DATE_FORMAT.format(instant);
    }

    private static String formatDuration(Duration duration) {
        if (duration == null) return "-";

        long millis = duration.toMillis();
        if (millis < 1000) return millis + "ms";
        if (millis < 60000) return String.format("%.1fs", millis / 1000.0);
        if (millis < 3600000) return String.format("%.1fm", millis / 60000.0);
        return String.format("%.1fh", millis / 3600000.0);
    }
}
