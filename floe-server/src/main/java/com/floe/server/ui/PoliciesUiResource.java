package com.floe.server.ui;

import com.floe.core.health.HealthThresholds;
import com.floe.core.policy.ExpireSnapshotsConfig;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OrphanCleanupConfig;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.RewriteDataFilesConfig;
import com.floe.core.policy.RewriteManifestsConfig;
import com.floe.core.policy.ScheduleConfig;
import com.floe.core.policy.TablePattern;
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
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** UI resource for policies pages. */
@Path("/ui/policies")
@Produces(MediaType.TEXT_HTML)
public class PoliciesUiResource extends UiResource {

    private static final Logger LOG = LoggerFactory.getLogger(PoliciesUiResource.class);

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("MMM d, yyyy h:mm a").withZone(ZoneId.systemDefault());

    @Inject Template policies;

    @Inject
    @io.quarkus.qute.Location("policy-detail.html")
    Template policyDetail;

    @Inject PolicyStore policyStore;

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 100;

    /** Main policies list page with pagination. */
    @GET
    public TemplateInstance index(
            @QueryParam("limit") @DefaultValue("50") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        LOG.debug("Loading policies list: limit={}, offset={}", limit, offset);

        limit = Math.max(1, Math.min(limit, MAX_LIMIT));
        offset = Math.max(0, offset);

        List<MaintenancePolicy> paginatedPolicies = policyStore.listAll(limit, offset);
        int totalCount = policyStore.count();
        int enabledCount = policyStore.countEnabled();

        List<PolicyListView> views = paginatedPolicies.stream().map(PolicyListView::from).toList();

        boolean hasMore = offset + views.size() < totalCount;

        return page(policies, "/ui/policies")
                .data("policies", views)
                .data("totalCount", totalCount)
                .data("enabledCount", enabledCount)
                .data("disabledCount", totalCount - enabledCount)
                .data("limit", limit)
                .data("offset", offset)
                .data("hasMore", hasMore)
                .data("nextOffset", offset + limit)
                .data("prevOffset", Math.max(0, offset - limit))
                .data("hasPrev", offset > 0);
    }

    /** Policy detail page. */
    @GET
    @Path("/{id}")
    public Response detail(@PathParam("id") String id) {
        LOG.debug("Viewing policy: {}", id);

        Optional<MaintenancePolicy> policyOpt = policyStore.getById(id);

        if (policyOpt.isEmpty()) {
            return Response.seeOther(URI.create("/ui/policies")).build();
        }

        PolicyDetailView view = PolicyDetailView.from(policyOpt.get());
        TemplateInstance template = page(policyDetail, "/ui/policies").data("policy", view);

        return Response.ok(template).build();
    }

    // View models for templates

    /** View model for policies list. */
    public record PolicyListView(
            String id,
            String name,
            String description,
            String tablePatternDisplay,
            boolean enabled,
            int priority,
            boolean hasRewriteDataFiles,
            boolean hasExpireSnapshots,
            boolean hasOrphanCleanup,
            boolean hasRewriteManifests,
            boolean hasAnyOperations) {
        public static PolicyListView from(MaintenancePolicy policy) {
            return new PolicyListView(
                    policy.id(),
                    policy.getNameOrDefault(),
                    policy.description(),
                    formatTablePattern(policy.tablePattern()),
                    Boolean.TRUE.equals(policy.enabled()),
                    policy.priority(),
                    policy.rewriteDataFiles() != null,
                    policy.expireSnapshots() != null,
                    policy.orphanCleanup() != null,
                    policy.rewriteManifests() != null,
                    policy.hasAnyOperations());
        }
    }

    /** View model for policy detail page. */
    public record PolicyDetailView(
            String id,
            String name,
            String description,
            String tablePatternDisplay,
            boolean enabled,
            int priority,
            boolean hasRewriteDataFiles,
            boolean hasExpireSnapshots,
            boolean hasOrphanCleanup,
            boolean hasRewriteManifests,
            int operationCount,
            String createdAtFormatted,
            String updatedAtFormatted,
            boolean hasTags,
            List<TagView> tags,
            List<OperationConfigView> operationConfigs,
            ThresholdsView thresholds) {
        public static PolicyDetailView from(MaintenancePolicy policy) {
            int opCount = 0;
            if (policy.rewriteDataFiles() != null) opCount++;
            if (policy.expireSnapshots() != null) opCount++;
            if (policy.orphanCleanup() != null) opCount++;
            if (policy.rewriteManifests() != null) opCount++;

            List<TagView> tagViews = List.of();
            if (policy.tags() != null && !policy.tags().isEmpty()) {
                tagViews =
                        policy.tags().entrySet().stream()
                                .map(e -> new TagView(e.getKey(), e.getValue()))
                                .toList();
            }

            List<OperationConfigView> operationConfigs = buildOperationConfigs(policy);
            ThresholdsView thresholds = ThresholdsView.from(policy.healthThresholds());

            return new PolicyDetailView(
                    policy.id(),
                    policy.getNameOrDefault(),
                    policy.description(),
                    formatTablePattern(policy.tablePattern()),
                    Boolean.TRUE.equals(policy.enabled()),
                    policy.priority(),
                    policy.rewriteDataFiles() != null,
                    policy.expireSnapshots() != null,
                    policy.orphanCleanup() != null,
                    policy.rewriteManifests() != null,
                    opCount,
                    formatInstant(policy.createdAt()),
                    formatInstant(policy.updatedAt()),
                    policy.tags() != null && !policy.tags().isEmpty(),
                    tagViews,
                    operationConfigs,
                    thresholds);
        }
    }

    /**
     * View model for tags.
     *
     * @param key tag key
     * @param value tag value
     */
    public record TagView(String key, String value) {}

    public record OperationConfigView(
            String name,
            String description,
            boolean configured,
            String scheduleSummary,
            List<ConfigItemView> configItems) {}

    public record ConfigItemView(String label, String value) {}

    public record ThresholdsView(boolean usesDefaults, List<ThresholdItemView> items) {
        public static ThresholdsView from(HealthThresholds thresholds) {
            HealthThresholds effective =
                    thresholds != null ? thresholds : HealthThresholds.defaults();
            List<ThresholdItemView> items =
                    List.of(
                            new ThresholdItemView(
                                    "Small files (%)",
                                    formatPercent(effective.smallFilePercentWarning()),
                                    formatPercent(effective.smallFilePercentCritical())),
                            new ThresholdItemView(
                                    "Large files (%)",
                                    formatPercent(effective.largeFilePercentWarning()),
                                    formatPercent(effective.largeFilePercentCritical())),
                            new ThresholdItemView(
                                    "File count",
                                    String.valueOf(effective.fileCountWarning()),
                                    String.valueOf(effective.fileCountCritical())),
                            new ThresholdItemView(
                                    "Snapshot count",
                                    String.valueOf(effective.snapshotCountWarning()),
                                    String.valueOf(effective.snapshotCountCritical())),
                            new ThresholdItemView(
                                    "Snapshot age (days)",
                                    String.valueOf(effective.snapshotAgeWarningDays()),
                                    String.valueOf(effective.snapshotAgeCriticalDays())),
                            new ThresholdItemView(
                                    "Delete file count",
                                    String.valueOf(effective.deleteFileCountWarning()),
                                    String.valueOf(effective.deleteFileCountCritical())),
                            new ThresholdItemView(
                                    "Delete file ratio",
                                    formatPercent(effective.deleteFileRatioWarning() * 100),
                                    formatPercent(effective.deleteFileRatioCritical() * 100)),
                            new ThresholdItemView(
                                    "Manifest count",
                                    String.valueOf(effective.manifestCountWarning()),
                                    String.valueOf(effective.manifestCountCritical())),
                            new ThresholdItemView(
                                    "Manifest size",
                                    formatBytes(effective.manifestSizeWarningBytes()),
                                    formatBytes(effective.manifestSizeCriticalBytes())),
                            new ThresholdItemView(
                                    "Partition count",
                                    String.valueOf(effective.partitionCountWarning()),
                                    String.valueOf(effective.partitionCountCritical())),
                            new ThresholdItemView(
                                    "Partition skew (x)",
                                    formatNumber(effective.partitionSkewWarning()),
                                    formatNumber(effective.partitionSkewCritical())),
                            new ThresholdItemView(
                                    "Stale metadata (days)",
                                    String.valueOf(effective.staleMetadataWarningDays()),
                                    String.valueOf(effective.staleMetadataCriticalDays())));
            return new ThresholdsView(thresholds == null, items);
        }
    }

    public record ThresholdItemView(String label, String warning, String critical) {}

    // Formatting helpers

    private static String formatTablePattern(TablePattern pattern) {
        if (pattern == null) {
            return "*.*.*";
        }
        return pattern.toString();
    }

    private static String formatInstant(Instant instant) {
        if (instant == null || instant.equals(Instant.EPOCH)) {
            return "-";
        }
        return DATE_FORMAT.format(instant);
    }

    private static List<OperationConfigView> buildOperationConfigs(MaintenancePolicy policy) {
        List<OperationConfigView> configs = new ArrayList<>();

        configs.add(
                new OperationConfigView(
                        "Rewrite Data Files",
                        "Compaction settings and schedule.",
                        policy.rewriteDataFiles() != null,
                        formatSchedule(policy.rewriteDataFilesSchedule()),
                        buildRewriteDataFilesConfig(policy.rewriteDataFiles())));

        configs.add(
                new OperationConfigView(
                        "Expire Snapshots",
                        "Snapshot retention and cleanup settings.",
                        policy.expireSnapshots() != null,
                        formatSchedule(policy.expireSnapshotsSchedule()),
                        buildExpireSnapshotsConfig(policy.expireSnapshots())));

        configs.add(
                new OperationConfigView(
                        "Orphan Cleanup",
                        "Orphan file cleanup settings.",
                        policy.orphanCleanup() != null,
                        formatSchedule(policy.orphanCleanupSchedule()),
                        buildOrphanCleanupConfig(policy.orphanCleanup())));

        configs.add(
                new OperationConfigView(
                        "Rewrite Manifests",
                        "Manifest rewrite settings.",
                        policy.rewriteManifests() != null,
                        formatSchedule(policy.rewriteManifestsSchedule()),
                        buildRewriteManifestsConfig(policy.rewriteManifests())));

        return configs;
    }

    private static List<ConfigItemView> buildRewriteDataFilesConfig(RewriteDataFilesConfig config) {
        if (config == null) {
            return List.of();
        }
        List<ConfigItemView> items = new ArrayList<>();
        items.add(new ConfigItemView("Strategy", valueOrDash(config.strategy())));
        if (config.targetFileSizeBytes() != null) {
            items.add(
                    new ConfigItemView(
                            "Target file size", formatBytes(config.targetFileSizeBytes())));
        }
        if (config.maxFileGroupSizeBytes() != null) {
            items.add(
                    new ConfigItemView(
                            "Max group size", formatBytes(config.maxFileGroupSizeBytes())));
        }
        if (config.maxConcurrentFileGroupRewrites() != null) {
            items.add(
                    new ConfigItemView(
                            "Max concurrent rewrites",
                            String.valueOf(config.maxConcurrentFileGroupRewrites())));
        }
        if (config.rewriteJobOrder() != null) {
            items.add(new ConfigItemView("Job order", config.rewriteJobOrder()));
        }
        if (config.filter() != null) {
            items.add(new ConfigItemView("Filter", config.filter()));
        }
        if (config.sortOrder() != null && !config.sortOrder().isEmpty()) {
            items.add(new ConfigItemView("Sort order", String.join(", ", config.sortOrder())));
        }
        if (config.zOrderColumns() != null && !config.zOrderColumns().isEmpty()) {
            items.add(new ConfigItemView("Z-order", String.join(", ", config.zOrderColumns())));
        }
        return items;
    }

    private static List<ConfigItemView> buildExpireSnapshotsConfig(ExpireSnapshotsConfig config) {
        if (config == null) {
            return List.of();
        }
        List<ConfigItemView> items = new ArrayList<>();
        if (config.retainLast() != null) {
            items.add(new ConfigItemView("Retain last", String.valueOf(config.retainLast())));
        }
        if (config.maxSnapshotAge() != null) {
            items.add(
                    new ConfigItemView(
                            "Max snapshot age", formatDuration(config.maxSnapshotAge())));
        }
        if (config.cleanExpiredMetadata() != null) {
            items.add(
                    new ConfigItemView(
                            "Clean expired metadata",
                            config.cleanExpiredMetadata() ? "true" : "false"));
        }
        if (config.expireSnapshotId() != null) {
            items.add(
                    new ConfigItemView(
                            "Expire snapshot ID", String.valueOf(config.expireSnapshotId())));
        }
        return items;
    }

    private static List<ConfigItemView> buildOrphanCleanupConfig(OrphanCleanupConfig config) {
        if (config == null) {
            return List.of();
        }
        List<ConfigItemView> items = new ArrayList<>();
        if (config.retentionPeriodInDays() != null) {
            items.add(
                    new ConfigItemView(
                            "Retention", formatDuration(config.retentionPeriodInDays())));
        }
        if (config.location() != null) {
            items.add(new ConfigItemView("Location", config.location()));
        }
        if (config.prefixMismatchMode() != null) {
            items.add(new ConfigItemView("Prefix mismatch", config.prefixMismatchMode()));
        }
        if (config.equalSchemes() != null && !config.equalSchemes().isEmpty()) {
            items.add(
                    new ConfigItemView(
                            "Equal schemes", String.join(", ", config.equalSchemes().keySet())));
        }
        if (config.equalAuthorities() != null && !config.equalAuthorities().isEmpty()) {
            items.add(
                    new ConfigItemView(
                            "Equal authorities",
                            String.join(", ", config.equalAuthorities().keySet())));
        }
        return items;
    }

    private static List<ConfigItemView> buildRewriteManifestsConfig(RewriteManifestsConfig config) {
        if (config == null) {
            return List.of();
        }
        List<ConfigItemView> items = new ArrayList<>();
        if (config.specId() != null) {
            items.add(new ConfigItemView("Spec ID", String.valueOf(config.specId())));
        }
        if (config.stagingLocation() != null) {
            items.add(new ConfigItemView("Staging", config.stagingLocation()));
        }
        if (config.sortBy() != null && !config.sortBy().isEmpty()) {
            items.add(new ConfigItemView("Sort by", String.join(", ", config.sortBy())));
        }
        if (config.rewriteIf() != null) {
            items.add(new ConfigItemView("Rewrite if", "custom filter"));
        }
        return items;
    }

    private static String formatSchedule(ScheduleConfig schedule) {
        if (schedule == null) {
            return "-";
        }
        if (schedule.enabled() != null && !schedule.enabled()) {
            return "Disabled";
        }
        List<String> parts = new ArrayList<>();
        if (schedule.cronExpression() != null && !schedule.cronExpression().isBlank()) {
            parts.add("cron: " + schedule.cronExpression());
        } else if (schedule.intervalInDays() != null) {
            parts.add("every " + formatDuration(schedule.intervalInDays()));
        }
        if (schedule.windowStart() != null || schedule.windowEnd() != null) {
            parts.add(
                    "window: "
                            + (schedule.windowStart() != null ? schedule.windowStart() : "start")
                            + " - "
                            + (schedule.windowEnd() != null ? schedule.windowEnd() : "end"));
        }
        if (schedule.allowedDays() != null && !schedule.allowedDays().isEmpty()) {
            parts.add("days: " + joinDays(schedule.allowedDays()));
        }
        if (schedule.timeoutInHours() != null) {
            parts.add("timeout: " + formatDuration(schedule.timeoutInHours()));
        }
        return parts.isEmpty() ? "Enabled" : String.join(", ", parts);
    }

    private static String joinDays(Set<java.time.DayOfWeek> days) {
        return days.stream().map(d -> d.name().substring(0, 3)).sorted().toList().toString();
    }

    private static String valueOrDash(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }

    private static String formatDuration(Duration duration) {
        if (duration == null) {
            return "-";
        }
        long seconds = duration.getSeconds();
        if (seconds % 86400 == 0) {
            return (seconds / 86400) + "d";
        }
        if (seconds % 3600 == 0) {
            return (seconds / 3600) + "h";
        }
        if (seconds % 60 == 0) {
            return (seconds / 60) + "m";
        }
        return seconds + "s";
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static String formatPercent(double value) {
        return String.format("%.1f%%", value);
    }

    private static String formatNumber(double value) {
        return String.format("%.1f", value);
    }
}
