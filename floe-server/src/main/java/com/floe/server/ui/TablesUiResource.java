/*
 * Copyright 2026 The Floe Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.floe.server.ui;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.catalog.TableMetadata;
import com.floe.core.health.HealthIssue;
import com.floe.core.health.HealthReport;
import com.floe.core.health.HealthThresholds;
import com.floe.core.health.TableHealthAssessor;
import com.floe.core.health.TableHealthStore;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.operation.OperationStats;
import com.floe.core.operation.OperationStore;
import com.floe.core.orchestrator.MaintenanceDebtScore;
import com.floe.core.orchestrator.MaintenancePlanner;
import com.floe.core.orchestrator.PlannedOperation;
import com.floe.core.policy.ExpireSnapshotsConfig;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OrphanCleanupConfig;
import com.floe.core.policy.PolicyMatcher;
import com.floe.core.policy.RewriteDataFilesConfig;
import com.floe.core.policy.RewriteManifestsConfig;
import com.floe.core.policy.ScheduleConfig;
import com.floe.core.policy.TablePattern;
import com.floe.server.config.FloeConfig;
import com.floe.server.config.HealthConfig;
import com.floe.server.health.HealthReportCache;
import com.floe.server.health.HealthReportCache.HealthCacheKey;
import com.floe.server.scheduler.SchedulerConfig;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** UI resource for tables pages. */
@Path("/ui/tables")
@Produces(MediaType.TEXT_HTML)
public class TablesUiResource extends UiResource {

    private static final Logger LOG = LoggerFactory.getLogger(TablesUiResource.class);

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("MMM d, yyyy h:mm a").withZone(ZoneId.systemDefault());

    @Inject Template tables;

    @Inject
    @io.quarkus.qute.Location("table-detail.html")
    Template tableDetail;

    @Inject CatalogClient catalogClient;

    @Inject FloeConfig config;

    @Inject HealthConfig healthConfig;

    @Inject TableHealthStore healthStore;

    @Inject PolicyMatcher policyMatcher;

    @Inject OperationStore operationStore;

    @Inject SchedulerConfig schedulerConfig;

    @Inject HealthReportCache healthReportCache;

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 100;

    /** Main tables list page with pagination. */
    @GET
    public TemplateInstance index(
            @QueryParam("limit") @DefaultValue("50") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        LOG.debug("Loading tables list: limit={}, offset={}", limit, offset);

        // Clamp limit to valid range
        limit = Math.max(1, Math.min(limit, MAX_LIMIT));
        offset = Math.max(0, offset);

        boolean catalogHealthy = false;
        List<TableListView> tableViews = List.of();
        List<String> namespaces = List.of();
        String errorMessage = null;
        int totalTables = 0;
        boolean hasMore = false;

        try {
            catalogHealthy = catalogClient.isHealthy();
            if (catalogHealthy) {
                namespaces = catalogClient.listNamespaces();
                List<TableIdentifier> allTables = catalogClient.listAllTables();
                totalTables = allTables.size();

                // Apply pagination
                List<TableIdentifier> paginatedTables =
                        allTables.stream().skip(offset).limit(limit).toList();

                tableViews = paginatedTables.stream().map(TableListView::from).toList();
                hasMore = offset + tableViews.size() < totalTables;
            }
        } catch (Exception e) {
            LOG.error("Failed to load tables", e);
            errorMessage = "Failed to connect to catalog: " + e.getMessage();
        }

        return page(tables, "/ui/tables")
                .data("catalogName", catalogClient.getCatalogName())
                .data("catalogType", formatCatalogType(config.catalog().type()))
                .data("catalogHealthy", catalogHealthy)
                .data("tables", tableViews)
                .data("namespaces", namespaces)
                .data("totalTables", totalTables)
                .data("totalNamespaces", namespaces.size())
                .data("errorMessage", errorMessage)
                .data("limit", limit)
                .data("offset", offset)
                .data("hasMore", hasMore)
                .data("nextOffset", offset + limit)
                .data("prevOffset", Math.max(0, offset - limit))
                .data("hasPrev", offset > 0);
    }

    /** Table detail page. */
    @GET
    @Path("/{namespace}/{table}")
    public Response detail(
            @PathParam("namespace") String namespace, @PathParam("table") String tableName) {
        LOG.debug("Loading table detail: {}.{}", namespace, tableName);

        try {
            TableIdentifier id =
                    TableIdentifier.of(catalogClient.getCatalogName(), namespace, tableName);

            // Load the table for health assessment
            Optional<Table> tableOpt = catalogClient.loadTable(id);
            if (tableOpt.isEmpty()) {
                return Response.seeOther(URI.create("/ui/tables")).build();
            }

            Optional<TableMetadata> metadataOpt = catalogClient.getTableMetadata(id);
            if (metadataOpt.isEmpty()) {
                return Response.seeOther(URI.create("/ui/tables")).build();
            }

            MaintenancePolicy matchedPolicy =
                    policyMatcher
                            .findEffectivePolicy(catalogClient.getCatalogName(), id)
                            .orElse(null);

            HealthThresholds thresholds =
                    matchedPolicy != null
                            ? matchedPolicy.effectiveThresholds()
                            : HealthThresholds.defaults();
            TableHealthAssessor.ScanMode scanMode =
                    TableHealthAssessor.ScanMode.fromString(healthConfig.scanMode());

            // Perform health assessment
            HealthReport healthReport;
            boolean fromCache = false;
            HealthCacheKey cacheKey =
                    HealthCacheKey.of(id, scanMode, healthConfig.sampleLimit(), thresholds);
            Optional<HealthReport> cachedReport = healthReportCache.getIfFresh(cacheKey);
            if (cachedReport.isPresent()) {
                healthReport = cachedReport.get();
                fromCache = true;
            } else {
                TableHealthAssessor assessor =
                        new TableHealthAssessor(thresholds, scanMode, healthConfig.sampleLimit());
                healthReport = assessor.assess(id, tableOpt.get());
                healthReportCache.put(cacheKey, healthReport);
            }

            // Persist health report when enabled
            if (!fromCache && healthConfig.persistenceEnabled() && healthStore != null) {
                try {
                    if (shouldPersistHealth(id, healthReport)) {
                        healthStore.save(healthReport);
                        if (healthConfig.maxReportsPerTable() > 0) {
                            healthStore.pruneHistory(
                                    id.catalog(),
                                    id.namespace(),
                                    id.table(),
                                    healthConfig.maxReportsPerTable());
                        }
                    }
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to persist health report for {}: {}",
                            id.toQualifiedName(),
                            e.getMessage());
                }
            }

            OperationStats recentStats =
                    operationStore != null
                            ? operationStore.getStatsForTable(
                                    id.catalog(), id.namespace(), id.table(), Duration.ofHours(24))
                            : null;

            TableDetailView view =
                    TableDetailView.from(
                            metadataOpt.get(),
                            healthReport,
                            matchedPolicy,
                            thresholds,
                            scanMode,
                            healthConfig.sampleLimit(),
                            schedulerConfig,
                            recentStats);
            TemplateInstance template =
                    page(tableDetail, "/ui/tables")
                            .data("table", view)
                            .data("catalogName", catalogClient.getCatalogName());

            return Response.ok(template).build();
        } catch (Exception e) {
            LOG.error("Failed to load table {}.{}", namespace, tableName, e);
            return Response.seeOther(URI.create("/ui/tables")).build();
        }
    }

    // View models for templates

    /** View model for tables list. */
    public record TableListView(
            String catalog, String namespace, String name, String qualifiedName) {
        public static TableListView from(TableIdentifier id) {
            return new TableListView(
                    id.catalog(), id.namespace(), id.table(), id.toQualifiedName());
        }
    }

    /** View model for table detail page. */
    public record TableDetailView(
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
            String totalDataSizeFormatted,
            String averageFileSizeFormatted,
            long totalRecordCount,
            String totalRecordCountFormatted,
            int manifestCount,
            String totalManifestSizeFormatted,
            int formatVersion,
            String partitionSpec,
            String sortOrder,
            String lastModifiedFormatted,
            boolean isEmpty,
            // Health assessment fields
            String healthStatus,
            int smallFileCount,
            int deleteFileCount,
            int positionDeleteFileCount,
            int equalityDeleteFileCount,
            int partitionCount,
            List<HealthIssueView> issues,
            List<RecommendationView> recommendations,
            String matchedPolicyName,
            String matchedPolicyId,
            int currentScore,
            ThresholdsView thresholds,
            ScanModeView scanMode,
            AutoModeView autoMode,
            List<ScheduleView> schedules) {
        public static TableDetailView from(
                TableMetadata meta,
                HealthReport health,
                MaintenancePolicy policy,
                HealthThresholds thresholds,
                TableHealthAssessor.ScanMode scanMode,
                int sampleLimit,
                SchedulerConfig schedulerConfig,
                OperationStats recentStats) {
            String status = "healthy";
            if (health.issues().stream()
                    .anyMatch(i -> i.severity() == HealthIssue.Severity.CRITICAL)) {
                status = "critical";
            } else if (health.issues().stream()
                    .anyMatch(i -> i.severity() == HealthIssue.Severity.WARNING)) {
                status = "warning";
            }

            return new TableDetailView(
                    meta.identifier().catalog(),
                    meta.identifier().namespace(),
                    meta.identifier().table(),
                    meta.identifier().toQualifiedName(),
                    meta.location(),
                    meta.snapshotCount(),
                    meta.currentSnapshotId(),
                    formatInstant(meta.currentSnapshotTimestamp()),
                    formatInstant(meta.oldestSnapshotTimestamp()),
                    meta.dataFileCount(),
                    formatBytes(meta.totalDataFileSizeBytes()),
                    formatAverageFileSize(meta.totalDataFileSizeBytes(), meta.dataFileCount()),
                    meta.totalRecordCount(),
                    formatNumber(meta.totalRecordCount()),
                    meta.manifestCount(),
                    formatBytes(meta.totalManifestSizeBytes()),
                    meta.formatVersion(),
                    meta.partitionSpec(),
                    meta.sortOrder(),
                    formatInstant(meta.lastModified()),
                    meta.isEmpty(),
                    status,
                    health.smallFileCount(),
                    meta.deleteFileCount(),
                    meta.positionDeleteFileCount(),
                    meta.equalityDeleteFileCount(),
                    health.partitionCount(),
                    health.issues().stream().map(HealthIssueView::from).toList(),
                    buildRecommendations(health, thresholds),
                    policy != null ? policy.name() : null,
                    policy != null ? policy.id() : null,
                    computeScore(health),
                    ThresholdsView.from(thresholds, policy),
                    ScanModeView.from(scanMode, sampleLimit),
                    AutoModeView.from(health, recentStats, schedulerConfig),
                    buildSchedules(policy));
        }
    }

    /** View model for health issues. */
    public record HealthIssueView(String type, String severity, String message) {
        public static HealthIssueView from(HealthIssue issue) {
            return new HealthIssueView(
                    formatIssueType(issue.type()),
                    issue.severity().name().toLowerCase(Locale.ROOT),
                    issue.message());
        }

        private static String formatIssueType(HealthIssue.Type type) {
            return switch (type) {
                case TOO_MANY_SMALL_FILES -> "Too Many Small Files";
                case TOO_MANY_LARGE_FILES -> "Too Many Large Files";
                case HIGH_FILE_COUNT -> "High File Count";
                case TOO_MANY_SNAPSHOTS -> "Too Many Snapshots";
                case OLD_SNAPSHOTS -> "Old Snapshots";
                case TOO_MANY_DELETE_FILES -> "Too Many Delete Files";
                case HIGH_DELETE_FILE_RATIO -> "High Delete File Ratio";
                case TOO_MANY_MANIFESTS -> "Too Many Manifests";
                case LARGE_MANIFEST_LIST -> "Large Manifest List";
                case TOO_MANY_PARTITIONS -> "Too Many Partitions";
                case PARTITION_SKEW -> "Partition Skew";
                case TABLE_EMPTY -> "Table Empty";
                case STALE_METADATA -> "Stale Metadata";
            };
        }
    }

    /** View model for maintenance recommendations. */
    public record RecommendationView(String operationType, String severity, String reason) {
        public static RecommendationView from(PlannedOperation plan) {
            return new RecommendationView(
                    formatOperationType(plan.operationType()),
                    plan.severity().name().toLowerCase(Locale.ROOT),
                    plan.reason());
        }
    }

    public record ThresholdsView(String sourceLabel, List<ThresholdItem> items) {
        public static ThresholdsView from(HealthThresholds thresholds, MaintenancePolicy policy) {
            HealthThresholds resolved =
                    thresholds != null ? thresholds : HealthThresholds.defaults();
            String source =
                    policy != null
                            ? "Policy thresholds (" + policy.name() + ")"
                            : "Default thresholds";
            List<ThresholdItem> items = new ArrayList<>();
            items.add(
                    new ThresholdItem(
                            "Small file size", formatBytes(resolved.smallFileSizeBytes()), "-"));
            items.add(
                    new ThresholdItem(
                            "Small file percent",
                            formatPercent(resolved.smallFilePercentWarning()),
                            formatPercent(resolved.smallFilePercentCritical())));
            items.add(
                    new ThresholdItem(
                            "Large file size", formatBytes(resolved.largeFileSizeBytes()), "-"));
            items.add(
                    new ThresholdItem(
                            "Large file percent",
                            formatPercent(resolved.largeFilePercentWarning()),
                            formatPercent(resolved.largeFilePercentCritical())));
            items.add(
                    new ThresholdItem(
                            "Total file count",
                            formatNumber(resolved.fileCountWarning()),
                            formatNumber(resolved.fileCountCritical())));
            items.add(
                    new ThresholdItem(
                            "Snapshot count",
                            formatNumber(resolved.snapshotCountWarning()),
                            formatNumber(resolved.snapshotCountCritical())));
            items.add(
                    new ThresholdItem(
                            "Snapshot age (days)",
                            String.valueOf(resolved.snapshotAgeWarningDays()),
                            String.valueOf(resolved.snapshotAgeCriticalDays())));
            items.add(
                    new ThresholdItem(
                            "Delete file count",
                            formatNumber(resolved.deleteFileCountWarning()),
                            formatNumber(resolved.deleteFileCountCritical())));
            items.add(
                    new ThresholdItem(
                            "Delete file ratio",
                            formatPercent(resolved.deleteFileRatioWarning() * 100),
                            formatPercent(resolved.deleteFileRatioCritical() * 100)));
            items.add(
                    new ThresholdItem(
                            "Manifest count",
                            formatNumber(resolved.manifestCountWarning()),
                            formatNumber(resolved.manifestCountCritical())));
            items.add(
                    new ThresholdItem(
                            "Manifest size",
                            formatBytes(resolved.manifestSizeWarningBytes()),
                            formatBytes(resolved.manifestSizeCriticalBytes())));
            items.add(
                    new ThresholdItem(
                            "Partition count",
                            formatNumber(resolved.partitionCountWarning()),
                            formatNumber(resolved.partitionCountCritical())));
            items.add(
                    new ThresholdItem(
                            "Partition skew",
                            formatRatio(resolved.partitionSkewWarning()),
                            formatRatio(resolved.partitionSkewCritical())));
            items.add(
                    new ThresholdItem(
                            "Stale metadata (days)",
                            String.valueOf(resolved.staleMetadataWarningDays()),
                            String.valueOf(resolved.staleMetadataCriticalDays())));
            return new ThresholdsView(source, items);
        }
    }

    public record ThresholdItem(String label, String warning, String critical) {}

    public record ScanModeView(String mode, String detail, String caveat) {
        public static ScanModeView from(TableHealthAssessor.ScanMode mode, int sampleLimit) {
            String detail =
                    switch (mode) {
                        case SAMPLE -> "Sample up to " + formatNumber(sampleLimit) + " files";
                        case SCAN -> "Full scan of table files";
                        case METADATA -> "Metadata-only scan";
                    };
            String caveat =
                    mode == TableHealthAssessor.ScanMode.METADATA
                            ? "Metadata tables may be unavailable; snapshot fallback can hide signals."
                            : null;
            return new ScanModeView(mode.name().toLowerCase(Locale.ROOT), detail, caveat);
        }
    }

    public record AutoModeView(
            String debtScoreFormatted,
            List<DebtBreakdownItem> breakdown,
            String throttlingStatus,
            String backoffStatus,
            String schedulerStatus) {
        public static AutoModeView from(
                HealthReport report, OperationStats stats, SchedulerConfig schedulerConfig) {
            MaintenanceDebtScore debtScore = MaintenanceDebtScore.calculate(report, stats);
            List<DebtBreakdownItem> breakdown = new ArrayList<>();
            Map<String, Double> raw =
                    debtScore != null ? debtScore.breakdown() : new LinkedHashMap<>();
            for (String key : raw.keySet()) {
                breakdown.add(new DebtBreakdownItem(formatDebtKey(key), formatScore(raw.get(key))));
            }
            String throttling = "Normal";
            String backoff = "None";
            if (stats != null && schedulerConfig != null) {
                if (stats.consecutiveZeroChangeRuns() >= schedulerConfig.zeroChangeThreshold()) {
                    throttling =
                            "Reduced frequency by "
                                    + schedulerConfig.zeroChangeFrequencyReductionPercent()
                                    + "% (min "
                                    + schedulerConfig.zeroChangeMinIntervalHours()
                                    + "h)";
                }
                if (stats.consecutiveFailures() >= schedulerConfig.failureBackoffThreshold()) {
                    if (stats.lastRunAt() != null) {
                        Instant backoffUntil =
                                stats.lastRunAt()
                                        .plus(
                                                Duration.ofHours(
                                                        schedulerConfig.failureBackoffHours()));
                        backoff = "Until " + formatInstant(backoffUntil);
                    } else {
                        backoff = "Active";
                    }
                }
            }
            String schedulerStatus =
                    schedulerConfig != null && schedulerConfig.enabled() ? "Enabled" : "Disabled";
            return new AutoModeView(
                    formatScore(debtScore.score()),
                    breakdown,
                    throttling,
                    backoff,
                    schedulerStatus);
        }
    }

    public record DebtBreakdownItem(String label, String value) {}

    public record ScheduleView(String operation, String schedule, String status) {
        public static ScheduleView from(String operation, ScheduleConfig schedule) {
            if (schedule == null) {
                return new ScheduleView(operation, "Manual", "Disabled");
            }
            String label;
            if (schedule.cronExpression() != null && !schedule.cronExpression().isBlank()) {
                label = "Cron: " + schedule.cronExpression();
            } else if (schedule.intervalInDays() != null) {
                label = "Every " + formatDuration(schedule.intervalInDays());
            } else {
                label = "Manual";
            }
            String status = schedule.isEnabled() ? "Enabled" : "Disabled";
            return new ScheduleView(operation, label, status);
        }
    }

    // Formatting helpers

    private static String formatInstant(Instant instant) {
        if (instant == null) {
            return "-";
        }
        return DATE_FORMAT.format(instant);
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static String formatAverageFileSize(long totalBytes, int fileCount) {
        if (fileCount == 0) return "-";
        long avgBytes = totalBytes / fileCount;
        return formatBytes(avgBytes);
    }

    private static String formatNumber(long number) {
        if (number < 1_000) return String.valueOf(number);
        if (number < 1_000_000) return String.format("%.1fK", number / 1_000.0);
        if (number < 1_000_000_000) return String.format("%.1fM", number / 1_000_000.0);
        return String.format("%.2fB", number / 1_000_000_000.0);
    }

    private static String formatPercent(double percent) {
        if (Double.isNaN(percent)) {
            return "-";
        }
        return String.format("%.1f%%", percent);
    }

    private static String formatRatio(double ratio) {
        if (Double.isNaN(ratio)) {
            return "-";
        }
        return String.format("%.2f", ratio);
    }

    private static String formatScore(double score) {
        if (Double.isNaN(score)) {
            return "-";
        }
        return String.format("%.0f", score);
    }

    private static String formatDebtKey(String key) {
        return switch (key) {
            case "healthIssues" -> "Health issues";
            case "staleMetadata" -> "Stale metadata";
            case "failureRate" -> "Failure rate";
            case "failureStreak" -> "Failure streak";
            default -> key;
        };
    }

    private static String formatDuration(Duration duration) {
        if (duration == null) {
            return "-";
        }
        long seconds = duration.getSeconds();
        if (seconds < 60) {
            return seconds + "s";
        }
        if (seconds < 3600) {
            return (seconds / 60) + "m";
        }
        if (seconds < 86400) {
            return (seconds / 3600) + "h";
        }
        return (seconds / 86400) + "d";
    }

    private static String formatCatalogType(String type) {
        return switch (type.toUpperCase(Locale.ROOT)) {
            case "REST" -> "REST Catalog";
            case "HIVE" -> "Hive Metastore";
            case "POLARIS" -> "Polaris";
            case "NESSIE" -> "Nessie";
            default -> type;
        };
    }

    private static String formatOperationType(MaintenanceOperation.Type type) {
        if (type == null) {
            return "";
        }
        return type.name().toLowerCase(Locale.ROOT).replace('_', ' ');
    }

    private static List<RecommendationView> buildRecommendations(
            HealthReport health, HealthThresholds thresholds) {
        if (health == null) {
            return List.of();
        }

        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .name("ui-recommendations")
                        .tablePattern(TablePattern.matchAll())
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .expireSnapshots(ExpireSnapshotsConfig.defaults())
                        .orphanCleanup(OrphanCleanupConfig.defaults())
                        .rewriteManifests(RewriteManifestsConfig.defaults())
                        .build();

        MaintenancePlanner planner = new MaintenancePlanner();
        HealthThresholds resolved = thresholds != null ? thresholds : HealthThresholds.defaults();
        List<PlannedOperation> plans = planner.plan(health, policy, resolved);
        return plans.stream().map(RecommendationView::from).toList();
    }

    private static List<ScheduleView> buildSchedules(MaintenancePolicy policy) {
        if (policy == null) {
            return List.of();
        }
        List<ScheduleView> schedules = new ArrayList<>();
        schedules.add(ScheduleView.from("Rewrite data files", policy.rewriteDataFilesSchedule()));
        schedules.add(ScheduleView.from("Expire snapshots", policy.expireSnapshotsSchedule()));
        schedules.add(ScheduleView.from("Orphan cleanup", policy.orphanCleanupSchedule()));
        schedules.add(ScheduleView.from("Rewrite manifests", policy.rewriteManifestsSchedule()));
        return schedules;
    }

    private boolean shouldPersistHealth(TableIdentifier id, HealthReport report) {
        List<HealthReport> history =
                healthStore.findHistory(id.catalog(), id.namespace(), id.table(), 1);
        if (history.isEmpty()) {
            return true;
        }
        HealthReport latest = history.get(0);
        int latestScore = computeScore(latest);
        int currentScore = computeScore(report);
        if (latestScore != currentScore) {
            return true;
        }
        return !issueSignature(latest).equals(issueSignature(report));
    }

    private static int computeScore(HealthReport report) {
        if (report == null) {
            return 100;
        }
        int score = 100;
        for (HealthIssue issue : report.issues()) {
            if (issue.severity() == HealthIssue.Severity.CRITICAL) {
                score -= 30;
            } else if (issue.severity() == HealthIssue.Severity.WARNING) {
                score -= 10;
            }
        }
        return Math.max(0, score);
    }

    private static Set<String> issueSignature(HealthReport report) {
        if (report == null || report.issues() == null) {
            return Set.of();
        }
        return report.issues().stream()
                .map(issue -> issue.type().name() + ":" + issue.severity().name())
                .collect(Collectors.toSet());
    }
}
