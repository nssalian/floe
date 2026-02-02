package com.floe.core.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthReport;
import com.floe.core.health.HealthThresholds;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.policy.ExpireSnapshotsConfig;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OrphanCleanupConfig;
import com.floe.core.policy.RewriteDataFilesConfig;
import com.floe.core.policy.RewriteManifestsConfig;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MaintenancePlannerTest {

    private MaintenancePlanner planner;
    private MaintenancePolicy policy;

    @BeforeEach
    void setUp() {
        planner = new MaintenancePlanner();
        policy = fullPolicy();
    }

    @Test
    void planSmallFilesAboveThresholdReturnsRewriteDataFiles() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .smallFilePercentWarning(10.0)
                        .smallFilePercentCritical(20.0)
                        .build();
        HealthReport report = baseReportBuilder().dataFileCount(100).smallFileCount(30).build();

        List<PlannedOperation> planned = planner.plan(report, policy, thresholds);

        assertThat(planned)
                .singleElement()
                .satisfies(
                        op ->
                                assertThat(op.operationType())
                                        .isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES));
    }

    @Test
    void planSmallFilesBelowThresholdNoRewrite() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .smallFilePercentWarning(10.0)
                        .smallFilePercentCritical(20.0)
                        .build();
        HealthReport report = baseReportBuilder().dataFileCount(100).smallFileCount(5).build();

        List<PlannedOperation> planned = planner.plan(report, policy, thresholds);

        assertThat(planned).isEmpty();
    }

    @Test
    void planOldSnapshotsReturnsExpireSnapshots() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .snapshotAgeWarningDays(10)
                        .snapshotAgeCriticalDays(20)
                        .build();
        HealthReport report = baseReportBuilder().oldestSnapshotAgeDays(25L).build();

        List<PlannedOperation> planned = planner.plan(report, policy, thresholds);

        assertThat(planned)
                .singleElement()
                .satisfies(
                        op ->
                                assertThat(op.operationType())
                                        .isEqualTo(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS));
    }

    @Test
    void planRecentSnapshotsNoExpire() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .snapshotAgeWarningDays(10)
                        .snapshotAgeCriticalDays(20)
                        .build();
        HealthReport report = baseReportBuilder().oldestSnapshotAgeDays(2L).build();

        List<PlannedOperation> planned = planner.plan(report, policy, thresholds);

        assertThat(planned).isEmpty();
    }

    @Test
    void planHighDeleteRatioReturnsOrphanCleanup() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .deleteFileRatioWarning(0.10)
                        .deleteFileRatioCritical(0.20)
                        .build();
        HealthReport report = baseReportBuilder().dataFileCount(100).deleteFileCount(30).build();

        List<PlannedOperation> planned = planner.plan(report, policy, thresholds);

        assertThat(planned)
                .singleElement()
                .satisfies(
                        op ->
                                assertThat(op.operationType())
                                        .isEqualTo(MaintenanceOperation.Type.ORPHAN_CLEANUP));
    }

    @Test
    void planManyManifestsReturnsRewriteManifests() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .manifestCountWarning(5)
                        .manifestCountCritical(10)
                        .build();
        HealthReport report = baseReportBuilder().manifestCount(12).build();

        List<PlannedOperation> planned = planner.plan(report, policy, thresholds);

        assertThat(planned)
                .singleElement()
                .satisfies(
                        op ->
                                assertThat(op.operationType())
                                        .isEqualTo(MaintenanceOperation.Type.REWRITE_MANIFESTS));
    }

    @Test
    void planHealthyTableNoOperations() {
        HealthReport report = baseReportBuilder().build();

        List<PlannedOperation> planned = planner.plan(report, policy, HealthThresholds.defaults());

        assertThat(planned).isEmpty();
    }

    @Test
    void planMultipleIssuesReturnsMultipleOperations() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .smallFilePercentWarning(10.0)
                        .smallFilePercentCritical(20.0)
                        .snapshotCountWarning(5)
                        .snapshotCountCritical(10)
                        .deleteFileRatioWarning(0.10)
                        .deleteFileRatioCritical(0.20)
                        .manifestCountWarning(5)
                        .manifestCountCritical(10)
                        .build();
        HealthReport report =
                baseReportBuilder()
                        .dataFileCount(100)
                        .smallFileCount(30)
                        .snapshotCount(12)
                        .deleteFileCount(30)
                        .manifestCount(12)
                        .build();

        List<PlannedOperation> planned = planner.plan(report, policy, thresholds);

        assertThat(planned)
                .extracting(PlannedOperation::operationType)
                .containsExactlyInAnyOrder(
                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                        MaintenanceOperation.Type.ORPHAN_CLEANUP,
                        MaintenanceOperation.Type.REWRITE_MANIFESTS);
    }

    @Test
    void planReasonAttached() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .smallFilePercentWarning(10.0)
                        .smallFilePercentCritical(20.0)
                        .build();
        HealthReport report = baseReportBuilder().dataFileCount(100).smallFileCount(30).build();

        List<PlannedOperation> planned = planner.plan(report, policy, thresholds);

        assertThat(planned).singleElement().satisfies(op -> assertThat(op.reason()).isNotBlank());
    }

    @Test
    void planSeverityMatches() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .smallFilePercentWarning(10.0)
                        .smallFilePercentCritical(50.0)
                        .build();
        HealthReport report = baseReportBuilder().dataFileCount(100).smallFileCount(20).build();

        List<PlannedOperation> planned = planner.plan(report, policy, thresholds);

        assertThat(planned)
                .singleElement()
                .satisfies(op -> assertThat(op.severity().name()).isEqualTo("WARNING"));
    }

    @Test
    void planRespectsEnabledOperations() {
        MaintenancePolicy limitedPolicy =
                MaintenancePolicy.builder()
                        .name("limited")
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .build();
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .manifestCountWarning(5)
                        .manifestCountCritical(10)
                        .build();
        HealthReport report = baseReportBuilder().manifestCount(12).build();

        List<PlannedOperation> planned = planner.plan(report, limitedPolicy, thresholds);

        assertThat(planned).isEmpty();
    }

    @Test
    void planUsesCustomThresholds() {
        HealthReport report = baseReportBuilder().dataFileCount(100).smallFileCount(30).build();
        HealthThresholds customThresholds =
                HealthThresholds.builder()
                        .smallFilePercentWarning(40.0)
                        .smallFilePercentCritical(60.0)
                        .build();

        List<PlannedOperation> planned = planner.plan(report, policy, customThresholds);

        assertThat(planned).isEmpty();
    }

    private MaintenancePolicy fullPolicy() {
        return MaintenancePolicy.builder()
                .name("policy")
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .expireSnapshots(ExpireSnapshotsConfig.defaults())
                .orphanCleanup(OrphanCleanupConfig.defaults())
                .rewriteManifests(RewriteManifestsConfig.defaults())
                .build();
    }

    private HealthReport.Builder baseReportBuilder() {
        return HealthReport.builder(TableIdentifier.of("demo", "db", "table"))
                .dataFileCount(0)
                .smallFileCount(0)
                .largeFileCount(0)
                .snapshotCount(0)
                .deleteFileCount(0)
                .manifestCount(0)
                .totalManifestSizeBytes(0);
    }
}
