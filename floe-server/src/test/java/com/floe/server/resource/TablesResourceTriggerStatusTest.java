package com.floe.server.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthThresholds;
import com.floe.core.health.TableHealthStore;
import com.floe.core.operation.OperationStore;
import com.floe.core.policy.ExpireSnapshotsConfig;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OperationType;
import com.floe.core.policy.OrphanCleanupConfig;
import com.floe.core.policy.PolicyMatcher;
import com.floe.core.policy.RewriteDataFilesConfig;
import com.floe.core.policy.RewriteManifestsConfig;
import com.floe.core.policy.ScheduleConfig;
import com.floe.core.policy.TriggerConditions;
import com.floe.server.api.ErrorResponse;
import com.floe.server.api.TriggerStatusResponse;
import com.floe.server.config.HealthConfig;
import com.floe.server.health.HealthReportCache;
import com.floe.server.scheduler.SchedulerConfig;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Tests for the trigger-status API endpoint in TablesResource. */
@ExtendWith(MockitoExtension.class)
class TablesResourceTriggerStatusTest {

    @Mock CatalogClient catalogClient;

    @Mock HealthConfig healthConfig;

    @Mock TableHealthStore healthStore;

    @Mock PolicyMatcher policyMatcher;

    @Mock HealthReportCache healthReportCache;

    @Mock OperationStore operationStore;

    @Mock SchedulerConfig schedulerConfig;

    @InjectMocks TablesResource resource;

    private Table mockTable;

    @BeforeEach
    void setUp() {
        lenient().when(catalogClient.getCatalogName()).thenReturn("demo");
        lenient().when(healthConfig.scanMode()).thenReturn("scan");
        lenient().when(healthConfig.sampleLimit()).thenReturn(1000);
        lenient().when(healthReportCache.getIfFresh(any())).thenReturn(Optional.empty());
        lenient()
                .when(healthStore.findHistory(anyString(), anyString(), anyString(), anyInt()))
                .thenReturn(List.of());

        mockTable = mock(Table.class, withSettings().lenient());
        when(mockTable.currentSnapshot()).thenReturn(null);
        when(mockTable.snapshots()).thenReturn(List.of());
    }

    @Test
    void shouldReturn404WhenTableNotFound() {
        when(catalogClient.loadTable(any(TableIdentifier.class))).thenReturn(Optional.empty());

        Response response = resource.getTriggerStatus("test", "nonexistent");

        assertThat(response.getStatus()).isEqualTo(404);
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertThat(body.error()).isEqualTo("Table not found: test.nonexistent");
    }

    @Test
    void shouldReturnNoPolicyStatusWhenNoMatchingPolicy() {
        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mockTable));
        when(policyMatcher.findEffectivePolicy(anyString(), any(TableIdentifier.class)))
                .thenReturn(Optional.empty());
        when(schedulerConfig.conditionBasedTriggeringEnabled()).thenReturn(true);

        Response response = resource.getTriggerStatus("test", "events");

        assertThat(response.getStatus()).isEqualTo(200);
        TriggerStatusResponse body = (TriggerStatusResponse) response.getEntity();

        assertThat(body.catalog()).isEqualTo("demo");
        assertThat(body.namespace()).isEqualTo("test");
        assertThat(body.table()).isEqualTo("events");
        assertThat(body.policyName()).isNull();
        assertThat(body.conditionBasedTriggeringEnabled()).isTrue();

        for (OperationType opType : OperationType.values()) {
            TriggerStatusResponse.OperationTriggerStatus status =
                    body.operationStatuses().get(opType.name());
            assertThat(status).isNotNull();
            assertThat(status.shouldTrigger()).isFalse();
            assertThat(status.enabled()).isFalse();
            assertThat(status.unmetConditions()).contains("No matching policy");
        }
    }

    @Test
    void shouldReturnAlwaysTriggerWhenConditionBasedTriggeringDisabled() {
        MaintenancePolicy policy = createPolicyWithAllOperationsEnabled(null);

        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mockTable));
        when(policyMatcher.findEffectivePolicy(anyString(), any(TableIdentifier.class)))
                .thenReturn(Optional.of(policy));
        when(schedulerConfig.conditionBasedTriggeringEnabled()).thenReturn(false);
        when(operationStore.findLastOperationTime(
                        anyString(), anyString(), anyString(), anyString()))
                .thenReturn(Optional.empty());

        Response response = resource.getTriggerStatus("test", "events");

        assertThat(response.getStatus()).isEqualTo(200);
        TriggerStatusResponse body = (TriggerStatusResponse) response.getEntity();

        assertThat(body.policyName()).isEqualTo("test-policy");
        assertThat(body.conditionBasedTriggeringEnabled()).isFalse();

        for (OperationType opType : OperationType.values()) {
            TriggerStatusResponse.OperationTriggerStatus status =
                    body.operationStatuses().get(opType.name());
            assertThat(status.shouldTrigger()).isTrue();
            assertThat(status.enabled()).isTrue();
        }
    }

    @Test
    void shouldReturnDisabledStatusForDisabledOperations() {
        ScheduleConfig enabledSchedule =
                ScheduleConfig.builder().enabled(true).cronExpression("0 0 * * *").build();

        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .name("rewrite-only")
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(enabledSchedule)
                        .build();

        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mockTable));
        when(policyMatcher.findEffectivePolicy(anyString(), any(TableIdentifier.class)))
                .thenReturn(Optional.of(policy));
        when(schedulerConfig.conditionBasedTriggeringEnabled()).thenReturn(false);
        when(operationStore.findLastOperationTime(
                        anyString(), anyString(), anyString(), anyString()))
                .thenReturn(Optional.empty());

        Response response = resource.getTriggerStatus("test", "events");

        assertThat(response.getStatus()).isEqualTo(200);
        TriggerStatusResponse body = (TriggerStatusResponse) response.getEntity();

        TriggerStatusResponse.OperationTriggerStatus rewriteStatus =
                body.operationStatuses().get(OperationType.REWRITE_DATA_FILES.name());
        assertThat(rewriteStatus.enabled()).isTrue();
        assertThat(rewriteStatus.shouldTrigger()).isTrue();

        TriggerStatusResponse.OperationTriggerStatus expireStatus =
                body.operationStatuses().get(OperationType.EXPIRE_SNAPSHOTS.name());
        assertThat(expireStatus.enabled()).isFalse();
        assertThat(expireStatus.shouldTrigger()).isFalse();
        assertThat(expireStatus.unmetConditions()).contains("Operation disabled in policy");
    }

    @Test
    void shouldEvaluateTriggerConditions() {
        TriggerConditions conditions =
                TriggerConditions.builder()
                        .smallFilePercentageAbove(10.0)
                        .minIntervalMinutes(60)
                        .build();

        MaintenancePolicy policy = createPolicyWithAllOperationsEnabled(conditions);

        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mockTable));
        when(policyMatcher.findEffectivePolicy(anyString(), any(TableIdentifier.class)))
                .thenReturn(Optional.of(policy));
        when(schedulerConfig.conditionBasedTriggeringEnabled()).thenReturn(true);
        when(operationStore.findLastOperationTime(
                        anyString(), anyString(), anyString(), anyString()))
                .thenReturn(Optional.empty());

        Response response = resource.getTriggerStatus("test", "events");

        assertThat(response.getStatus()).isEqualTo(200);
        TriggerStatusResponse body = (TriggerStatusResponse) response.getEntity();
        assertThat(body.conditionBasedTriggeringEnabled()).isTrue();
    }

    @Test
    void shouldShowBlockedByMinInterval() {
        TriggerConditions conditions = TriggerConditions.builder().minIntervalMinutes(60).build();

        MaintenancePolicy policy = createPolicyWithAllOperationsEnabled(conditions);

        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mockTable));
        when(policyMatcher.findEffectivePolicy(anyString(), any(TableIdentifier.class)))
                .thenReturn(Optional.of(policy));
        when(schedulerConfig.conditionBasedTriggeringEnabled()).thenReturn(true);

        Instant thirtyMinutesAgo = Instant.now().minusSeconds(30 * 60);
        when(operationStore.findLastOperationTime(
                        anyString(), anyString(), anyString(), anyString()))
                .thenReturn(Optional.of(thirtyMinutesAgo));

        Response response = resource.getTriggerStatus("test", "events");

        assertThat(response.getStatus()).isEqualTo(200);
        TriggerStatusResponse body = (TriggerStatusResponse) response.getEntity();

        for (OperationType opType : OperationType.values()) {
            TriggerStatusResponse.OperationTriggerStatus status =
                    body.operationStatuses().get(opType.name());
            assertThat(status.shouldTrigger()).isFalse();
            assertThat(status.nextEligibleTime()).isNotNull();
            assertThat(status.unmetConditions().stream().anyMatch(c -> c.contains("Min interval")))
                    .isTrue();
        }
    }

    @Test
    void shouldIncludePolicyNameInResponse() {
        ScheduleConfig enabledSchedule =
                ScheduleConfig.builder().enabled(true).cronExpression("0 0 * * *").build();

        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .name("my-custom-policy")
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(enabledSchedule)
                        .build();

        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mockTable));
        when(policyMatcher.findEffectivePolicy(anyString(), any(TableIdentifier.class)))
                .thenReturn(Optional.of(policy));
        when(schedulerConfig.conditionBasedTriggeringEnabled()).thenReturn(true);
        when(operationStore.findLastOperationTime(
                        anyString(), anyString(), anyString(), anyString()))
                .thenReturn(Optional.empty());

        Response response = resource.getTriggerStatus("test", "events");

        assertThat(response.getStatus()).isEqualTo(200);
        TriggerStatusResponse body = (TriggerStatusResponse) response.getEntity();
        assertThat(body.policyName()).isEqualTo("my-custom-policy");
    }

    private MaintenancePolicy createPolicyWithAllOperationsEnabled(TriggerConditions conditions) {
        ScheduleConfig enabledSchedule =
                ScheduleConfig.builder().enabled(true).cronExpression("0 0 * * *").build();

        return MaintenancePolicy.builder()
                .name("test-policy")
                .triggerConditions(conditions)
                .healthThresholds(HealthThresholds.defaults())
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .rewriteDataFilesSchedule(enabledSchedule)
                .expireSnapshots(ExpireSnapshotsConfig.defaults())
                .expireSnapshotsSchedule(enabledSchedule)
                .rewriteManifests(RewriteManifestsConfig.defaults())
                .rewriteManifestsSchedule(enabledSchedule)
                .orphanCleanup(OrphanCleanupConfig.defaults())
                .orphanCleanupSchedule(enabledSchedule)
                .build();
    }
}
