package com.floe.core.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.ExecutionResult;
import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.operation.InMemoryOperationStore;
import com.floe.core.operation.OperationStore;
import com.floe.core.policy.ExpireSnapshotsConfig;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OrphanCleanupConfig;
import com.floe.core.policy.PolicyMatcher;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.RewriteDataFilesConfig;
import com.floe.core.policy.RewriteManifestsConfig;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MaintenanceOrchestratorPlannerTest {

    @Mock private com.floe.core.engine.ExecutionEngine engine;

    @Mock private MaintenancePlanner planner;

    private PolicyStore policyStore;
    private MaintenanceOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        policyStore = new com.floe.core.policy.InMemoryPolicyStore();
        PolicyMatcher matcher = new PolicyMatcher(policyStore);
        OperationStore operationStore = new InMemoryOperationStore();
        orchestrator =
                new MaintenanceOrchestrator(policyStore, matcher, engine, operationStore, planner);

        when(engine.getEngineType()).thenReturn(com.floe.core.engine.EngineType.SPARK);
        when(engine.execute(any(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            TableIdentifier table = invocation.getArgument(0);
                            MaintenanceOperation op = invocation.getArgument(1);
                            ExecutionResult result =
                                    new ExecutionResult(
                                            "exec",
                                            table,
                                            op.getType(),
                                            ExecutionStatus.SUCCEEDED,
                                            Instant.now(),
                                            Instant.now(),
                                            Map.of(),
                                            java.util.Optional.empty(),
                                            java.util.Optional.empty());
                            return CompletableFuture.completedFuture(result);
                        });

        policyStore.save(fullPolicy());
    }

    @Test
    void runMaintenanceUsesPlannerOutput() {
        TableIdentifier table = TableIdentifier.of("demo", "db", "table");
        when(planner.plan(any(), any(), any()))
                .thenReturn(
                        List.of(
                                PlannedOperation.critical(
                                        MaintenanceOperation.Type.REWRITE_DATA_FILES,
                                        "small files")));

        orchestrator.runMaintenanceWithHealth("demo", table, sampleHealthReport(table));

        ArgumentCaptor<MaintenanceOperation> opCaptor =
                ArgumentCaptor.forClass(MaintenanceOperation.class);
        verify(engine, times(1)).execute(any(), opCaptor.capture(), any());
        assertThat(opCaptor.getValue().getType())
                .isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES);
    }

    @Test
    void runMaintenanceSkipsUnneededOps() {
        TableIdentifier table = TableIdentifier.of("demo", "db", "table");
        when(planner.plan(any(), any(), any()))
                .thenReturn(
                        List.of(
                                PlannedOperation.critical(
                                        MaintenanceOperation.Type.REWRITE_MANIFESTS, "manifests")));

        orchestrator.runMaintenanceWithHealth("demo", table, sampleHealthReport(table));

        ArgumentCaptor<MaintenanceOperation> opCaptor =
                ArgumentCaptor.forClass(MaintenanceOperation.class);
        verify(engine, times(1)).execute(any(), opCaptor.capture(), any());
        assertThat(opCaptor.getValue().getType())
                .isEqualTo(MaintenanceOperation.Type.REWRITE_MANIFESTS);
    }

    @Test
    void runMaintenanceAttachesReason() {
        TableIdentifier table = TableIdentifier.of("demo", "db", "table");
        when(planner.plan(any(), any(), any()))
                .thenReturn(
                        List.of(
                                PlannedOperation.warning(
                                        MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                                        "old snapshots")));

        OrchestratorResult result =
                orchestrator.runMaintenanceWithHealth("demo", table, sampleHealthReport(table));

        assertThat(result.operationResults())
                .singleElement()
                .satisfies(
                        op ->
                                assertThat(op.metrics().get("plannerReason"))
                                        .isEqualTo("old snapshots"));
    }

    @Test
    void runMaintenanceFallbackWithoutHealth() {
        TableIdentifier table = TableIdentifier.of("demo", "db", "table");

        orchestrator.runMaintenance("demo", table);

        verify(engine, times(4)).execute(any(), any(), any());
    }

    private MaintenancePolicy fullPolicy() {
        return MaintenancePolicy.builder()
                .name("policy")
                .tablePattern(com.floe.core.policy.TablePattern.parse("demo.db.*"))
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .expireSnapshots(ExpireSnapshotsConfig.defaults())
                .orphanCleanup(OrphanCleanupConfig.defaults())
                .rewriteManifests(RewriteManifestsConfig.defaults())
                .build();
    }

    private com.floe.core.health.HealthReport sampleHealthReport(TableIdentifier table) {
        return com.floe.core.health.HealthReport.builder(table).build();
    }
}
