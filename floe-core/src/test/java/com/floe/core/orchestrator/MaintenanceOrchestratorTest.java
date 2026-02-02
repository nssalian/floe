package com.floe.core.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.ExecutionContext;
import com.floe.core.engine.ExecutionEngine;
import com.floe.core.engine.ExecutionResult;
import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.maintenance.RewriteDataFilesOperation;
import com.floe.core.operation.InMemoryOperationStore;
import com.floe.core.operation.OperationRecord;
import com.floe.core.operation.OperationStatus;
import com.floe.core.operation.OperationStore;
import com.floe.core.policy.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MaintenanceOrchestratorTest {

    @Mock private ExecutionEngine engine;

    private PolicyStore policyStore;
    private OperationStore operationStore;
    private MaintenanceOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        policyStore = new InMemoryPolicyStore();
        PolicyMatcher policyMatcher = new PolicyMatcher(policyStore);
        operationStore = new InMemoryOperationStore();
        orchestrator =
                new MaintenanceOrchestrator(
                        policyStore,
                        policyMatcher,
                        engine,
                        operationStore,
                        new MaintenancePlanner());
        when(engine.getEngineType()).thenReturn(com.floe.core.engine.EngineType.SPARK);
    }

    @Test
    void shouldReturnNoPolicyWhenNoMatchingPolicy() {
        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");

        OrchestratorResult result = orchestrator.runMaintenance(table.catalog(), table);

        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.NO_POLICY);
        assertThat(result.policyName()).isNull();
        assertThat(result.operationResults()).isEmpty();
        verify(engine, never()).execute(any(), any(), any());

        // Verify operation was persisted
        List<OperationRecord> records = operationStore.findRecent(10);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).status()).isEqualTo(OperationStatus.NO_POLICY);
    }

    @Test
    void shouldReturnNoOperationsWhenPolicyHasNoneEnabled() {
        // Policy with no operations enabled (configs are null)
        MaintenancePolicy policy =
                createPolicy(
                        "test-policy",
                        "catalog.db.*",
                        10,
                        null,
                        null, // no compaction
                        null,
                        null, // no expire
                        null,
                        null, // no orphan
                        null // no manifests
                        );
        policyStore.save(policy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");

        OrchestratorResult result = orchestrator.runMaintenance(table.catalog(), table);

        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.NO_OPERATIONS);
        assertThat(result.policyName()).isEqualTo("test-policy");
        verify(engine, never()).execute(any(), any(), any());

        // Verify operation was persisted
        List<OperationRecord> records = operationStore.findRecent(10);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).status()).isEqualTo(OperationStatus.NO_OPERATIONS);
    }

    @Test
    void shouldExecuteCompactionWhenEnabled() {
        MaintenancePolicy policy =
                createPolicy(
                        "compaction-policy",
                        "catalog.db.*",
                        10,
                        createRewriteDataFilesConfig(),
                        enabledSchedule(),
                        null,
                        null,
                        null,
                        null,
                        null);
        policyStore.save(policy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");
        ExecutionResult execResult =
                createSuccessResult(table, MaintenanceOperation.Type.REWRITE_DATA_FILES);
        when(engine.execute(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(execResult));

        OrchestratorResult result = orchestrator.runMaintenance(table.catalog(), table);

        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.SUCCESS);
        assertThat(result.policyName()).isEqualTo("compaction-policy");
        assertThat(result.operationResults()).hasSize(1);
        assertThat(result.operationResults().get(0).operationType())
                .isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES);
        assertThat(result.operationResults().get(0).isSuccess()).isTrue();

        verify(engine, times(1))
                .execute(
                        eq(table),
                        any(RewriteDataFilesOperation.class),
                        any(ExecutionContext.class));

        // Verify operation was persisted with SUCCESS status
        List<OperationRecord> records = operationStore.findRecent(10);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).status()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(records.get(0).results()).isNotNull();
        assertThat(records.get(0).results().successCount()).isEqualTo(1);
    }

    @Test
    void shouldExecuteMultipleOperationsSequentially() {
        MaintenancePolicy policy =
                createPolicy(
                        "full-policy",
                        "catalog.db.*",
                        10,
                        createRewriteDataFilesConfig(),
                        enabledSchedule(),
                        createExpireSnapshotsConfig(),
                        enabledSchedule(),
                        null,
                        null,
                        null);
        policyStore.save(policy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");
        ExecutionResult execResult =
                createSuccessResult(table, MaintenanceOperation.Type.REWRITE_DATA_FILES);
        when(engine.execute(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(execResult));

        OrchestratorResult result = orchestrator.runMaintenance(table.catalog(), table);

        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.SUCCESS);
        assertThat(result.operationResults()).hasSize(2);
        assertThat(result.successCount()).isEqualTo(2);

        verify(engine, times(2)).execute(any(), any(), any());

        // Verify operation was persisted
        List<OperationRecord> records = operationStore.findRecent(10);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).status()).isEqualTo(OperationStatus.SUCCESS);
    }

    @Test
    void shouldSkipRemainingOperationsOnFailure() {
        MaintenancePolicy policy =
                createPolicy(
                        "multi-policy",
                        "catalog.db.*",
                        10,
                        createRewriteDataFilesConfig(),
                        enabledSchedule(),
                        createExpireSnapshotsConfig(),
                        enabledSchedule(),
                        createOrphanCleanupConfig(),
                        enabledSchedule(),
                        null);
        policyStore.save(policy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");
        ExecutionResult failedResult =
                createFailedResult(
                        table, MaintenanceOperation.Type.REWRITE_DATA_FILES, "Compaction failed");
        when(engine.execute(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(failedResult));

        OrchestratorResult result = orchestrator.runMaintenance(table.catalog(), table);

        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.FAILED);
        assertThat(result.operationResults()).hasSize(3);
        assertThat(result.failedCount()).isEqualTo(1);
        assertThat(result.skippedCount()).isEqualTo(2);

        // Only first operation should have been executed
        verify(engine, times(1)).execute(any(), any(), any());

        // Verify operation was persisted with FAILED status
        List<OperationRecord> records = operationStore.findRecent(10);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).status()).isEqualTo(OperationStatus.FAILED);
    }

    @Test
    void shouldFilterOperationsByType() {
        // This test verifies that only enabled operations run
        MaintenancePolicy policy =
                createPolicy(
                        "single-op-policy",
                        "catalog.db.*",
                        10,
                        createRewriteDataFilesConfig(),
                        enabledSchedule(),
                        null,
                        null, // expire NOT enabled
                        null,
                        null, // orphan NOT enabled
                        null);
        policyStore.save(policy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");
        ExecutionResult execResult =
                createSuccessResult(table, MaintenanceOperation.Type.REWRITE_DATA_FILES);
        when(engine.execute(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(execResult));

        OrchestratorResult result = orchestrator.runMaintenance(table.catalog(), table);

        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.SUCCESS);
        assertThat(result.operationResults()).hasSize(1);
        assertThat(result.operationResults().get(0).operationType())
                .isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES);

        verify(engine, times(1)).execute(any(), any(), any());
    }

    @Test
    void shouldSelectHigherPriorityPolicy() {
        MaintenancePolicy generalPolicy =
                createPolicy(
                        "general",
                        "catalog.*.*",
                        10,
                        createRewriteDataFilesConfig(),
                        enabledSchedule(),
                        null,
                        null,
                        null,
                        null,
                        null);
        MaintenancePolicy specificPolicy =
                createPolicy(
                        "specific",
                        "catalog.db.*",
                        100,
                        null,
                        null,
                        createExpireSnapshotsConfig(),
                        enabledSchedule(),
                        null,
                        null,
                        null);

        policyStore.save(generalPolicy);
        policyStore.save(specificPolicy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");
        ExecutionResult execResult =
                createSuccessResult(table, MaintenanceOperation.Type.EXPIRE_SNAPSHOTS);
        when(engine.execute(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(execResult));

        OrchestratorResult result = orchestrator.runMaintenance(table.catalog(), table);

        // Should use specific policy (expire snapshots, not compaction)
        assertThat(result.policyName()).isEqualTo("specific");
        assertThat(result.operationResults()).hasSize(1);
        assertThat(result.operationResults().get(0).operationType())
                .isEqualTo(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS);
    }

    @Test
    void shouldRunMaintenanceWithSpecificPolicy() {
        MaintenancePolicy policy =
                createPolicy(
                        "named-policy",
                        "catalog.db.*",
                        10,
                        createRewriteDataFilesConfig(),
                        enabledSchedule(),
                        null,
                        null,
                        null,
                        null,
                        null);
        policyStore.save(policy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");
        ExecutionResult execResult =
                createSuccessResult(table, MaintenanceOperation.Type.REWRITE_DATA_FILES);
        when(engine.execute(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(execResult));

        OrchestratorResult result =
                orchestrator.runMaintenanceWithPolicy(table.catalog(), table, "named-policy");

        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.SUCCESS);
        assertThat(result.policyName()).isEqualTo("named-policy");

        // Verify operation was persisted with policy name
        List<OperationRecord> records = operationStore.findRecent(10);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).policyName()).isEqualTo("named-policy");
    }

    @Test
    void shouldReturnNoPolicyWhenSpecificPolicyNotFound() {
        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");

        OrchestratorResult result =
                orchestrator.runMaintenanceWithPolicy(table.catalog(), table, "nonexistent");

        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.NO_POLICY);
        assertThat(result.message()).contains("not found");
        verify(engine, never()).execute(any(), any(), any());

        // Verify operation was persisted
        List<OperationRecord> records = operationStore.findRecent(10);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).status()).isEqualTo(OperationStatus.NO_POLICY);
    }

    @Test
    void shouldReturnNoPolicyWhenSpecificPolicyIsDisabled() {
        MaintenancePolicy disabledPolicy =
                new MaintenancePolicy(
                        UUID.randomUUID().toString(),
                        "disabled-policy",
                        "Disabled test policy",
                        TablePattern.parse("catalog.db.*"),
                        false, // disabled
                        createRewriteDataFilesConfig(),
                        enabledSchedule(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        10,
                        null, // healthThresholds
                        null, // triggerConditions
                        Map.of(),
                        Instant.now(),
                        Instant.now());
        policyStore.save(disabledPolicy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");

        OrchestratorResult result =
                orchestrator.runMaintenanceWithPolicy(table.catalog(), table, "disabled-policy");

        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.NO_POLICY);
        assertThat(result.message()).contains("disabled");
        verify(engine, never()).execute(any(), any(), any());
    }

    @Test
    void shouldPersistOperationOnFailedExecution() {
        MaintenancePolicy policy =
                createPolicy(
                        "error-policy",
                        "catalog.db.*",
                        10,
                        createRewriteDataFilesConfig(),
                        enabledSchedule(),
                        null,
                        null,
                        null,
                        null,
                        null);
        policyStore.save(policy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "table");

        // Mock engine to throw exception - orchestrator catches it and returns failed result
        when(engine.execute(any(), any(), any()))
                .thenThrow(new RuntimeException("Engine exploded"));

        OrchestratorResult result = orchestrator.runMaintenance(table.catalog(), table);

        // Orchestrator catches exception and returns FAILED status
        assertThat(result.status()).isEqualTo(OrchestratorResult.Status.FAILED);
        assertThat(result.operationResults()).hasSize(1);
        assertThat(result.operationResults().get(0).errorMessage()).contains("Engine exploded");

        // Verify operation was persisted with FAILED status
        List<OperationRecord> records = operationStore.findRecent(10);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).status()).isEqualTo(OperationStatus.FAILED);
    }

    @Test
    void shouldTrackOperationsForTable() {
        MaintenancePolicy policy =
                createPolicy(
                        "track-policy",
                        "catalog.db.*",
                        10,
                        createRewriteDataFilesConfig(),
                        enabledSchedule(),
                        null,
                        null,
                        null,
                        null,
                        null);
        policyStore.save(policy);

        TableIdentifier table = TableIdentifier.of("catalog", "db", "events");
        ExecutionResult execResult =
                createSuccessResult(table, MaintenanceOperation.Type.REWRITE_DATA_FILES);
        when(engine.execute(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(execResult));

        // Run maintenance 3 times
        orchestrator.runMaintenance(table.catalog(), table);
        orchestrator.runMaintenance(table.catalog(), table);
        orchestrator.runMaintenance(table.catalog(), table);

        // Query by table
        List<OperationRecord> records = operationStore.findByTable("catalog", "db", "events", 10);
        assertThat(records).hasSize(3);
        assertThat(records).allMatch(r -> r.tableName().equals("events"));
    }

    // Helper methods

    private MaintenancePolicy createPolicy(
            String name,
            String pattern,
            int priority,
            RewriteDataFilesConfig rewriteConfig,
            ScheduleConfig rewriteSchedule,
            ExpireSnapshotsConfig expireConfig,
            ScheduleConfig expireSchedule,
            OrphanCleanupConfig orphanConfig,
            ScheduleConfig orphanSchedule,
            ScheduleConfig manifestSchedule) {
        return new MaintenancePolicy(
                UUID.randomUUID().toString(),
                name,
                "Test policy",
                TablePattern.parse(pattern),
                true,
                rewriteConfig,
                rewriteSchedule,
                expireConfig,
                expireSchedule,
                orphanConfig,
                orphanSchedule,
                null, // rewriteManifests config
                manifestSchedule,
                priority,
                null, // healthThresholds
                null, // triggerConditions
                Map.of(),
                Instant.now(),
                Instant.now());
    }

    private ScheduleConfig enabledSchedule() {
        return new ScheduleConfig(
                Duration.ofHours(6), // interval
                null, // cronExpression
                null, // windowStart
                null, // windowEnd
                null, // allowedDays
                Duration.ofHours(1), // timeout
                0, // priority
                true // enabled
                );
    }

    private RewriteDataFilesConfig createRewriteDataFilesConfig() {
        return RewriteDataFilesConfig.builder()
                .strategy("BINPACK")
                .targetFileSizeBytes(536870912L)
                .partialProgressEnabled(true)
                .build();
    }

    private ExpireSnapshotsConfig createExpireSnapshotsConfig() {
        return ExpireSnapshotsConfig.builder()
                .retainLast(5)
                .maxSnapshotAge(Duration.ofDays(7))
                .build();
    }

    private OrphanCleanupConfig createOrphanCleanupConfig() {
        return OrphanCleanupConfig.builder()
                .retentionPeriodInDays(Duration.ofDays(3))
                .prefixMismatchMode("ERROR")
                .build();
    }

    private ExecutionResult createSuccessResult(
            TableIdentifier table, MaintenanceOperation.Type opType) {
        Instant start = Instant.now();
        return new ExecutionResult(
                UUID.randomUUID().toString(),
                table,
                opType,
                ExecutionStatus.SUCCEEDED,
                start,
                start.plusSeconds(10),
                Map.of("rewrittenDataFilesCount", 10),
                Optional.empty(),
                Optional.empty());
    }

    private ExecutionResult createFailedResult(
            TableIdentifier table, MaintenanceOperation.Type opType, String error) {
        Instant start = Instant.now();
        return new ExecutionResult(
                UUID.randomUUID().toString(),
                table,
                opType,
                ExecutionStatus.FAILED,
                start,
                start.plusSeconds(5),
                Map.of(),
                Optional.of(error),
                Optional.empty());
    }
}
