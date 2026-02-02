package com.floe.core.orchestrator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.ExecutionEngine;
import com.floe.core.engine.ExecutionResult;
import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.metrics.OperationMetricsEmitter;
import com.floe.core.operation.InMemoryOperationStore;
import com.floe.core.policy.*;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MaintenanceOrchestratorMetricsTest {

    @Mock private ExecutionEngine engine;

    @Mock private OperationMetricsEmitter metricsEmitter;

    private MaintenanceOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        PolicyStore policyStore = new InMemoryPolicyStore();
        PolicyMatcher matcher = new PolicyMatcher(policyStore);

        policyStore.save(
                MaintenancePolicy.builder()
                        .name("policy")
                        .tablePattern(TablePattern.parse("demo.db.*"))
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .build());

        orchestrator =
                new MaintenanceOrchestrator(
                        policyStore,
                        matcher,
                        engine,
                        new InMemoryOperationStore(),
                        java.util.concurrent.Executors.newSingleThreadExecutor(),
                        new MaintenancePlanner(),
                        metricsEmitter);

        when(engine.getEngineType()).thenReturn(com.floe.core.engine.EngineType.SPARK);
        when(engine.execute(any(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            TableIdentifier table = invocation.getArgument(0);
                            MaintenanceOperation op = invocation.getArgument(1);
                            return CompletableFuture.completedFuture(
                                    new ExecutionResult(
                                            "exec",
                                            table,
                                            op.getType(),
                                            ExecutionStatus.SUCCEEDED,
                                            Instant.now(),
                                            Instant.now(),
                                            Map.of(),
                                            java.util.Optional.empty(),
                                            java.util.Optional.empty()));
                        });
    }

    @Test
    void recordOperationExecutionCalled() {
        TableIdentifier table = TableIdentifier.of("demo", "db", "table");

        orchestrator.runMaintenance("demo", table);

        verify(metricsEmitter, times(1)).recordOperationExecution(any(), any(), any());
    }
}
