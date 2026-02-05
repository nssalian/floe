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

package com.floe.server.scheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.ExecutionStatus;
import com.floe.core.health.TableHealthStore;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.operation.OperationStore;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
import com.floe.core.orchestrator.OperationResult;
import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.RewriteDataFilesConfig;
import com.floe.core.policy.ScheduleConfig;
import com.floe.core.policy.TablePattern;
import com.floe.core.scheduler.DistributedLock;
import com.floe.core.scheduler.ScheduleExecutionStore;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchedulerBudgetTest {

    @Mock private com.floe.core.policy.PolicyStore policyStore;

    @Mock private CatalogClient catalogClient;

    @Mock private MaintenanceOrchestrator orchestrator;

    @Mock private OperationStore operationStore;

    @Mock private TableHealthStore healthStore;

    @Mock private ScheduleExecutionStore executionStore;

    @Mock private DistributedLock distributedLock;

    private MaintenancePolicy policy;

    @BeforeEach
    void setUp() {
        policy =
                MaintenancePolicy.builder()
                        .name("policy")
                        .tablePattern(TablePattern.matchAll())
                        .enabled(true)
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(
                                ScheduleConfig.builder()
                                        .interval(Duration.ofDays(1))
                                        .enabled(true)
                                        .build())
                        .build();

        when(policyStore.listEnabled()).thenReturn(List.of(policy));
        when(executionStore.getRecord(any(), any(), any())).thenReturn(Optional.empty());
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(healthStore.findHistory(any(), any(), any(), anyInt())).thenReturn(List.of());
    }

    @Test
    void maxTablesPerPollEnforced() {
        when(catalogClient.listAllTables())
                .thenReturn(
                        List.of(
                                TableIdentifier.of("demo", "db", "t1"),
                                TableIdentifier.of("demo", "db", "t2")));
        when(orchestrator.runMaintenanceWithHealthAndStats(any(), any(), any(), any()))
                .thenReturn(emptyResult());

        SchedulerService scheduler = createService(1, 0, 0);
        scheduler.poll();

        verify(orchestrator, times(1)).runMaintenanceWithHealthAndStats(any(), any(), any(), any());
    }

    @Test
    void maxOperationsPerPollEnforced() {
        when(catalogClient.listAllTables())
                .thenReturn(
                        List.of(
                                TableIdentifier.of("demo", "db", "t1"),
                                TableIdentifier.of("demo", "db", "t2")));
        when(orchestrator.runMaintenanceWithHealthAndStats(any(), any(), any(), any()))
                .thenReturn(emptyResult());

        SchedulerService scheduler = createService(0, 1, 0);
        scheduler.poll();

        verify(orchestrator, times(1)).runMaintenanceWithHealthAndStats(any(), any(), any(), any());
    }

    @Test
    void maxBytesPerHourEnforced() {
        when(catalogClient.listAllTables())
                .thenReturn(
                        List.of(
                                TableIdentifier.of("demo", "db", "t1"),
                                TableIdentifier.of("demo", "db", "t2")));
        when(orchestrator.runMaintenanceWithHealthAndStats(any(), any(), any(), any()))
                .thenReturn(resultWithBytes(600));

        SchedulerService scheduler = createService(0, 0, 500);
        scheduler.poll();

        verify(orchestrator, times(1)).runMaintenanceWithHealthAndStats(any(), any(), any(), any());
    }

    @Test
    void budgetDefaultsToUnlimited() {
        when(catalogClient.listAllTables())
                .thenReturn(
                        List.of(
                                TableIdentifier.of("demo", "db", "t1"),
                                TableIdentifier.of("demo", "db", "t2")));
        when(orchestrator.runMaintenanceWithHealthAndStats(any(), any(), any(), any()))
                .thenReturn(emptyResult());

        SchedulerService scheduler = createService(0, 0, 0);
        scheduler.poll();

        verify(orchestrator, times(2)).runMaintenanceWithHealthAndStats(any(), any(), any(), any());
    }

    @Test
    void budgetResetsBetweenPolls() throws Exception {
        when(catalogClient.listAllTables())
                .thenReturn(List.of(TableIdentifier.of("demo", "db", "t1")));
        when(orchestrator.runMaintenanceWithHealthAndStats(any(), any(), any(), any()))
                .thenReturn(resultWithBytes(600));

        SchedulerService scheduler = createService(0, 0, 500);
        scheduler.poll();

        Field hourStartField = SchedulerService.class.getDeclaredField("currentHourStart");
        hourStartField.setAccessible(true);
        hourStartField.set(scheduler, Instant.now().minus(Duration.ofHours(2)));

        scheduler.poll();

        verify(orchestrator, times(2)).runMaintenanceWithHealthAndStats(any(), any(), any(), any());
    }

    private SchedulerService createService(int maxTables, int maxOps, long maxBytes) {
        SchedulerConfig config =
                new SchedulerConfig() {
                    @Override
                    public boolean enabled() {
                        return true;
                    }

                    @Override
                    public boolean distributedLockEnabled() {
                        return false;
                    }

                    @Override
                    public int maxTablesPerPoll() {
                        return maxTables;
                    }

                    @Override
                    public int maxOperationsPerPoll() {
                        return maxOps;
                    }

                    @Override
                    public long maxBytesPerHour() {
                        return maxBytes;
                    }

                    @Override
                    public int failureBackoffThreshold() {
                        return 3;
                    }

                    @Override
                    public int failureBackoffHours() {
                        return 6;
                    }

                    @Override
                    public int zeroChangeThreshold() {
                        return 5;
                    }

                    @Override
                    public int zeroChangeFrequencyReductionPercent() {
                        return 50;
                    }

                    @Override
                    public int zeroChangeMinIntervalHours() {
                        return 6;
                    }

                    @Override
                    public boolean conditionBasedTriggeringEnabled() {
                        return true;
                    }
                };

        return new SchedulerService(
                policyStore,
                catalogClient,
                orchestrator,
                executionStore,
                distributedLock,
                operationStore,
                healthStore,
                config);
    }

    private OrchestratorResult emptyResult() {
        TableIdentifier table = TableIdentifier.of("demo", "db", "t1");
        return OrchestratorResult.completed(
                "id",
                table,
                policy.getNameOrDefault(),
                Instant.now(),
                List.of(
                        new OperationResult(
                                MaintenanceOperation.Type.REWRITE_DATA_FILES,
                                ExecutionStatus.SUCCEEDED,
                                Instant.now(),
                                Instant.now(),
                                Map.of(),
                                null)));
    }

    private OrchestratorResult resultWithBytes(long bytes) {
        TableIdentifier table = TableIdentifier.of("demo", "db", "t1");
        return OrchestratorResult.completed(
                "id",
                table,
                policy.getNameOrDefault(),
                Instant.now(),
                List.of(
                        new OperationResult(
                                MaintenanceOperation.Type.REWRITE_DATA_FILES,
                                ExecutionStatus.SUCCEEDED,
                                Instant.now(),
                                Instant.now(),
                                Map.of("bytesRewritten", bytes),
                                null)));
    }
}
