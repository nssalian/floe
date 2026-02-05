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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthIssue;
import com.floe.core.health.HealthReport;
import com.floe.core.health.TableHealthStore;
import com.floe.core.operation.OperationStore;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.RewriteDataFilesConfig;
import com.floe.core.policy.ScheduleConfig;
import com.floe.core.policy.TablePattern;
import com.floe.core.scheduler.DistributedLock;
import com.floe.core.scheduler.InMemoryScheduleExecutionStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

class SchedulerPrioritizationTest {

    @Test
    void prioritizeByDebtScore() {
        CatalogClient catalogClient = Mockito.mock(CatalogClient.class);
        MaintenanceOrchestrator orchestrator = Mockito.mock(MaintenanceOrchestrator.class);
        OperationStore operationStore = Mockito.mock(OperationStore.class);
        TableHealthStore healthStore = Mockito.mock(TableHealthStore.class);
        com.floe.core.policy.PolicyStore policyStore =
                Mockito.mock(com.floe.core.policy.PolicyStore.class);

        TableIdentifier high = TableIdentifier.of("demo", "db", "high");
        TableIdentifier low = TableIdentifier.of("demo", "db", "low");

        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables()).thenReturn(List.of(low, high));
        when(operationStore.findByTable(any(), any(), any(), anyInt())).thenReturn(List.of());

        HealthReport highReport =
                HealthReport.builder(high)
                        .issues(
                                List.of(
                                        HealthIssue.critical(
                                                HealthIssue.Type.TOO_MANY_SNAPSHOTS, "bad")))
                        .build();
        HealthReport lowReport = HealthReport.builder(low).build();

        when(healthStore.findHistory("demo", "db", "high", 1)).thenReturn(List.of(highReport));
        when(healthStore.findHistory("demo", "db", "low", 1)).thenReturn(List.of(lowReport));

        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .name("policy")
                        .tablePattern(TablePattern.matchAll())
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(
                                ScheduleConfig.builder()
                                        .interval(Duration.ofDays(1))
                                        .enabled(true)
                                        .build())
                        .build();

        SchedulerConfig config = new TestSchedulerConfig();

        when(orchestrator.runMaintenanceWithHealthAndStats(any(), any(), any(), any()))
                .thenReturn(
                        OrchestratorResult.noOperations("id", high, policy.name(), Instant.now()));
        when(policyStore.listEnabled()).thenReturn(List.of(policy));

        InOrder inOrder = inOrder(orchestrator);

        SchedulerService schedulerService =
                new SchedulerService(
                        policyStore,
                        catalogClient,
                        orchestrator,
                        new InMemoryScheduleExecutionStore(),
                        new DistributedLock.NoOpDistributedLock(),
                        operationStore,
                        healthStore,
                        config);

        schedulerService.poll();

        inOrder.verify(orchestrator)
                .runMaintenanceWithHealthAndStats(eq("demo"), eq(high), eq(highReport), any());
        inOrder.verify(orchestrator)
                .runMaintenanceWithHealthAndStats(eq("demo"), eq(low), eq(lowReport), any());
    }

    private static class TestSchedulerConfig implements SchedulerConfig {

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
            return 0;
        }

        @Override
        public int maxOperationsPerPoll() {
            return 0;
        }

        @Override
        public long maxBytesPerHour() {
            return 0;
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
    }
}
