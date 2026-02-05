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

package com.floe.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
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
import com.floe.server.scheduler.SchedulerConfig;
import com.floe.server.scheduler.SchedulerService;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag("integration")
class AutoModeIntegrationTest {

    @Test
    void autoModeRespectsMaxTablesPerPoll() {
        CatalogClient catalogClient = Mockito.mock(CatalogClient.class);
        MaintenanceOrchestrator orchestrator = Mockito.mock(MaintenanceOrchestrator.class);
        OperationStore operationStore = Mockito.mock(OperationStore.class);
        TableHealthStore healthStore = Mockito.mock(TableHealthStore.class);
        com.floe.core.policy.PolicyStore policyStore =
                Mockito.mock(com.floe.core.policy.PolicyStore.class);

        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables())
                .thenReturn(
                        List.of(
                                TableIdentifier.of("demo", "db", "t1"),
                                TableIdentifier.of("demo", "db", "t2")));
        when(healthStore.findHistory(any(), any(), any(), anyInt())).thenReturn(List.of());

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
        when(policyStore.listEnabled()).thenReturn(List.of(policy));

        when(orchestrator.runMaintenanceWithHealthAndStats(any(), any(), any(), any()))
                .thenReturn(
                        OrchestratorResult.noOperations(
                                "id",
                                TableIdentifier.of("demo", "db", "t1"),
                                "policy",
                                Instant.now()));

        SchedulerService scheduler =
                new SchedulerService(
                        policyStore,
                        catalogClient,
                        orchestrator,
                        new InMemoryScheduleExecutionStore(),
                        new DistributedLock.NoOpDistributedLock(),
                        operationStore,
                        healthStore,
                        new AutoModeConfig());

        scheduler.poll();

        verify(orchestrator, times(1)).runMaintenanceWithHealthAndStats(any(), any(), any(), any());
    }

    private static class AutoModeConfig implements SchedulerConfig {

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
            return 1;
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
