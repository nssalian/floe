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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.TableHealthStore;
import com.floe.core.operation.OperationRecord;
import com.floe.core.operation.OperationStatus;
import com.floe.core.operation.OperationStore;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
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
import org.mockito.Mockito;

class SchedulerThrottlingTest {

    @Test
    void throttleRepeatedFailuresBacksOff() {
        CatalogClient catalogClient = Mockito.mock(CatalogClient.class);
        MaintenanceOrchestrator orchestrator = Mockito.mock(MaintenanceOrchestrator.class);
        OperationStore operationStore = Mockito.mock(OperationStore.class);
        TableHealthStore healthStore = Mockito.mock(TableHealthStore.class);
        com.floe.core.policy.PolicyStore policyStore =
                Mockito.mock(com.floe.core.policy.PolicyStore.class);

        TableIdentifier table = TableIdentifier.of("demo", "db", "table");
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables()).thenReturn(List.of(table));
        when(healthStore.findHistory(any(), any(), any(), anyInt())).thenReturn(List.of());

        OperationRecord failed =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("db")
                        .tableName("table")
                        .status(OperationStatus.FAILED)
                        .startedAt(Instant.now())
                        .build();
        when(operationStore.findByTable(any(), any(), any(), anyInt())).thenReturn(List.of(failed));

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

        SchedulerService scheduler =
                new SchedulerService(
                        policyStore,
                        catalogClient,
                        orchestrator,
                        new InMemoryScheduleExecutionStore(),
                        new DistributedLock.NoOpDistributedLock(),
                        operationStore,
                        healthStore,
                        new TestSchedulerConfig());

        scheduler.poll();

        verify(orchestrator, never()).runMaintenanceWithHealthAndStats(any(), any(), any(), any());
    }

    @Test
    void reduceFrequencyForZeroChangeRuns() {
        CatalogClient catalogClient = Mockito.mock(CatalogClient.class);
        MaintenanceOrchestrator orchestrator = Mockito.mock(MaintenanceOrchestrator.class);
        OperationStore operationStore = Mockito.mock(OperationStore.class);
        TableHealthStore healthStore = Mockito.mock(TableHealthStore.class);
        com.floe.core.policy.PolicyStore policyStore =
                Mockito.mock(com.floe.core.policy.PolicyStore.class);

        TableIdentifier table = TableIdentifier.of("demo", "db", "table");
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables()).thenReturn(List.of(table));
        when(healthStore.findHistory(any(), any(), any(), anyInt())).thenReturn(List.of());

        OperationRecord zeroChange =
                OperationRecord.builder()
                        .catalog("demo")
                        .namespace("db")
                        .tableName("table")
                        .status(OperationStatus.SUCCESS)
                        .startedAt(Instant.now())
                        .normalizedMetrics(java.util.Map.of())
                        .build();
        when(operationStore.findByTable(any(), any(), any(), anyInt()))
                .thenReturn(List.of(zeroChange));

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

        InMemoryScheduleExecutionStore executionStore = new InMemoryScheduleExecutionStore();
        SchedulerService scheduler =
                new SchedulerService(
                        policyStore,
                        catalogClient,
                        orchestrator,
                        executionStore,
                        new DistributedLock.NoOpDistributedLock(),
                        operationStore,
                        healthStore,
                        new ZeroChangeConfig());

        scheduler.poll();

        verify(orchestrator, never()).runMaintenanceWithHealthAndStats(any(), any(), any(), any());
        assertTrue(
                executionStore
                        .getRecord(policy.id(), "REWRITE_DATA_FILES", "demo.db.table")
                        .isPresent());
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
            return 1;
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

    private static class ZeroChangeConfig extends TestSchedulerConfig {

        @Override
        public int zeroChangeThreshold() {
            return 1;
        }
    }
}
