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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthReport;
import com.floe.core.health.TableHealthStore;
import com.floe.core.operation.OperationStore;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OperationType;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.RewriteDataFilesConfig;
import com.floe.core.policy.ScheduleConfig;
import com.floe.core.policy.TablePattern;
import com.floe.core.policy.TriggerConditions;
import com.floe.core.scheduler.DistributedLock;
import com.floe.core.scheduler.InMemoryScheduleExecutionStore;
import com.floe.core.scheduler.ScheduleExecutionStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for signal-based triggering (Phase 6) in SchedulerService.
 *
 * <p>These tests verify that trigger conditions are evaluated correctly and that operations are
 * skipped or executed based on table health metrics.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("SchedulerService - Trigger Conditions")
class SchedulerTriggerConditionsTest {

    private static final String CATALOG_NAME = "test-catalog";

    @Mock private PolicyStore policyStore;

    @Mock private CatalogClient catalogClient;

    @Mock private MaintenanceOrchestrator orchestrator;

    @Mock private OperationStore operationStore;

    @Mock private TableHealthStore healthStore;

    private ScheduleExecutionStore executionStore;
    private SchedulerService scheduler;

    @BeforeEach
    void setUp() {
        executionStore = new InMemoryScheduleExecutionStore();
        DistributedLock distributedLock = new DistributedLock.NoOpDistributedLock();

        scheduler =
                new SchedulerService(
                        policyStore,
                        catalogClient,
                        orchestrator,
                        executionStore,
                        distributedLock,
                        operationStore,
                        healthStore,
                        createConfig(true));

        lenient().when(catalogClient.getCatalogName()).thenReturn(CATALOG_NAME);
        lenient()
                .when(operationStore.findByTable(any(), any(), any(), anyInt()))
                .thenReturn(List.of());
    }

    @Nested
    @DisplayName("when trigger conditions are configured")
    class TriggerConditionsConfigured {

        @Test
        @DisplayName("should skip operation when no conditions are met")
        void shouldSkipWhenNoConditionsMet() {
            // Policy with trigger conditions requiring high small file percentage
            TriggerConditions conditions =
                    TriggerConditions.builder().smallFilePercentageAbove(50.0).build();

            MaintenancePolicy policy = createPolicyWithTriggerConditions(conditions);
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "db", "events");

            // Health report shows only 10% small files - below threshold
            HealthReport healthReport =
                    HealthReport.builder(table).smallFileCount(10).dataFileCount(100).build();

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(healthStore.findHistory(eq(CATALOG_NAME), eq("db"), eq("events"), eq(1)))
                    .thenReturn(List.of(healthReport));
            when(operationStore.findLastOperationTime(any(), any(), any(), any()))
                    .thenReturn(Optional.empty());

            scheduler.poll();

            // Should not trigger maintenance - conditions not met
            verify(orchestrator, never())
                    .runMaintenanceWithHealthAndStats(any(), any(), any(), any());

            // Should record execution with next check time
            var record =
                    executionStore.getRecord(
                            policy.id(), "REWRITE_DATA_FILES", CATALOG_NAME + ".db.events");
            assertThat(record).isPresent();
        }

        @Test
        @DisplayName("should trigger operation when condition is met")
        void shouldTriggerWhenConditionMet() {
            // Policy with trigger conditions requiring high small file percentage
            TriggerConditions conditions =
                    TriggerConditions.builder().smallFilePercentageAbove(50.0).build();

            MaintenancePolicy policy = createPolicyWithTriggerConditions(conditions);
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "db", "events");

            // Health report shows 60% small files - above threshold
            HealthReport healthReport =
                    HealthReport.builder(table).smallFileCount(60).dataFileCount(100).build();

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(healthStore.findHistory(eq(CATALOG_NAME), eq("db"), eq("events"), eq(1)))
                    .thenReturn(List.of(healthReport));
            when(operationStore.findLastOperationTime(any(), any(), any(), any()))
                    .thenReturn(Optional.empty());
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "id", table, policy.name(), Instant.now()));

            scheduler.poll();

            // Should trigger maintenance - condition met
            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(table), any(), any());
        }

        @Test
        @DisplayName("should respect minInterval between operations")
        void shouldRespectMinInterval() {
            // Policy with trigger conditions and 2-hour min interval
            TriggerConditions conditions =
                    TriggerConditions.builder()
                            .smallFilePercentageAbove(50.0)
                            .minIntervalMinutes(120)
                            .build();

            MaintenancePolicy policy = createPolicyWithTriggerConditions(conditions);
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "db", "events");

            // Health report shows 60% small files - condition met
            HealthReport healthReport =
                    HealthReport.builder(table).smallFileCount(60).dataFileCount(100).build();

            // Last operation was 30 minutes ago - within min interval
            Instant lastOpTime = Instant.now().minus(Duration.ofMinutes(30));

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(healthStore.findHistory(eq(CATALOG_NAME), eq("db"), eq("events"), eq(1)))
                    .thenReturn(List.of(healthReport));
            when(operationStore.findLastOperationTime(
                            eq(CATALOG_NAME),
                            eq("db"),
                            eq("events"),
                            eq(OperationType.REWRITE_DATA_FILES.name())))
                    .thenReturn(Optional.of(lastOpTime));

            scheduler.poll();

            // Should not trigger - min interval not elapsed
            verify(orchestrator, never())
                    .runMaintenanceWithHealthAndStats(any(), any(), any(), any());
        }

        @Test
        @DisplayName("should force trigger when critical pipeline deadline exceeded")
        void shouldForceTriggerOnCriticalPipelineDeadline() {
            // Critical pipeline with 1-hour max delay
            TriggerConditions conditions =
                    TriggerConditions.builder()
                            .smallFilePercentageAbove(90.0) // Very high threshold - won't be met
                            .criticalPipeline(true)
                            .criticalPipelineMaxDelayMinutes(60)
                            .build();

            MaintenancePolicy policy = createPolicyWithTriggerConditions(conditions);
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "db", "critical_events");

            // Health report shows only 10% small files - threshold NOT met
            HealthReport healthReport =
                    HealthReport.builder(table).smallFileCount(10).dataFileCount(100).build();

            // Last operation was 2 hours ago - exceeds max delay
            Instant lastOpTime = Instant.now().minus(Duration.ofHours(2));

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(healthStore.findHistory(eq(CATALOG_NAME), eq("db"), eq("critical_events"), eq(1)))
                    .thenReturn(List.of(healthReport));
            when(operationStore.findLastOperationTime(
                            eq(CATALOG_NAME),
                            eq("db"),
                            eq("critical_events"),
                            eq(OperationType.REWRITE_DATA_FILES.name())))
                    .thenReturn(Optional.of(lastOpTime));
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "id", table, policy.name(), Instant.now()));

            scheduler.poll();

            // Should force trigger due to critical pipeline deadline
            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(table), any(), any());
        }

        @Test
        @DisplayName("should evaluate OR logic - any condition triggers")
        void shouldTriggerOnAnyConditionMet() {
            // Multiple conditions - only one needs to be met
            TriggerConditions conditions =
                    TriggerConditions.builder()
                            .smallFilePercentageAbove(80.0) // NOT met
                            .snapshotCountAbove(50) // Will be met
                            .deleteFileCountAbove(200) // NOT met
                            .build();

            MaintenancePolicy policy = createPolicyWithTriggerConditions(conditions);
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "db", "events");

            // Health report: only snapshot count exceeds threshold
            HealthReport healthReport =
                    HealthReport.builder(table)
                            .smallFileCount(10)
                            .dataFileCount(100)
                            .snapshotCount(75) // Above 50 threshold
                            .deleteFileCount(50)
                            .build();

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(healthStore.findHistory(eq(CATALOG_NAME), eq("db"), eq("events"), eq(1)))
                    .thenReturn(List.of(healthReport));
            when(operationStore.findLastOperationTime(any(), any(), any(), any()))
                    .thenReturn(Optional.empty());
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "id", table, policy.name(), Instant.now()));

            scheduler.poll();

            // Should trigger - snapshot count condition met
            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(table), any(), any());
        }
    }

    @Nested
    @DisplayName("when trigger conditions are null (backward compatibility)")
    class TriggerConditionsNull {

        @Test
        @DisplayName("should always trigger when schedule is due")
        void shouldAlwaysTriggerWhenScheduleIsDue() {
            // Policy without trigger conditions - pure cron behavior
            MaintenancePolicy policy = createPolicyWithoutTriggerConditions();
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "db", "events");

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(healthStore.findHistory(any(), any(), any(), anyInt())).thenReturn(List.of());
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "id", table, policy.name(), Instant.now()));

            scheduler.poll();

            // Should trigger - no conditions to evaluate
            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(table), any(), any());
        }
    }

    @Nested
    @DisplayName("when conditionBasedTriggeringEnabled is false")
    class TriggerConditionsDisabled {

        @BeforeEach
        void setUp() {
            // Re-create scheduler with condition-based triggering disabled
            scheduler =
                    new SchedulerService(
                            policyStore,
                            catalogClient,
                            orchestrator,
                            executionStore,
                            new DistributedLock.NoOpDistributedLock(),
                            operationStore,
                            healthStore,
                            createConfig(false) // Disabled
                            );

            lenient().when(catalogClient.getCatalogName()).thenReturn(CATALOG_NAME);
            lenient()
                    .when(operationStore.findByTable(any(), any(), any(), anyInt()))
                    .thenReturn(List.of());
        }

        @Test
        @DisplayName("should ignore trigger conditions and run on schedule")
        void shouldIgnoreTriggerConditionsWhenDisabled() {
            // Policy with trigger conditions that would NOT be met
            TriggerConditions conditions =
                    TriggerConditions.builder()
                            .smallFilePercentageAbove(99.0) // Very high - won't be met
                            .build();

            MaintenancePolicy policy = createPolicyWithTriggerConditions(conditions);
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "db", "events");

            // Health report shows only 10% small files
            HealthReport healthReport =
                    HealthReport.builder(table).smallFileCount(10).dataFileCount(100).build();

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(healthStore.findHistory(eq(CATALOG_NAME), eq("db"), eq("events"), eq(1)))
                    .thenReturn(List.of(healthReport));
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "id", table, policy.name(), Instant.now()));

            scheduler.poll();

            // Should trigger anyway - conditions are ignored when feature is disabled
            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(table), any(), any());
        }
    }

    @Nested
    @DisplayName("when health data is unavailable")
    class HealthDataUnavailable {

        @Test
        @DisplayName("should skip when no health history and conditions require health data")
        void shouldSkipWhenNoHealthHistoryAndConditionsRequireHealthData() {
            TriggerConditions conditions =
                    TriggerConditions.builder().smallFilePercentageAbove(50.0).build();

            MaintenancePolicy policy = createPolicyWithTriggerConditions(conditions);
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "db", "events");

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(healthStore.findHistory(eq(CATALOG_NAME), eq("db"), eq("events"), eq(1)))
                    .thenReturn(List.of()); // No health history
            when(operationStore.findLastOperationTime(any(), any(), any(), any()))
                    .thenReturn(Optional.empty());

            scheduler.poll();

            // Should NOT trigger - conditions require health data but none available
            verify(orchestrator, never())
                    .runMaintenanceWithHealthAndStats(any(), any(), any(), any());

            // Should record execution with next check time
            var record =
                    executionStore.getRecord(
                            policy.id(), "REWRITE_DATA_FILES", CATALOG_NAME + ".db.events");
            assertThat(record).isPresent();
        }

        @Test
        @DisplayName("should trigger when no health history and only interval conditions")
        void shouldTriggerWhenNoHealthHistoryAndOnlyIntervalConditions() {
            // Conditions with only minInterval (no health-based thresholds)
            TriggerConditions conditions =
                    TriggerConditions.builder().minIntervalMinutes(60).build();

            MaintenancePolicy policy = createPolicyWithTriggerConditions(conditions);
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "db", "events");

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(healthStore.findHistory(eq(CATALOG_NAME), eq("db"), eq("events"), eq(1)))
                    .thenReturn(List.of()); // No health history
            when(operationStore.findLastOperationTime(any(), any(), any(), any()))
                    .thenReturn(Optional.empty());
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "id", table, policy.name(), Instant.now()));

            scheduler.poll();

            // Should trigger - no health-based conditions to evaluate
            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(table), any(), any());
        }
    }

    // Helper methods

    private SchedulerConfig createConfig(boolean conditionBasedTriggeringEnabled) {
        return new SchedulerConfig() {
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
                return conditionBasedTriggeringEnabled;
            }
        };
    }

    private MaintenancePolicy createPolicyWithTriggerConditions(TriggerConditions conditions) {
        return MaintenancePolicy.builder()
                .name("signal-based-policy")
                .tablePattern(TablePattern.matchAll())
                .enabled(true)
                .priority(10)
                .triggerConditions(conditions)
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .rewriteDataFilesSchedule(
                        ScheduleConfig.builder().interval(Duration.ofDays(1)).enabled(true).build())
                .build();
    }

    private MaintenancePolicy createPolicyWithoutTriggerConditions() {
        return MaintenancePolicy.builder()
                .name("cron-based-policy")
                .tablePattern(TablePattern.matchAll())
                .enabled(true)
                .priority(10)
                .triggerConditions(null) // No trigger conditions - pure cron
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .rewriteDataFilesSchedule(
                        ScheduleConfig.builder().interval(Duration.ofDays(1)).enabled(true).build())
                .build();
    }
}
