package com.floe.server.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.TableHealthStore;
import com.floe.core.operation.OperationStore;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.core.policy.*;
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

@ExtendWith(MockitoExtension.class)
@DisplayName("SchedulerService")
class SchedulerServiceTest {

    private static final String CATALOG_NAME = "test-catalog";

    @Mock private PolicyStore policyStore;

    @Mock private CatalogClient catalogClient;

    @Mock private MaintenanceOrchestrator orchestrator;

    @Mock private OperationStore operationStore;

    @Mock private TableHealthStore healthStore;

    private ScheduleExecutionStore executionStore;
    private SchedulerService schedulerService;

    @BeforeEach
    void setUp() {
        executionStore = new InMemoryScheduleExecutionStore();
        DistributedLock distributedLock = new DistributedLock.NoOpDistributedLock();
        // Scheduler enabled, distributed lock disabled by default in tests
        SchedulerConfig schedulerConfig =
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
                };
        schedulerService =
                new SchedulerService(
                        policyStore,
                        catalogClient,
                        orchestrator,
                        executionStore,
                        distributedLock,
                        operationStore,
                        healthStore,
                        schedulerConfig);
        // Use lenient to avoid UnnecessaryStubbingException for tests that don't use catalog
        lenient().when(catalogClient.getCatalogName()).thenReturn(CATALOG_NAME);
        lenient()
                .when(operationStore.findByTable(any(), any(), any(), anyInt()))
                .thenReturn(List.of());
        lenient()
                .when(healthStore.findHistory(any(), any(), any(), anyInt()))
                .thenReturn(List.of());
    }

    @Nested
    @DisplayName("poll")
    class PollTests {

        @Test
        @DisplayName("should do nothing when no enabled policies exist")
        void shouldDoNothingWhenNoEnabledPolicies() {
            when(policyStore.listEnabled()).thenReturn(List.of());

            schedulerService.poll();

            verify(orchestrator, never())
                    .runMaintenanceWithHealthAndStats(anyString(), any(), any(), any());
            assertThat(schedulerService.getExecutionCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("should trigger maintenance for due operation")
        void shouldTriggerMaintenanceForDueOperation() {
            MaintenancePolicy policy = createPolicy("test-policy", "*.*.*");
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "namespace", "table");

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "test-id", table, policy.name(), Instant.now()));

            schedulerService.poll();

            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(table), any(), any());
        }

        @Test
        @DisplayName("should not trigger maintenance when operation is not due")
        void shouldNotTriggerMaintenanceWhenOperationIsNotDue() {
            MaintenancePolicy policy = createPolicy("test-policy", "*.*.*");
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "namespace", "table");

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));

            // Pre-record an execution that is not yet due
            String tableKey = CATALOG_NAME + ".namespace.table";
            Instant now = Instant.now();
            executionStore.recordExecution(
                    policy.id(),
                    "REWRITE_DATA_FILES",
                    tableKey,
                    now,
                    now.plusSeconds(3600)); // Due in 1 hour

            schedulerService.poll();

            verify(orchestrator, never())
                    .runMaintenanceWithHealthAndStats(anyString(), any(), any(), any());
        }

        @Test
        @DisplayName("should only process matching tables for policy pattern")
        void shouldOnlyProcessMatchingTablesForPolicyPattern() {
            MaintenancePolicy policy = createPolicy("prod-policy", "test-catalog.prod.*");
            TableIdentifier prodTable = TableIdentifier.of(CATALOG_NAME, "prod", "events");
            TableIdentifier devTable = TableIdentifier.of(CATALOG_NAME, "dev", "events");

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(prodTable, devTable));
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(prodTable), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "test-id", prodTable, policy.name(), Instant.now()));

            schedulerService.poll();

            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(prodTable), any(), any());
            verify(orchestrator, never())
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(devTable), any(), any());
        }

        @Test
        @DisplayName("should skip policy when schedule window is not active")
        void shouldSkipPolicyWhenScheduleWindowIsNotActive() {
            // Create policy with a schedule that has canRunNow() returning false
            MaintenancePolicy policy = createPolicyWithDisabledSchedule("test-policy", "*.*.*");

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            // Note: listAllTables won't be called since policy has no enabled operations

            schedulerService.poll();

            verify(orchestrator, never())
                    .runMaintenanceWithHealthAndStats(anyString(), any(), any(), any());
        }

        @Test
        @DisplayName("should record execution after triggering maintenance")
        void shouldRecordExecutionAfterTriggeringMaintenance() {
            MaintenancePolicy policy = createPolicy("test-policy", "*.*.*");
            TableIdentifier table = TableIdentifier.of(CATALOG_NAME, "namespace", "table");
            String tableKey = CATALOG_NAME + ".namespace.table";

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table));
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "test-id", table, policy.name(), Instant.now()));

            schedulerService.poll();

            var record = executionStore.getRecord(policy.id(), "REWRITE_DATA_FILES", tableKey);
            assertThat(record).isPresent();
            assertThat(record.get().lastRunAt()).isNotNull();
            assertThat(record.get().nextRunAt()).isAfter(record.get().lastRunAt());
        }

        @Test
        @DisplayName("should handle orchestrator exceptions gracefully")
        void shouldHandleOrchestratorExceptionsGracefully() {
            MaintenancePolicy policy = createPolicy("test-policy", "*.*.*");
            TableIdentifier table1 = TableIdentifier.of(CATALOG_NAME, "ns", "table1");
            TableIdentifier table2 = TableIdentifier.of(CATALOG_NAME, "ns", "table2");

            when(policyStore.listEnabled()).thenReturn(List.of(policy));
            when(catalogClient.listAllTables()).thenReturn(List.of(table1, table2));
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table1), any(), any()))
                    .thenThrow(new RuntimeException("Simulated failure"));
            when(orchestrator.runMaintenanceWithHealthAndStats(
                            eq(CATALOG_NAME), eq(table2), any(), any()))
                    .thenReturn(
                            OrchestratorResult.noOperations(
                                    "test-id", table2, policy.name(), Instant.now()));

            // Should not throw, and should continue to table2
            schedulerService.poll();

            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(table1), any(), any());
            verify(orchestrator)
                    .runMaintenanceWithHealthAndStats(eq(CATALOG_NAME), eq(table2), any(), any());
        }

        @Test
        @DisplayName("should skip concurrent poll execution")
        void shouldSkipConcurrentPollExecution() {
            // Simulate a long-running poll
            when(policyStore.listEnabled())
                    .thenAnswer(
                            invocation -> {
                                // While this is running, mark as running
                                assertThat(schedulerService.isRunning()).isTrue();
                                return List.of();
                            });

            schedulerService.poll();

            assertThat(schedulerService.isRunning()).isFalse();
        }

        @Test
        @DisplayName("should increment execution count on each poll")
        void shouldIncrementExecutionCountOnEachPoll() {
            when(policyStore.listEnabled()).thenReturn(List.of());

            assertThat(schedulerService.getExecutionCount()).isZero();

            schedulerService.poll();
            assertThat(schedulerService.getExecutionCount()).isEqualTo(1);

            schedulerService.poll();
            assertThat(schedulerService.getExecutionCount()).isEqualTo(2);

            schedulerService.poll();
            assertThat(schedulerService.getExecutionCount()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("calculateNextRun")
    class CalculateNextRunTests {

        @Test
        @DisplayName("should calculate next run from interval")
        void shouldCalculateNextRunFromInterval() {
            ScheduleConfig schedule =
                    ScheduleConfig.builder().interval(Duration.ofHours(6)).enabled(true).build();

            Instant fromTime = Instant.parse("2024-01-15T10:00:00Z");
            Instant nextRun = schedulerService.calculateNextRun(schedule, fromTime);

            assertThat(nextRun).isEqualTo(Instant.parse("2024-01-15T16:00:00Z"));
        }

        @Test
        @DisplayName("should calculate next run from cron expression as daily fallback")
        void shouldCalculateNextRunFromCronExpressionAsDailyFallback() {
            ScheduleConfig schedule =
                    ScheduleConfig.builder().cronExpression("0 2 * * *").enabled(true).build();

            Instant fromTime = Instant.parse("2024-01-15T10:00:00Z");
            Instant nextRun = schedulerService.calculateNextRun(schedule, fromTime);

            // Simplified: cron falls back to 1 day
            assertThat(nextRun).isEqualTo(Instant.parse("2024-01-16T10:00:00Z"));
        }

        @Test
        @DisplayName("should default to 1 day when no schedule specified")
        void shouldDefaultToOneDayWhenNoScheduleSpecified() {
            ScheduleConfig schedule = ScheduleConfig.builder().enabled(true).build();

            Instant fromTime = Instant.parse("2024-01-15T10:00:00Z");
            Instant nextRun = schedulerService.calculateNextRun(schedule, fromTime);

            assertThat(nextRun).isEqualTo(Instant.parse("2024-01-16T10:00:00Z"));
        }
    }

    @Nested
    @DisplayName("triggerPoll")
    class TriggerPollTests {

        @Test
        @DisplayName("should manually trigger poll cycle")
        void shouldManuallyTriggerPollCycle() {
            when(policyStore.listEnabled()).thenReturn(List.of());

            schedulerService.triggerPoll();

            assertThat(schedulerService.getExecutionCount()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("distributedLocking")
    class DistributedLockingTests {

        @Test
        @DisplayName("should report distributed lock disabled by default")
        void shouldReportDistributedLockDisabledByDefault() {
            assertThat(schedulerService.isDistributedLockEnabled()).isFalse();
        }

        @Test
        @DisplayName("should use distributed lock when enabled")
        void shouldUseDistributedLockWhenEnabled() {
            // Create scheduler with distributed lock enabled
            SchedulerConfig enabledConfig =
                    new SchedulerConfig() {
                        @Override
                        public boolean enabled() {
                            return true;
                        }

                        @Override
                        public boolean distributedLockEnabled() {
                            return true;
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
                    };
            DistributedLock mockLock = mock(DistributedLock.class);
            DistributedLock.LockHandle mockHandle = mock(DistributedLock.LockHandle.class);

            when(mockLock.tryAcquire(eq("floe-scheduler"), any(Duration.class)))
                    .thenReturn(Optional.of(mockHandle));
            when(policyStore.listEnabled()).thenReturn(List.of());

            SchedulerService lockingScheduler =
                    new SchedulerService(
                            policyStore,
                            catalogClient,
                            orchestrator,
                            executionStore,
                            mockLock,
                            operationStore,
                            healthStore,
                            enabledConfig);

            lockingScheduler.poll();

            verify(mockLock).tryAcquire(eq("floe-scheduler"), any(Duration.class));
            verify(mockHandle).close();
            assertThat(lockingScheduler.getExecutionCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("should skip poll when lock is held by another replica")
        void shouldSkipPollWhenLockIsHeldByAnotherReplica() {
            // Create scheduler with distributed lock enabled
            SchedulerConfig enabledConfig =
                    new SchedulerConfig() {
                        @Override
                        public boolean enabled() {
                            return true;
                        }

                        @Override
                        public boolean distributedLockEnabled() {
                            return true;
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
                    };
            DistributedLock mockLock = mock(DistributedLock.class);

            // Lock is held by another process
            when(mockLock.tryAcquire(eq("floe-scheduler"), any(Duration.class)))
                    .thenReturn(Optional.empty());

            SchedulerService lockingScheduler =
                    new SchedulerService(
                            policyStore,
                            catalogClient,
                            orchestrator,
                            executionStore,
                            mockLock,
                            operationStore,
                            healthStore,
                            enabledConfig);

            lockingScheduler.poll();

            verify(mockLock).tryAcquire(eq("floe-scheduler"), any(Duration.class));
            // Should not have executed any policies
            verify(policyStore, never()).listEnabled();
            assertThat(lockingScheduler.getExecutionCount()).isZero();
        }
    }

    // Helper methods

    private MaintenancePolicy createPolicy(String name, String pattern) {
        return MaintenancePolicy.builder()
                .name(name)
                .tablePattern(TablePattern.parse(pattern))
                .enabled(true)
                .priority(10)
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .rewriteDataFilesSchedule(
                        ScheduleConfig.builder().interval(Duration.ofDays(1)).enabled(true).build())
                .build();
    }

    private MaintenancePolicy createPolicyWithDisabledSchedule(String name, String pattern) {
        return MaintenancePolicy.builder()
                .name(name)
                .tablePattern(TablePattern.parse(pattern))
                .enabled(true)
                .priority(10)
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .rewriteDataFilesSchedule(
                        ScheduleConfig.builder()
                                .interval(Duration.ofDays(1))
                                .enabled(false) // Schedule disabled
                                .build())
                .build();
    }
}
