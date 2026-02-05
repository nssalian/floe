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

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthReport;
import com.floe.core.health.TableHealthStore;
import com.floe.core.operation.NormalizedMetrics;
import com.floe.core.operation.OperationRecord;
import com.floe.core.operation.OperationStats;
import com.floe.core.operation.OperationStore;
import com.floe.core.orchestrator.MaintenanceDebtScore;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.core.orchestrator.TriggerEvaluator;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OperationType;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.ScheduleConfig;
import com.floe.core.policy.TriggerConditions;
import com.floe.core.scheduler.DistributedLock;
import com.floe.core.scheduler.ScheduleExecutionStore;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler daemon that polls for due maintenance operations and triggers them.
 *
 * <p>Every minute, the scheduler:
 *
 * <ol>
 *   <li>Acquires a distributed lock (if configured) to prevent duplicate runs
 *   <li>Lists all enabled policies
 *   <li>For each policy, checks each operation's schedule
 *   <li>For due operations, finds matching tables and triggers maintenance
 *   <li>Records execution time to prevent duplicate runs
 * </ol>
 */
@ApplicationScoped
public class SchedulerService {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerService.class);
    private static final String SCHEDULER_LOCK_NAME = "floe-scheduler";
    private static final Duration LOCK_TTL = Duration.ofMinutes(5);

    private final PolicyStore policyStore;
    private final CatalogClient catalogClient;
    private final MaintenanceOrchestrator orchestrator;
    private final ScheduleExecutionStore executionStore;
    private final DistributedLock distributedLock;
    private final OperationStore operationStore;
    private final TableHealthStore healthStore;
    private final TriggerEvaluator triggerEvaluator;
    private final boolean enabled;
    private final boolean distributedLockEnabled;
    private final boolean conditionBasedTriggeringEnabled;

    // Budget controls
    private final int maxTablesPerPoll;
    private final int maxOperationsPerPoll;
    private final long maxBytesPerHour;
    private final int failureBackoffThreshold;
    private final int failureBackoffHours;
    private final int zeroChangeThreshold;
    private final int zeroChangeFrequencyReductionPercent;
    private final int zeroChangeMinIntervalHours;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger executionCount = new AtomicInteger(0);

    // Budget tracking for current hour
    private volatile long bytesRewrittenThisHour = 0;
    private volatile Instant currentHourStart = Instant.now();
    private final Object budgetLock = new Object();
    private final AtomicBoolean hourlyBudgetInitialized = new AtomicBoolean(false);

    /**
     * Create a new SchedulerService.
     *
     * @param policyStore store for maintenance policies
     * @param catalogClient client for accessing the Iceberg catalog
     * @param orchestrator orchestrator for executing maintenance operations
     * @param executionStore store for tracking schedule executions
     * @param distributedLock lock for coordinating across replicas
     * @param config scheduler configuration
     */
    @Inject
    public SchedulerService(
            PolicyStore policyStore,
            CatalogClient catalogClient,
            MaintenanceOrchestrator orchestrator,
            ScheduleExecutionStore executionStore,
            DistributedLock distributedLock,
            OperationStore operationStore,
            TableHealthStore healthStore,
            SchedulerConfig config) {
        this.policyStore = policyStore;
        this.catalogClient = catalogClient;
        this.orchestrator = orchestrator;
        this.executionStore = executionStore;
        this.distributedLock = distributedLock;
        this.operationStore = operationStore;
        this.healthStore = healthStore;
        this.triggerEvaluator = new TriggerEvaluator();
        this.enabled = config.enabled();
        this.distributedLockEnabled = config.distributedLockEnabled();
        this.conditionBasedTriggeringEnabled = config.conditionBasedTriggeringEnabled();
        this.maxTablesPerPoll = config.maxTablesPerPoll();
        this.maxOperationsPerPoll = config.maxOperationsPerPoll();
        this.maxBytesPerHour = config.maxBytesPerHour();
        this.failureBackoffThreshold = config.failureBackoffThreshold();
        this.failureBackoffHours = config.failureBackoffHours();
        this.zeroChangeThreshold = config.zeroChangeThreshold();
        this.zeroChangeFrequencyReductionPercent = config.zeroChangeFrequencyReductionPercent();
        this.zeroChangeMinIntervalHours = config.zeroChangeMinIntervalHours();

        if (!enabled) {
            LOG.info("Scheduler is DISABLED");
        } else if (distributedLockEnabled) {
            LOG.info(
                    "Scheduler configured with distributed locking (lock: {})",
                    SCHEDULER_LOCK_NAME);
        } else {
            LOG.info("Scheduler configured for single-replica mode (no distributed lock)");
        }

        if (maxTablesPerPoll > 0 || maxOperationsPerPoll > 0 || maxBytesPerHour > 0) {
            LOG.info(
                    "Scheduler budget limits: maxTablesPerPoll={}, maxOperationsPerPoll={}, maxBytesPerHour={}",
                    maxTablesPerPoll > 0 ? maxTablesPerPoll : "unlimited",
                    maxOperationsPerPoll > 0 ? maxOperationsPerPoll : "unlimited",
                    maxBytesPerHour > 0 ? maxBytesPerHour : "unlimited");
        }
    }

    /**
     * Main scheduler loop. Runs every minute to check for due operations.
     *
     * <p>If distributed locking is enabled, acquires the "floe-scheduler" lock before processing.
     * If the lock is held by another replica, this poll cycle is skipped.
     */
    @Scheduled(every = "1m", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void poll() {
        // Check if scheduler is enabled
        if (!enabled) {
            return;
        }

        // Local concurrency guard (within this JVM)
        if (!running.compareAndSet(false, true)) {
            LOG.debug("Scheduler already running locally, skipping this cycle");
            return;
        }

        try {
            if (distributedLockEnabled) {
                pollWithDistributedLock();
            } else {
                doPoll();
            }
        } finally {
            running.set(false);
        }
    }

    /** Poll with distributed lock acquisition. */
    private void pollWithDistributedLock() {
        Optional<DistributedLock.LockHandle> lockHandle =
                distributedLock.tryAcquire(SCHEDULER_LOCK_NAME, LOCK_TTL);

        if (lockHandle.isEmpty()) {
            LOG.debug(
                    "Scheduler lock '{}' held by another replica, skipping this cycle",
                    SCHEDULER_LOCK_NAME);
            return;
        }

        try (DistributedLock.LockHandle lock = lockHandle.get()) {
            LOG.debug("Acquired scheduler lock '{}'", SCHEDULER_LOCK_NAME);
            doPoll();
        }
    }

    /** Execute the actual poll logic. */
    private void doPoll() {
        try {
            Instant now = Instant.now();
            LOG.debug("Scheduler poll started at {}", now);

            initializeHourlyBudgetIfNeeded(now);

            // Reset hourly budget if hour has changed
            resetHourlyBudgetIfNeeded(now);

            // Initialize poll-level budget counters
            int tablesProcessed = 0;
            int operationsTriggered = 0;

            List<MaintenancePolicy> enabledPolicies = policyStore.listEnabled();
            LOG.debug("Found {} enabled policies", enabledPolicies.size());

            for (MaintenancePolicy policy : enabledPolicies) {
                // Check if we've hit budget limits
                if (isBudgetExhausted(tablesProcessed, operationsTriggered)) {
                    LOG.info(
                            "Budget exhausted (tables: {}, operations: {}), stopping poll cycle",
                            tablesProcessed,
                            operationsTriggered);
                    break;
                }

                int[] result = processPolicy(policy, now, tablesProcessed, operationsTriggered);
                tablesProcessed = result[0];
                operationsTriggered = result[1];
            }

            if (operationsTriggered > 0) {
                LOG.info(
                        "Scheduler triggered {} operations across {} tables",
                        operationsTriggered,
                        tablesProcessed);
            }

            executionCount.incrementAndGet();
        } catch (Exception e) {
            LOG.error("Scheduler poll failed", e);
        }
    }

    /** Reset the hourly budget counter if we've entered a new hour. */
    private void resetHourlyBudgetIfNeeded(Instant now) {
        synchronized (budgetLock) {
            if (Duration.between(currentHourStart, now).toHours() >= 1) {
                currentHourStart = truncateToHour(now);
                bytesRewrittenThisHour = computeBytesRewritten(currentHourStart, now);
                LOG.debug("Reset hourly bytes budget");
            }
        }
    }

    private void initializeHourlyBudgetIfNeeded(Instant now) {
        if (maxBytesPerHour <= 0 || operationStore == null) {
            return;
        }
        if (!hourlyBudgetInitialized.compareAndSet(false, true)) {
            return;
        }
        synchronized (budgetLock) {
            currentHourStart = truncateToHour(now);
            bytesRewrittenThisHour = computeBytesRewritten(currentHourStart, now);
        }
    }

    /** Check if any budget limit has been exhausted. */
    private boolean isBudgetExhausted(int tablesProcessed, int operationsTriggered) {
        if (maxTablesPerPoll > 0 && tablesProcessed >= maxTablesPerPoll) {
            return true;
        }
        if (maxOperationsPerPoll > 0 && operationsTriggered >= maxOperationsPerPoll) {
            return true;
        }
        if (maxBytesPerHour > 0) {
            synchronized (budgetLock) {
                if (bytesRewrittenThisHour >= maxBytesPerHour) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Process a single policy, checking each operation for due executions.
     *
     * @param policy The policy to process
     * @param now Current time
     * @param currentTables Current count of tables processed
     * @param currentOps Current count of operations triggered
     * @return Array of [tablesProcessed, operationsTriggered]
     */
    private int[] processPolicy(
            MaintenancePolicy policy, Instant now, int currentTables, int currentOps) {
        int tablesProcessed = currentTables;
        int operationsTriggered = currentOps;

        for (OperationType opType : OperationType.values()) {
            if (!policy.isOperationEnabled(opType)) {
                continue;
            }

            ScheduleConfig schedule = policy.getSchedule(opType);
            if (schedule == null || !schedule.canRunNow()) {
                continue;
            }

            // Check budget before processing
            if (isBudgetExhausted(tablesProcessed, operationsTriggered)) {
                break;
            }

            int[] result =
                    processOperation(
                            policy, opType, schedule, now, tablesProcessed, operationsTriggered);
            tablesProcessed = result[0];
            operationsTriggered = result[1];
        }

        return new int[] {tablesProcessed, operationsTriggered};
    }

    /**
     * Process a single operation for a policy, finding matching tables and triggering maintenance.
     *
     * @param policy the maintenance policy
     * @param opType the operation type to process
     * @param schedule the schedule configuration
     * @param now current time
     * @param currentTables current count of tables processed
     * @param currentOps current count of operations triggered
     * @return array of [tablesProcessed, operationsTriggered]
     */
    private int[] processOperation(
            MaintenancePolicy policy,
            OperationType opType,
            ScheduleConfig schedule,
            Instant now,
            int currentTables,
            int currentOps) {
        int tablesProcessed = currentTables;
        int operationsTriggered = currentOps;

        // Get all tables from catalog
        List<TableIdentifier> allTables = catalogClient.listAllTables();
        String catalogName = catalogClient.getCatalogName();

        List<TableCandidate> candidates = new java.util.ArrayList<>();

        for (TableIdentifier table : allTables) {
            if (!policy.tablePattern().matches(catalogName, table)) {
                continue;
            }

            String tableKey = buildTableKey(catalogName, table);

            if (!isOperationDue(policy.id(), opType.name(), tableKey, schedule, now)) {
                continue;
            }

            HealthReport latestHealth = loadLatestHealth(catalogName, table);
            OperationStats recentStats = loadRecentStats(catalogName, table);
            MaintenanceDebtScore score = MaintenanceDebtScore.calculate(latestHealth, recentStats);

            candidates.add(new TableCandidate(table, tableKey, latestHealth, recentStats, score));
        }

        candidates.sort(
                (a, b) -> {
                    int byScore = Double.compare(b.score.score(), a.score.score());
                    if (byScore != 0) {
                        return byScore;
                    }
                    return a.table.toQualifiedName().compareTo(b.table.toQualifiedName());
                });

        for (TableCandidate candidate : candidates) {
            if (isBudgetExhausted(tablesProcessed, operationsTriggered)) {
                break;
            }

            Optional<Instant> throttledNextRun =
                    evaluateThrottling(candidate.recentStats, schedule, now);
            if (throttledNextRun.isPresent()) {
                executionStore.recordExecution(
                        policy.id(),
                        opType.name(),
                        candidate.tableKey,
                        now,
                        throttledNextRun.get());
                continue;
            }

            // Evaluate trigger conditions if configured and feature is enabled
            TriggerConditions triggerConditions = policy.effectiveTriggerConditions();
            if (conditionBasedTriggeringEnabled && triggerConditions != null) {
                Instant lastOpTime =
                        operationStore
                                .findLastOperationTime(
                                        catalogName,
                                        candidate.table.namespace(),
                                        candidate.table.table(),
                                        opType.name())
                                .orElse(null);

                TriggerEvaluator.EvaluationResult evalResult =
                        triggerEvaluator.evaluate(
                                triggerConditions, candidate.latestHealth, lastOpTime, opType, now);

                if (!evalResult.shouldTrigger()) {
                    LOG.debug(
                            "Trigger conditions not met for {} on {}: {}",
                            opType,
                            candidate.tableKey,
                            evalResult.unmetConditions());
                    // Schedule next check - use min interval if available, otherwise re-check next
                    // poll
                    Instant nextCheck = evalResult.nextEligibleTime();
                    if (nextCheck == null) {
                        // No min interval - re-check on next scheduler poll (1 minute)
                        nextCheck = now.plusSeconds(60);
                    }
                    executionStore.recordExecution(
                            policy.id(), opType.name(), candidate.tableKey, now, nextCheck);
                    continue;
                }

                if (evalResult.forcedByCriticalPipeline()) {
                    LOG.info(
                            "Forcing {} on {} due to critical pipeline deadline",
                            opType,
                            candidate.tableKey);
                }
            }

            OrchestratorResult result =
                    triggerMaintenance(
                            policy,
                            opType,
                            catalogName,
                            candidate.table,
                            candidate.tableKey,
                            schedule,
                            now,
                            candidate.latestHealth,
                            candidate.recentStats);
            if (result != null) {
                tablesProcessed++;
                operationsTriggered++;

                // Track bytes rewritten for hourly budget
                trackBytesRewritten(result);
            }
        }

        return new int[] {tablesProcessed, operationsTriggered};
    }

    /** Extract and track bytes rewritten from operation result. */
    private void trackBytesRewritten(OrchestratorResult result) {
        if (result.aggregateMetrics() != null) {
            Object bytes = result.aggregateMetrics().get("bytesRewritten");
            if (bytes instanceof Number) {
                synchronized (budgetLock) {
                    bytesRewrittenThisHour += ((Number) bytes).longValue();
                }
            }
        }
    }

    private long computeBytesRewritten(Instant start, Instant end) {
        if (operationStore == null) {
            return 0L;
        }
        List<OperationRecord> records =
                operationStore.findInTimeRange(start, end, Integer.MAX_VALUE);
        if (records == null || records.isEmpty()) {
            return 0L;
        }
        long sum = 0L;
        for (OperationRecord record : records) {
            Map<String, Object> metrics =
                    record.normalizedMetrics() != null
                            ? record.normalizedMetrics()
                            : record.results() != null
                                    ? record.results().aggregatedMetrics()
                                    : Map.of();
            sum += getLong(metrics, NormalizedMetrics.BYTES_REWRITTEN);
        }
        return sum;
    }

    private long getLong(Map<String, Object> metrics, String key) {
        if (metrics == null || key == null) {
            return 0L;
        }
        Object value = metrics.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        return 0L;
    }

    private Instant truncateToHour(Instant instant) {
        return instant.truncatedTo(ChronoUnit.HOURS);
    }

    /**
     * Check if an operation is due for execution.
     *
     * @param policyId the policy ID
     * @param opType the operation type name
     * @param tableKey the fully qualified table key
     * @param schedule the schedule configuration
     * @param now current time
     * @return true if the operation is due, false otherwise
     */
    private boolean isOperationDue(
            String policyId, String opType, String tableKey, ScheduleConfig schedule, Instant now) {
        return executionStore
                .getRecord(policyId, opType, tableKey)
                .map(record -> record.isDue(now))
                .orElse(true); // Never run = due immediately
    }

    private HealthReport loadLatestHealth(String catalog, TableIdentifier table) {
        if (healthStore == null) {
            return null;
        }
        List<HealthReport> history =
                healthStore.findHistory(catalog, table.namespace(), table.table(), 1);
        return history.isEmpty() ? null : history.get(0);
    }

    private OperationStats loadRecentStats(String catalog, TableIdentifier table) {
        if (operationStore == null) {
            return null;
        }
        List<OperationRecord> records =
                operationStore.findByTable(catalog, table.namespace(), table.table(), 10);
        return OperationStats.fromRecords(records, 10);
    }

    private Optional<Instant> evaluateThrottling(
            OperationStats stats, ScheduleConfig schedule, Instant now) {
        if (stats == null) {
            return Optional.empty();
        }

        if (stats.consecutiveFailures() >= failureBackoffThreshold && stats.lastRunAt() != null) {
            Instant backoffUntil = stats.lastRunAt().plus(Duration.ofHours(failureBackoffHours));
            if (backoffUntil.isAfter(now)) {
                return Optional.of(backoffUntil);
            }
        }

        if (stats.consecutiveZeroChangeRuns() >= zeroChangeThreshold) {
            Duration minInterval = Duration.ofHours(zeroChangeMinIntervalHours);
            Duration adjusted = minInterval;

            if (schedule.intervalInDays() != null) {
                double multiplier = 1.0 + (zeroChangeFrequencyReductionPercent / 100.0);
                long factor = Math.max(1, Math.round(multiplier));
                adjusted = schedule.intervalInDays().multipliedBy(factor);
                if (adjusted.compareTo(minInterval) < 0) {
                    adjusted = minInterval;
                }
            }

            return Optional.of(now.plus(adjusted));
        }

        return Optional.empty();
    }

    private record TableCandidate(
            TableIdentifier table,
            String tableKey,
            HealthReport latestHealth,
            OperationStats recentStats,
            MaintenanceDebtScore score) {}

    /**
     * Trigger maintenance for a table and record the execution.
     *
     * @param policy the maintenance policy
     * @param opType the operation type
     * @param catalogName the catalog name
     * @param table the table identifier
     * @param tableKey the fully qualified table key
     * @param schedule the schedule configuration
     * @param now current time
     * @return the OrchestratorResult if successful, null on error
     */
    private OrchestratorResult triggerMaintenance(
            MaintenancePolicy policy,
            OperationType opType,
            String catalogName,
            TableIdentifier table,
            String tableKey,
            ScheduleConfig schedule,
            Instant now,
            HealthReport latestHealth,
            OperationStats recentStats) {
        try {
            LOG.info(
                    "Triggering {} for table {} (policy: {})",
                    opType,
                    tableKey,
                    policy.getNameOrDefault());

            OrchestratorResult result =
                    orchestrator.runMaintenanceWithHealthAndStats(
                            catalogName, table, latestHealth, recentStats);

            // Calculate next run time
            Instant nextRun = calculateNextRun(schedule, now);

            // Record the execution
            executionStore.recordExecution(policy.id(), opType.name(), tableKey, now, nextRun);

            LOG.info(
                    "Completed {} for table {} with status: {}. Next run: {}",
                    opType,
                    tableKey,
                    result.status(),
                    nextRun);

            return result;
        } catch (Exception e) {
            LOG.error("Failed to trigger {} for table {}", opType, tableKey, e);
            return null;
        }
    }

    /**
     * Calculate the next run time based on the schedule configuration.
     *
     * @param schedule the schedule configuration (interval or cron)
     * @param fromTime the time to calculate from
     * @return the next scheduled run time
     */
    Instant calculateNextRun(ScheduleConfig schedule, Instant fromTime) {
        // Use interval if specified
        if (schedule.intervalInDays() != null) {
            return fromTime.plus(schedule.intervalInDays());
        }

        // Parse cron expression using Quartz
        if (schedule.cronExpression() != null) {
            try {
                CronExpression cron = new CronExpression(schedule.cronExpression());
                Date nextRun = cron.getNextValidTimeAfter(Date.from(fromTime));
                if (nextRun != null) {
                    return nextRun.toInstant();
                }
            } catch (ParseException e) {
                LOG.warn(
                        "Invalid cron expression '{}', falling back to daily: {}",
                        schedule.cronExpression(),
                        e.getMessage());
            }
        }

        // Default: 1 day
        return fromTime.plus(Duration.ofDays(1));
    }

    /**
     * Build a fully qualified table key.
     *
     * @param catalog the catalog name
     * @param table the table identifier
     * @return the key in format "catalog.namespace.table"
     */
    private String buildTableKey(String catalog, TableIdentifier table) {
        return catalog + "." + table.namespace() + "." + table.table();
    }

    /** Get the number of scheduler poll cycles completed. */
    public int getExecutionCount() {
        return executionCount.get();
    }

    /** Check if the scheduler is currently running a poll cycle. */
    public boolean isRunning() {
        return running.get();
    }

    /** Check if the scheduler is enabled. */
    public boolean isEnabled() {
        return enabled;
    }

    /** Check if distributed locking is enabled. */
    public boolean isDistributedLockEnabled() {
        return distributedLockEnabled;
    }

    /** Manually trigger a poll cycle (for testing). */
    public void triggerPoll() {
        poll();
    }
}
