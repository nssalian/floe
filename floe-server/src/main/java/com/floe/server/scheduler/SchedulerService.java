package com.floe.server.scheduler;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
import com.floe.core.orchestrator.OrchestratorResult;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OperationType;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.ScheduleConfig;
import com.floe.core.scheduler.DistributedLock;
import com.floe.core.scheduler.ScheduleExecutionStore;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
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
    private final boolean enabled;
    private final boolean distributedLockEnabled;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger executionCount = new AtomicInteger(0);

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
            SchedulerConfig config) {
        this.policyStore = policyStore;
        this.catalogClient = catalogClient;
        this.orchestrator = orchestrator;
        this.executionStore = executionStore;
        this.distributedLock = distributedLock;
        this.enabled = config.enabled();
        this.distributedLockEnabled = config.distributedLockEnabled();

        if (!enabled) {
            LOG.info("Scheduler is DISABLED");
        } else if (distributedLockEnabled) {
            LOG.info(
                    "Scheduler configured with distributed locking (lock: {})",
                    SCHEDULER_LOCK_NAME);
        } else {
            LOG.info("Scheduler configured for single-replica mode (no distributed lock)");
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

            List<MaintenancePolicy> enabledPolicies = policyStore.listEnabled();
            LOG.debug("Found {} enabled policies", enabledPolicies.size());

            int triggered = 0;
            for (MaintenancePolicy policy : enabledPolicies) {
                triggered += processPolicy(policy, now);
            }

            if (triggered > 0) {
                LOG.info("Scheduler triggered {} maintenance operations", triggered);
            }

            executionCount.incrementAndGet();
        } catch (Exception e) {
            LOG.error("Scheduler poll failed", e);
        }
    }

    /**
     * Process a single policy, checking each operation for due executions.
     *
     * @param policy The policy to process
     * @param now Current time
     * @return Number of operations triggered
     */
    private int processPolicy(MaintenancePolicy policy, Instant now) {
        int triggered = 0;

        for (OperationType opType : OperationType.values()) {
            if (!policy.isOperationEnabled(opType)) {
                continue;
            }

            ScheduleConfig schedule = policy.getSchedule(opType);
            if (schedule == null || !schedule.canRunNow()) {
                continue;
            }

            triggered += processOperation(policy, opType, schedule, now);
        }

        return triggered;
    }

    /**
     * Process a single operation for a policy, finding matching tables and triggering maintenance.
     *
     * @param policy the maintenance policy
     * @param opType the operation type to process
     * @param schedule the schedule configuration
     * @param now current time
     * @return number of tables triggered
     */
    private int processOperation(
            MaintenancePolicy policy, OperationType opType, ScheduleConfig schedule, Instant now) {
        int triggered = 0;

        // Get all tables from catalog
        List<TableIdentifier> allTables = catalogClient.listAllTables();
        String catalogName = catalogClient.getCatalogName();

        for (TableIdentifier table : allTables) {
            // Check if this table matches the policy pattern
            if (!policy.tablePattern().matches(catalogName, table)) {
                continue;
            }

            String tableKey = buildTableKey(catalogName, table);

            // Check if this operation is due for this table
            if (!isOperationDue(policy.id(), opType.name(), tableKey, schedule, now)) {
                continue;
            }

            // Trigger the maintenance
            if (triggerMaintenance(policy, opType, catalogName, table, tableKey, schedule, now)) {
                triggered++;
            }
        }

        return triggered;
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
     * @return true if maintenance was triggered successfully, false on error
     */
    private boolean triggerMaintenance(
            MaintenancePolicy policy,
            OperationType opType,
            String catalogName,
            TableIdentifier table,
            String tableKey,
            ScheduleConfig schedule,
            Instant now) {
        try {
            LOG.info(
                    "Triggering {} for table {} (policy: {})",
                    opType,
                    tableKey,
                    policy.getNameOrDefault());

            OrchestratorResult result = orchestrator.runMaintenance(catalogName, table);

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

            return true;
        } catch (Exception e) {
            LOG.error("Failed to trigger {} for table {}", opType, tableKey, e);
            return false;
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
