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

package com.floe.core.orchestrator;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.ExecutionContext;
import com.floe.core.engine.ExecutionEngine;
import com.floe.core.engine.ExecutionResult;
import com.floe.core.engine.ExecutionStatus;
import com.floe.core.health.HealthReport;
import com.floe.core.maintenance.*;
import com.floe.core.metrics.OperationMetricsEmitter;
import com.floe.core.operation.OperationRecord;
import com.floe.core.operation.OperationResults;
import com.floe.core.operation.OperationStatus;
import com.floe.core.operation.OperationStore;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.OperationType;
import com.floe.core.policy.PolicyMatcher;
import com.floe.core.policy.PolicyStore;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates maintenance operations for Iceberg tables.
 *
 * <p>The Orchestrator connects:
 *
 * <ul>
 *   <li>PolicyStore - where policies live
 *   <li>PolicyMatcher - which policy applies to which table
 *   <li>ExecutionEngine - where to run maintenance
 *   <li>OperationStore - where operation history is persisted
 * </ul>
 *
 * <p>Flow:
 *
 * <ol>
 *   <li>Receive table identifier
 *   <li>Create operation record in RUNNING state
 *   <li>Look up matching policy via PolicyMatcher
 *   <li>Build operations from policy configuration
 *   <li>Execute operations sequentially via ExecutionEngine
 *   <li>Update operation record with results
 *   <li>Return aggregated results
 * </ol>
 */
public class MaintenanceOrchestrator {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceOrchestrator.class);

    private final PolicyStore policyStore;
    private final PolicyMatcher policyMatcher;
    private final ExecutionEngine executionEngine;
    private final OperationStore operationStore;
    private final ExecutorService executorService;
    private final MaintenancePlanner planner;
    private final OperationMetricsEmitter metricsEmitter;

    /**
     * Create a new MaintenanceOrchestrator with a default thread pool.
     *
     * @param policyStore the store for maintenance policies
     * @param policyMatcher the matcher for finding applicable policies
     * @param executionEngine the engine for executing maintenance operations
     * @param operationStore the store for operation history
     */
    public MaintenanceOrchestrator(
            PolicyStore policyStore,
            PolicyMatcher policyMatcher,
            ExecutionEngine executionEngine,
            OperationStore operationStore,
            MaintenancePlanner planner,
            OperationMetricsEmitter metricsEmitter) {
        this(
                policyStore,
                policyMatcher,
                executionEngine,
                operationStore,
                Executors.newFixedThreadPool(
                        4,
                        r -> {
                            Thread t = new Thread(r, "floe-maintenance-orchestrator");
                            t.setDaemon(true);
                            return t;
                        }),
                planner,
                metricsEmitter);
    }

    /**
     * Create a new MaintenanceOrchestrator with a custom executor service.
     *
     * @param policyStore the store for maintenance policies
     * @param policyMatcher the matcher for finding applicable policies
     * @param executionEngine the engine for executing maintenance operations
     * @param operationStore the store for operation history
     * @param executorService the executor service for running operations
     */
    public MaintenanceOrchestrator(
            PolicyStore policyStore,
            PolicyMatcher policyMatcher,
            ExecutionEngine executionEngine,
            OperationStore operationStore,
            ExecutorService executorService) {
        this(
                policyStore,
                policyMatcher,
                executionEngine,
                operationStore,
                executorService,
                new MaintenancePlanner(),
                new OperationMetricsEmitter.NoOp());
    }

    /**
     * Create a new MaintenanceOrchestrator with a custom planner.
     *
     * @param policyStore the store for maintenance policies
     * @param policyMatcher the matcher for finding applicable policies
     * @param executionEngine the engine for executing maintenance operations
     * @param operationStore the store for operation history
     * @param planner the maintenance planner
     */
    public MaintenanceOrchestrator(
            PolicyStore policyStore,
            PolicyMatcher policyMatcher,
            ExecutionEngine executionEngine,
            OperationStore operationStore,
            MaintenancePlanner planner) {
        this(
                policyStore,
                policyMatcher,
                executionEngine,
                operationStore,
                Executors.newFixedThreadPool(
                        4,
                        r -> {
                            Thread t = new Thread(r, "floe-maintenance-orchestrator");
                            t.setDaemon(true);
                            return t;
                        }),
                planner,
                new OperationMetricsEmitter.NoOp());
    }

    /**
     * Create a new MaintenanceOrchestrator with full customization.
     *
     * @param policyStore the store for maintenance policies
     * @param policyMatcher the matcher for finding applicable policies
     * @param executionEngine the engine for executing maintenance operations
     * @param operationStore the store for operation history
     * @param executorService the executor service for running operations
     * @param planner the maintenance planner
     */
    public MaintenanceOrchestrator(
            PolicyStore policyStore,
            PolicyMatcher policyMatcher,
            ExecutionEngine executionEngine,
            OperationStore operationStore,
            ExecutorService executorService,
            MaintenancePlanner planner) {
        this(
                policyStore,
                policyMatcher,
                executionEngine,
                operationStore,
                executorService,
                planner,
                new OperationMetricsEmitter.NoOp());
    }

    public MaintenanceOrchestrator(
            PolicyStore policyStore,
            PolicyMatcher policyMatcher,
            ExecutionEngine executionEngine,
            OperationStore operationStore,
            ExecutorService executorService,
            MaintenancePlanner planner,
            OperationMetricsEmitter metricsEmitter) {
        this.policyStore = policyStore;
        this.policyMatcher = policyMatcher;
        this.executionEngine = executionEngine;
        this.operationStore = operationStore;
        this.executorService = executorService;
        this.planner = planner;
        this.metricsEmitter =
                metricsEmitter != null ? metricsEmitter : new OperationMetricsEmitter.NoOp();
    }

    /**
     * Run all enabled maintenance operations for a table based on its matching policy.
     *
     * @param catalog the catalog name
     * @param table the table to maintain
     * @return aggregated result of all operations
     */
    public OrchestratorResult runMaintenance(String catalog, TableIdentifier table) {
        return runMaintenanceWithHealth(catalog, table, null);
    }

    /**
     * Run maintenance operations for a table, using health data to plan which operations to run.
     *
     * <p>When a health report is provided, the planner determines which operations are needed based
     * on the table's health issues. Only operations recommended by the planner AND enabled in the
     * policy will run.
     *
     * @param catalog the catalog name
     * @param table the table to maintain
     * @param healthReport the health assessment for the table (may be null for legacy behavior)
     * @return aggregated result of all operations
     */
    public OrchestratorResult runMaintenanceWithHealth(
            String catalog, TableIdentifier table, HealthReport healthReport) {
        return runMaintenanceInternal(catalog, table, healthReport, null);
    }

    public OrchestratorResult runMaintenanceWithHealthAndStats(
            String catalog,
            TableIdentifier table,
            HealthReport healthReport,
            com.floe.core.operation.OperationStats recentStats) {
        return runMaintenanceInternal(catalog, table, healthReport, recentStats);
    }

    /**
     * Internal method to run maintenance with optional health data.
     *
     * @param catalog the catalog name
     * @param table the table to maintain
     * @param healthReport the health assessment (may be null)
     * @return aggregated result of all operations
     */
    private OrchestratorResult runMaintenanceInternal(
            String catalog,
            TableIdentifier table,
            HealthReport healthReport,
            com.floe.core.operation.OperationStats recentStats) {
        String orchestrationId = UUID.randomUUID().toString();
        Instant startTime = Instant.now();
        LOG.info("Starting maintenance for table: {}", table);

        // 1. Create operation record in RUNNING state
        OperationRecord record =
                operationStore.createOperation(
                        OperationRecord.builder()
                                .catalog(catalog)
                                .namespace(table.namespace())
                                .tableName(table.table())
                                .engineType(executionEngine.getEngineType().name())
                                .executionId(orchestrationId)
                                .status(OperationStatus.RUNNING)
                                .startedAt(startTime)
                                .build());

        try {
            // 2. Get all policies and find matching one
            Optional<MaintenancePolicy> matchingPolicyOpt =
                    policyMatcher.findEffectivePolicy(catalog, table);

            if (matchingPolicyOpt.isEmpty()) {
                LOG.warn("No matching maintenance policy found for table: {}", table);
                OrchestratorResult result =
                        OrchestratorResult.noPolicy(orchestrationId, table, startTime);
                persistResult(record.id(), result);
                return result;
            }

            MaintenancePolicy matchingPolicy = matchingPolicyOpt.get();
            LOG.info("Found matching policy '{}' for table: {}", matchingPolicy.name(), table);

            // Update record with policy info
            UUID policyUuid =
                    matchingPolicy.id() != null ? UUID.fromString(matchingPolicy.id()) : null;
            operationStore.updatePolicyInfo(
                    record.id(),
                    matchingPolicy.name(),
                    policyUuid,
                    matchingPolicy.updatedAt() != null
                            ? matchingPolicy.updatedAt().toString()
                            : null);
            operationStore.updateStatus(record.id(), OperationStatus.RUNNING, null);

            // 3. Determine which operations to run
            List<OperationToRun> operations;
            if (healthReport != null) {
                // Use planner to determine operations based on health
                List<PlannedOperation> plannedOps =
                        planner.plan(
                                healthReport, matchingPolicy, matchingPolicy.effectiveThresholds());

                Map<MaintenanceOperation.Type, PlannedOperation> plannedByType =
                        selectMostSeverePlans(plannedOps);

                // Convert planned operations to operations to run
                Set<MaintenanceOperation.Type> plannedTypes = new HashSet<>(plannedByType.keySet());
                operations = buildOperations(matchingPolicy, plannedTypes, plannedByType);

                LOG.info(
                        "Planner recommended {} operations for table {}: {}",
                        plannedOps.size(),
                        table,
                        plannedTypes);
            } else {
                // Legacy behavior: run all enabled operations
                operations =
                        buildOperations(
                                matchingPolicy,
                                EnumSet.allOf(MaintenanceOperation.Type.class),
                                Map.of());
            }

            if (operations.isEmpty()) {
                LOG.info(
                        "No operations to run for table {} (policy={})",
                        table,
                        matchingPolicy.name());
                OrchestratorResult result =
                        OrchestratorResult.noOperations(
                                orchestrationId, table, matchingPolicy.name(), startTime);
                persistResult(record.id(), result);
                return result;
            }

            // 4. Execute operations sequentially
            List<OperationResult> results = executeOperations(table, operations);

            // 5. Build and persist final result
            OrchestratorResult result =
                    OrchestratorResult.completed(
                            orchestrationId, table, matchingPolicy.name(), startTime, results);
            persistResult(record.id(), result);
            return result;
        } catch (Exception e) {
            LOG.error("Maintenance orchestration failed for table {}", table, e);
            operationStore.markFailed(record.id(), e.getMessage());
            throw e;
        }
    }

    /**
     * Run maintenance for a table using a specific policy by name.
     *
     * @param catalog the catalog name
     * @param table the table to maintain
     * @param policyName the name of the policy to use
     * @return aggregated result of all operations
     */
    public OrchestratorResult runMaintenanceWithPolicy(
            String catalog, TableIdentifier table, String policyName) {
        String orchestrationId = UUID.randomUUID().toString();
        Instant startTime = Instant.now();
        LOG.info("Starting maintenance for table {} with policy '{}'", table, policyName);

        // 1. Create operation record in RUNNING state
        OperationRecord record =
                operationStore.createOperation(
                        OperationRecord.builder()
                                .catalog(catalog)
                                .namespace(table.namespace())
                                .tableName(table.table())
                                .policyName(policyName)
                                .engineType(executionEngine.getEngineType().name())
                                .executionId(orchestrationId)
                                .status(OperationStatus.RUNNING)
                                .startedAt(startTime)
                                .build());

        try {
            // 2. Find policy by name
            Optional<MaintenancePolicy> policyOpt = policyStore.findByName(policyName);

            if (policyOpt.isEmpty()) {
                LOG.warn("Policy '{}' not found", policyName);
                OrchestratorResult result =
                        OrchestratorResult.noPolicyWithMessage(
                                orchestrationId,
                                table,
                                startTime,
                                "Policy '" + policyName + "' not found");
                persistResult(record.id(), result);
                return result;
            }

            MaintenancePolicy policy = policyOpt.get();
            UUID policyUuid = policy.id() != null ? UUID.fromString(policy.id()) : null;
            operationStore.updatePolicyInfo(
                    record.id(),
                    policy.name(),
                    policyUuid,
                    policy.updatedAt() != null ? policy.updatedAt().toString() : null);

            // 3. Verify policy is enabled
            if (!policy.enabled()) {
                LOG.warn("Policy '{}' is disabled", policyName);
                OrchestratorResult result =
                        OrchestratorResult.noPolicyWithMessage(
                                orchestrationId,
                                table,
                                startTime,
                                "Policy '" + policyName + "' is disabled");
                persistResult(record.id(), result);
                return result;
            }

            LOG.info("Using policy '{}' for table: {}", policy.name(), table);

            // 4. Build operations based on policy (no filter - run all enabled)
            List<OperationToRun> operations =
                    buildOperations(
                            policy, EnumSet.allOf(MaintenanceOperation.Type.class), Map.of());

            if (operations.isEmpty()) {
                LOG.info("No operations enabled in policy '{}'", policy.name());
                OrchestratorResult result =
                        OrchestratorResult.noOperations(
                                orchestrationId, table, policy.name(), startTime);
                persistResult(record.id(), result);
                return result;
            }

            // 5. Execute operations sequentially
            List<OperationResult> results = executeOperations(table, operations);

            // 6. Build and persist final result
            OrchestratorResult result =
                    OrchestratorResult.completed(
                            orchestrationId, table, policy.name(), startTime, results);
            persistResult(record.id(), result);
            return result;
        } catch (Exception e) {
            LOG.error(
                    "Maintenance orchestration failed for table {} with policy {}",
                    table,
                    policyName,
                    e);
            operationStore.markFailed(record.id(), e.getMessage());
            throw e;
        }
    }

    /**
     * Execute a list of operations sequentially, stopping on failure.
     *
     * @param table the table to run operations on
     * @param operations the list of operations to execute
     * @return list of operation results
     */
    private List<OperationResult> executeOperations(
            TableIdentifier table, List<OperationToRun> operations) {
        List<OperationResult> results = new ArrayList<>();
        boolean hadFailure = false;

        for (OperationToRun op : operations) {
            if (hadFailure) {
                LOG.info("Skipping {} due to previous failure", op.type());
                results.add(OperationResult.skipped(op.type(), "Previous operation failed"));
                continue;
            }

            LOG.info("Executing {} on table {}", op.type(), table);
            OperationResult result = executeOperation(table, op);
            results.add(result);
            metricsEmitter.recordOperationExecution(
                    result.operationType(), result.status(), result.duration());

            if (result.status() == ExecutionStatus.FAILED) {
                hadFailure = true;
                LOG.warn(
                        "Operation {} failed on table {}: {}",
                        op.type(),
                        table,
                        result.errorMessage());
            } else {
                LOG.info("Operation {} completed on table {}", op.type(), table);
            }
        }

        return results;
    }

    /**
     * Persist the orchestration result to the operation store.
     *
     * @param recordId the operation record ID
     * @param result the orchestration result to persist
     */
    private void persistResult(UUID recordId, OrchestratorResult result) {
        OperationStatus status = OperationStatus.fromOrchestratorStatus(result.status());
        OperationResults results = OperationResults.from(result.operationResults());
        operationStore.updateStatus(recordId, status, results);
    }

    /** Internal record to hold an operation and its type. */
    private record OperationToRun(
            MaintenanceOperation.Type type, MaintenanceOperation operation, String plannerReason) {}

    /** Shutdown the orchestrator's thread pool. */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.error("ExecutorService did not terminate");
                }
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Build maintenance operations from policy based on filter.
     *
     * @param policy the maintenance policy
     * @param filter the set of operation types to include
     * @return list of operations to run
     */
    private List<OperationToRun> buildOperations(
            MaintenancePolicy policy,
            Set<MaintenanceOperation.Type> filter,
            Map<MaintenanceOperation.Type, PlannedOperation> plannedOps) {
        List<OperationToRun> operations = new ArrayList<>();
        // In the order - rewrite data files, expire snapshots, orphan cleanup, rewrite manifests
        if (filter.contains(MaintenanceOperation.Type.REWRITE_DATA_FILES)
                && policy.hasOperationConfig(OperationType.REWRITE_DATA_FILES)) {
            operations.add(
                    new OperationToRun(
                            MaintenanceOperation.Type.REWRITE_DATA_FILES,
                            buildRewriteDataFilesOperation(policy),
                            plannerReasonFor(
                                    plannedOps, MaintenanceOperation.Type.REWRITE_DATA_FILES)));
        }
        if (filter.contains(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS)
                && policy.hasOperationConfig(OperationType.EXPIRE_SNAPSHOTS)) {
            operations.add(
                    new OperationToRun(
                            MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                            buildExpireSnapshotsOperation(policy),
                            plannerReasonFor(
                                    plannedOps, MaintenanceOperation.Type.EXPIRE_SNAPSHOTS)));
        }

        if (filter.contains(MaintenanceOperation.Type.ORPHAN_CLEANUP)
                && policy.hasOperationConfig(OperationType.ORPHAN_CLEANUP)) {
            operations.add(
                    new OperationToRun(
                            MaintenanceOperation.Type.ORPHAN_CLEANUP,
                            buildOrphanCleanupOperation(policy),
                            plannerReasonFor(
                                    plannedOps, MaintenanceOperation.Type.ORPHAN_CLEANUP)));
        }

        if (filter.contains(MaintenanceOperation.Type.REWRITE_MANIFESTS)
                && policy.hasOperationConfig(OperationType.REWRITE_MANIFESTS)) {
            operations.add(
                    new OperationToRun(
                            MaintenanceOperation.Type.REWRITE_MANIFESTS,
                            buildRewriteManifestsOperation(policy),
                            plannerReasonFor(
                                    plannedOps, MaintenanceOperation.Type.REWRITE_MANIFESTS)));
        }
        return operations;
    }

    private RewriteDataFilesOperation buildRewriteDataFilesOperation(MaintenancePolicy policy) {
        var config = policy.rewriteDataFiles();
        var builder = RewriteDataFilesOperation.builder();

        if (config.strategy() != null) {
            builder.strategy(RewriteDataFilesOperation.RewriteStrategy.valueOf(config.strategy()));
        }
        if (config.sortOrder() != null) {
            builder.sortOrder(config.sortOrder());
        }
        if (config.zOrderColumns() != null) {
            builder.zOrderColumns(config.zOrderColumns());
        }
        if (config.targetFileSizeBytes() != null) {
            builder.targetFileSizeBytes(config.targetFileSizeBytes());
        }
        if (config.maxFileGroupSizeBytes() != null) {
            builder.maxFileGroupSizeBytes(config.maxFileGroupSizeBytes());
        }
        if (config.maxConcurrentFileGroupRewrites() != null) {
            builder.maxConcurrentFileGroupRewrites(config.maxConcurrentFileGroupRewrites());
        }
        if (config.partialProgressEnabled() != null) {
            builder.partialProgressEnabled(config.partialProgressEnabled());
        }
        if (config.partialProgressMaxCommits() != null) {
            builder.partialProgressMaxCommits(config.partialProgressMaxCommits());
        }
        if (config.partialProgressMaxFailedCommits() != null) {
            builder.partialProgressMaxFailedCommits(config.partialProgressMaxFailedCommits());
        }
        if (config.filter() != null) {
            builder.filter(config.filter());
        }
        if (config.rewriteJobOrder() != null) {
            builder.rewriteJobOrder(
                    RewriteDataFilesOperation.RewriteJobOrder.valueOf(config.rewriteJobOrder()));
        }
        if (config.useStartingSequenceNumber() != null) {
            builder.useStartingSequenceNumber(config.useStartingSequenceNumber());
        }
        if (config.removeDanglingDeletes() != null) {
            builder.removeDanglingDeletes(config.removeDanglingDeletes());
        }
        if (config.outputSpecId() != null) {
            builder.outputSpecId(config.outputSpecId());
        }

        return builder.build();
    }

    private ExpireSnapshotsOperation buildExpireSnapshotsOperation(MaintenancePolicy policy) {
        var config = policy.expireSnapshots();
        var builder = ExpireSnapshotsOperation.builder();

        if (config.retainLast() != null && config.retainLast() >= 0) {
            builder.retainLast(config.retainLast());
        }
        if (config.maxSnapshotAge() != null) {
            builder.maxSnapshotAge(config.maxSnapshotAge());
        }
        if (config.cleanExpiredMetadata() != null) {
            builder.cleanExpiredMetadata(config.cleanExpiredMetadata());
        }

        return builder.build();
    }

    private OrphanCleanupOperation buildOrphanCleanupOperation(MaintenancePolicy policy) {
        var config = policy.orphanCleanup();
        var builder = OrphanCleanupOperation.builder();

        if (config.retentionPeriodInDays() != null) {
            builder.olderThan(config.retentionPeriodInDays());
        }
        if (config.location() != null) {
            builder.location(config.location());
        }
        if (config.prefixMismatchMode() != null) {
            builder.prefixMismatchMode(
                    OrphanCleanupOperation.PrefixMismatchMode.valueOf(config.prefixMismatchMode()));
        }

        return builder.build();
    }

    private RewriteManifestsOperation buildRewriteManifestsOperation(MaintenancePolicy policy) {
        var config = policy.rewriteManifests();
        var builder = RewriteManifestsOperation.builder();

        if (config.specId() != null) {
            builder.specId(config.specId());
        }
        if (config.stagingLocation() != null) {
            builder.stagingLocation(config.stagingLocation());
        }
        if (config.sortBy() != null) {
            builder.sortBy(config.sortBy());
        }

        return builder.build();
    }

    /**
     * Execute a single maintenance operation on a table.
     *
     * @param table the table to run the operation on
     * @param opToRun the operation to execute
     * @return the operation result
     */
    private OperationResult executeOperation(TableIdentifier table, OperationToRun opToRun) {
        Instant opStartTime = Instant.now();
        String executionId = UUID.randomUUID().toString();
        ExecutionContext context =
                ExecutionContext.builder(executionId).timeoutSeconds(3600).build();
        MaintenanceOperation currentOp = opToRun.operation;
        try {
            Future<ExecutionResult> future = executionEngine.execute(table, currentOp, context);
            ExecutionResult result = future.get();
            String errorMsg =
                    result.errorMessage().isPresent() ? result.errorMessage().get() : null;
            Map<String, Object> metrics =
                    mergePlannerReason(result.metrics(), opToRun.plannerReason());
            return new OperationResult(
                    currentOp.getType(),
                    result.status(),
                    opStartTime,
                    Instant.now(),
                    metrics,
                    errorMsg);
        } catch (Exception e) {
            LOG.error("Exception executing {} on {}", currentOp.getType(), table, e);
            return new OperationResult(
                    currentOp.getType(),
                    ExecutionStatus.FAILED,
                    opStartTime,
                    Instant.now(),
                    mergePlannerReason(Map.of(), opToRun.plannerReason()),
                    e.getMessage());
        }
    }

    private static Map<MaintenanceOperation.Type, PlannedOperation> selectMostSeverePlans(
            List<PlannedOperation> plannedOps) {
        Map<MaintenanceOperation.Type, PlannedOperation> plannedByType =
                new EnumMap<>(MaintenanceOperation.Type.class);
        for (PlannedOperation plannedOp : plannedOps) {
            PlannedOperation existing = plannedByType.get(plannedOp.operationType());
            if (existing == null
                    || plannedOp.severity().ordinal() > existing.severity().ordinal()) {
                plannedByType.put(plannedOp.operationType(), plannedOp);
            }
        }
        return plannedByType;
    }

    private static String plannerReasonFor(
            Map<MaintenanceOperation.Type, PlannedOperation> plannedOps,
            MaintenanceOperation.Type type) {
        PlannedOperation plannedOp = plannedOps.get(type);
        return plannedOp != null ? plannedOp.reason() : null;
    }

    private static Map<String, Object> mergePlannerReason(
            Map<String, Object> metrics, String plannerReason) {
        if (plannerReason == null || plannerReason.isBlank()) {
            return metrics != null ? metrics : Map.of();
        }
        Map<String, Object> merged = new HashMap<>();
        if (metrics != null) {
            merged.putAll(metrics);
        }
        merged.put("plannerReason", plannerReason);
        return merged;
    }
}
