package com.floe.server.metrics;

import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.operation.OperationStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Centralized metrics service for Floe application.
 *
 * <p>Provides instrumentation for:
 *
 * <ul>
 *   <li>Maintenance operations (compaction, expiration, cleanup, manifests)
 *   <li>Policy management (create, update, delete, matches)
 *   <li>Authentication (success, failure, rate limits)
 *   <li>Table health assessments
 *   <li>API request metrics
 * </ul>
 *
 * <p>All metrics are prefixed with "floe_" for easy identification in monitoring systems.
 */
@ApplicationScoped
public class FloeMetrics {

    private static final String PREFIX = "floe";

    private final MeterRegistry registry;

    // Maintenance operation metrics
    private final Counter maintenanceTriggersTotal;
    private final Counter maintenanceSuccessTotal;
    private final Counter maintenanceFailuresTotal;
    private final ConcurrentHashMap<String, Timer> operationTimers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> operationCounters = new ConcurrentHashMap<>();

    // Policy metrics
    private final Counter policiesCreatedTotal;
    private final Counter policiesUpdatedTotal;
    private final Counter policiesDeletedTotal;
    private final Counter policyMatchesTotal;
    private final Counter policyNoMatchTotal;

    // Authentication metrics
    private final Counter authSuccessTotal;
    private final Counter authFailureTotal;

    // Trigger evaluation metrics
    private final Counter triggerEvaluationsTotal;
    private final Counter triggerConditionsMetTotal;
    private final Counter triggerConditionsNotMetTotal;
    private final Counter triggerBlockedByIntervalTotal;
    private final Counter triggerForcedByCriticalPipelineTotal;

    // Gauges for current state
    private final AtomicLong activePoliciesCount = new AtomicLong(0);
    private final AtomicLong runningOperationsCount = new AtomicLong(0);

    @Inject
    public FloeMetrics(MeterRegistry registry) {
        this.registry = registry;

        // Maintenance Operation Metrics

        this.maintenanceTriggersTotal =
                Counter.builder(PREFIX + "_maintenance_triggers_total")
                        .description("Total number of maintenance operations triggered")
                        .register(registry);

        this.maintenanceSuccessTotal =
                Counter.builder(PREFIX + "_maintenance_success_total")
                        .description("Total number of successful maintenance operations")
                        .register(registry);

        this.maintenanceFailuresTotal =
                Counter.builder(PREFIX + "_maintenance_failures_total")
                        .description("Total number of failed maintenance operations")
                        .register(registry);

        // Policy Metrics

        this.policiesCreatedTotal =
                Counter.builder(PREFIX + "_policies_created_total")
                        .description("Total number of policies created")
                        .register(registry);

        this.policiesUpdatedTotal =
                Counter.builder(PREFIX + "_policies_updated_total")
                        .description("Total number of policies updated")
                        .register(registry);

        this.policiesDeletedTotal =
                Counter.builder(PREFIX + "_policies_deleted_total")
                        .description("Total number of policies deleted")
                        .register(registry);

        this.policyMatchesTotal =
                Counter.builder(PREFIX + "_policy_matches_total")
                        .description("Total number of successful policy matches")
                        .register(registry);

        this.policyNoMatchTotal =
                Counter.builder(PREFIX + "_policy_no_match_total")
                        .description("Total number of tables with no matching policy")
                        .register(registry);

        // Authentication Metrics

        this.authSuccessTotal =
                Counter.builder(PREFIX + "_auth_success_total")
                        .description("Total number of successful authentications")
                        .register(registry);

        this.authFailureTotal =
                Counter.builder(PREFIX + "_auth_failure_total")
                        .description("Total number of failed authentications")
                        .register(registry);

        // Trigger Evaluation Metrics

        this.triggerEvaluationsTotal =
                Counter.builder(PREFIX + "_trigger_evaluations_total")
                        .description("Total number of trigger condition evaluations")
                        .register(registry);

        this.triggerConditionsMetTotal =
                Counter.builder(PREFIX + "_trigger_conditions_met_total")
                        .description("Total evaluations where trigger conditions were met")
                        .register(registry);

        this.triggerConditionsNotMetTotal =
                Counter.builder(PREFIX + "_trigger_conditions_not_met_total")
                        .description("Total evaluations where trigger conditions were not met")
                        .register(registry);

        this.triggerBlockedByIntervalTotal =
                Counter.builder(PREFIX + "_trigger_blocked_by_interval_total")
                        .description("Total evaluations blocked by minimum interval")
                        .register(registry);

        this.triggerForcedByCriticalPipelineTotal =
                Counter.builder(PREFIX + "_trigger_forced_by_critical_pipeline_total")
                        .description("Total evaluations forced by critical pipeline deadline")
                        .register(registry);

        // Gauges for Current State

        registry.gauge(PREFIX + "_active_policies", activePoliciesCount);
        registry.gauge(PREFIX + "_running_operations", runningOperationsCount);
    }

    public void recordMaintenanceTriggered() {
        maintenanceTriggersTotal.increment();
    }

    public void recordMaintenanceResult(OperationStatus status) {
        switch (status) {
            case SUCCESS -> maintenanceSuccessTotal.increment();
            case FAILED, PARTIAL_FAILURE -> maintenanceFailuresTotal.increment();
            default -> {} // RUNNING, PENDING, etc. are not terminal states
        }
    }

    public void recordOperationExecution(
            MaintenanceOperation.Type operationType, ExecutionStatus status, Duration duration) {
        String opName = operationType.name().toLowerCase(Locale.ROOT);

        // Increment counter by status
        String counterKey = opName + "_" + status.name().toLowerCase(Locale.ROOT);
        operationCounters
                .computeIfAbsent(
                        counterKey,
                        k ->
                                Counter.builder(PREFIX + "_operation_executions_total")
                                        .description("Total number of operation executions")
                                        .tag("operation", opName)
                                        .tag("status", status.name().toLowerCase(Locale.ROOT))
                                        .register(registry))
                .increment();

        // Record timing
        String timerKey = opName;
        operationTimers
                .computeIfAbsent(
                        timerKey,
                        k ->
                                Timer.builder(PREFIX + "_operation_duration_seconds")
                                        .description("Duration of maintenance operations")
                                        .tag("operation", opName)
                                        .register(registry))
                .record(duration);
    }

    public <T> T timeOperation(MaintenanceOperation.Type operationType, Supplier<T> execution) {
        String opName = operationType.name().toLowerCase(Locale.ROOT);
        Timer timer =
                operationTimers.computeIfAbsent(
                        opName,
                        k ->
                                Timer.builder(PREFIX + "_operation_duration_seconds")
                                        .description("Duration of maintenance operations")
                                        .tag("operation", opName)
                                        .register(registry));
        return timer.record(execution);
    }

    public void incrementRunningOperations() {
        runningOperationsCount.incrementAndGet();
    }

    public void decrementRunningOperations() {
        runningOperationsCount.decrementAndGet();
    }

    public void recordPolicyCreated() {
        policiesCreatedTotal.increment();
    }

    public void recordPolicyUpdated() {
        policiesUpdatedTotal.increment();
    }

    public void recordPolicyDeleted() {
        policiesDeletedTotal.increment();
    }

    public void recordPolicyMatch(String policyName) {
        policyMatchesTotal.increment();
        // Also track by policy name
        Counter.builder(PREFIX + "_policy_match_by_name_total")
                .description("Policy matches by policy name")
                .tag("policy", policyName)
                .register(registry)
                .increment();
    }

    public void recordNoMatchingPolicy() {
        policyNoMatchTotal.increment();
    }

    public void setActivePoliciesCount(long count) {
        activePoliciesCount.set(count);
    }

    public void recordAuthSuccess(String provider) {
        authSuccessTotal.increment();
        Counter.builder(PREFIX + "_auth_by_provider_total")
                .description("Authentication attempts by provider")
                .tag("provider", provider)
                .tag("result", "success")
                .register(registry)
                .increment();
    }

    public void recordAuthFailure(String provider, String reason) {
        authFailureTotal.increment();
        Counter.builder(PREFIX + "_auth_by_provider_total")
                .description("Authentication attempts by provider")
                .tag("provider", provider)
                .tag("result", "failure")
                .register(registry)
                .increment();

        Counter.builder(PREFIX + "_auth_failure_reason_total")
                .description("Authentication failures by reason")
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    /**
     * Record an API request with timing.
     *
     * @param endpoint The API endpoint
     * @param method The HTTP method
     * @param statusCode The response status code
     * @param duration The request duration
     */
    public void recordApiRequest(
            String endpoint, String method, int statusCode, Duration duration) {
        String status = statusCode >= 200 && statusCode < 400 ? "success" : "error";

        Timer.builder(PREFIX + "_api_request_duration_seconds")
                .description("API request duration")
                .tag("endpoint", endpoint)
                .tag("method", method)
                .tag("status", status)
                .register(registry)
                .record(duration);

        Counter.builder(PREFIX + "_api_requests_total")
                .description("Total API requests")
                .tag("endpoint", endpoint)
                .tag("method", method)
                .tag("status_code", String.valueOf(statusCode))
                .register(registry)
                .increment();
    }

    public MeterRegistry getRegistry() {
        return registry;
    }

    /**
     * Record a trigger condition evaluation.
     *
     * @param operationType the operation type being evaluated
     * @param conditionsMet whether conditions were met (should trigger)
     * @param blockedByInterval whether blocked by minimum interval
     * @param forcedByCriticalPipeline whether forced by critical pipeline deadline
     */
    public void recordTriggerEvaluation(
            String operationType,
            boolean conditionsMet,
            boolean blockedByInterval,
            boolean forcedByCriticalPipeline) {
        triggerEvaluationsTotal.increment();

        // Track by operation type
        Counter.builder(PREFIX + "_trigger_evaluations_by_operation_total")
                .description("Trigger evaluations by operation type")
                .tag("operation", operationType.toLowerCase(Locale.ROOT))
                .tag("result", conditionsMet ? "triggered" : "skipped")
                .register(registry)
                .increment();

        if (conditionsMet) {
            triggerConditionsMetTotal.increment();
        } else {
            triggerConditionsNotMetTotal.increment();
        }

        if (blockedByInterval) {
            triggerBlockedByIntervalTotal.increment();
        }

        if (forcedByCriticalPipeline) {
            triggerForcedByCriticalPipelineTotal.increment();
        }
    }

    /**
     * Record a trigger evaluation that resulted in triggering.
     *
     * @param operationType the operation type
     * @param forcedByCriticalPipeline whether forced by critical pipeline
     */
    public void recordTriggerConditionsMet(String operationType, boolean forcedByCriticalPipeline) {
        recordTriggerEvaluation(operationType, true, false, forcedByCriticalPipeline);
    }

    /**
     * Record a trigger evaluation that was blocked by minimum interval.
     *
     * @param operationType the operation type
     */
    public void recordTriggerBlockedByInterval(String operationType) {
        recordTriggerEvaluation(operationType, false, true, false);
    }

    /**
     * Record a trigger evaluation where conditions were not met.
     *
     * @param operationType the operation type
     */
    public void recordTriggerConditionsNotMet(String operationType) {
        recordTriggerEvaluation(operationType, false, false, false);
    }
}
