package com.floe.server.api;

import com.floe.core.orchestrator.TriggerEvaluator;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Response from trigger condition evaluation for a table.
 *
 * @param catalog catalog name
 * @param namespace namespace name
 * @param table table name
 * @param policyName name of the matched policy (null if no policy)
 * @param conditionBasedTriggeringEnabled whether condition-based triggering is enabled
 * @param operationStatuses per-operation trigger status
 */
public record TriggerStatusResponse(
        String catalog,
        String namespace,
        String table,
        String policyName,
        boolean conditionBasedTriggeringEnabled,
        Map<String, OperationTriggerStatus> operationStatuses) {

    /**
     * Status of trigger conditions for a specific operation type.
     *
     * @param operationType the operation type
     * @param shouldTrigger whether this operation should trigger
     * @param metConditions conditions that were met
     * @param unmetConditions conditions that were not met
     * @param nextEligibleTime when the operation is next eligible (if blocked by min interval)
     * @param forcedByCriticalPipeline true if would be forced due to critical pipeline deadline
     * @param enabled whether this operation is enabled in the policy
     */
    public record OperationTriggerStatus(
            String operationType,
            boolean shouldTrigger,
            List<String> metConditions,
            List<String> unmetConditions,
            Instant nextEligibleTime,
            boolean forcedByCriticalPipeline,
            boolean enabled) {

        public static OperationTriggerStatus from(
                String operationType, TriggerEvaluator.EvaluationResult result, boolean enabled) {
            return new OperationTriggerStatus(
                    operationType,
                    result.shouldTrigger(),
                    result.metConditions(),
                    result.unmetConditions(),
                    result.nextEligibleTime(),
                    result.forcedByCriticalPipeline(),
                    enabled);
        }

        public static OperationTriggerStatus disabled(String operationType) {
            return new OperationTriggerStatus(
                    operationType,
                    false,
                    List.of(),
                    List.of("Operation disabled in policy"),
                    null,
                    false,
                    false);
        }

        public static OperationTriggerStatus noPolicy(String operationType) {
            return new OperationTriggerStatus(
                    operationType,
                    false,
                    List.of(),
                    List.of("No matching policy"),
                    null,
                    false,
                    false);
        }
    }
}
