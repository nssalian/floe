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

package com.floe.core.policy;

import com.floe.core.catalog.TableIdentifier;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves which maintenance policy applies to a given table.
 *
 * <p>Policy resolution rules: 1. Only enabled policies are considered 2. Pattern must match the
 * table's catalog, namespace, and name 3. Higher priority policies win 4. If priorities are equal,
 * more specific patterns win 5. If still tied, policies are merged (more specific overrides less
 * specific)
 */
public class PolicyMatcher {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyMatcher.class);
    private final PolicyStore policyStore;

    /**
     * Create a PolicyMatcher with the given policy store.
     *
     * @param policyStore the policy store to query for policies
     * @throws NullPointerException if policyStore is null
     */
    public PolicyMatcher(PolicyStore policyStore) {
        this.policyStore = Objects.requireNonNull(policyStore, "PolicyStore cannot be null");
    }

    /**
     * Find the single most applicable policy for a table.
     *
     * @param catalog the catalog name
     * @param tableId the table identifier
     * @return the effective policy if found, empty otherwise
     */
    public Optional<MaintenancePolicy> findEffectivePolicy(
            String catalog, TableIdentifier tableId) {
        return policyStore.findEffectivePolicy(catalog, tableId);
    }

    /**
     * Find all policies that match a table, in priority order.
     *
     * @param catalog the catalog name
     * @param tableId the table identifier
     * @return list of matching policies sorted by priority
     */
    public List<MaintenancePolicy> findAllMatchingPolicies(
            String catalog, TableIdentifier tableId) {
        return policyStore.findMatchingPolicies(catalog, tableId);
    }

    /**
     * Get the effective configuration for a specific operation on a table.
     *
     * <p>This merges configurations from all matching policies, with more specific policies
     * overriding less specific ones for individual settings.
     *
     * @param catalog the catalog name
     * @param tableId the table identifier
     * @param operation the operation type
     * @return the effective configuration if found, empty otherwise
     */
    public Optional<EffectiveConfig> getEffectiveConfig(
            String catalog, TableIdentifier tableId, OperationType operation) {
        List<MaintenancePolicy> matchingPolicies = findAllMatchingPolicies(catalog, tableId);
        if (matchingPolicies.isEmpty()) {
            LOG.debug("No matching policies for {}.{}", catalog, tableId);
            return Optional.empty();
        }

        // Filter to policies that have this operation configured
        List<MaintenancePolicy> applicablePolicies =
                matchingPolicies.stream()
                        .filter(policy -> policy.isOperationEnabled(operation))
                        .toList();

        if (applicablePolicies.isEmpty()) {
            LOG.debug(
                    "No applicable policies for operation {} on {}.{}",
                    operation,
                    catalog,
                    tableId);
            return Optional.empty();
        }

        // Use the highest priority applicable policy as the base
        MaintenancePolicy effectivePolicy = applicablePolicies.getFirst();
        LOG.debug(
                "Effective policy for {} on {}.{}: {} (priority= {}",
                operation,
                catalog,
                tableId,
                effectivePolicy.name(),
                effectivePolicy.effectivePriority());

        return Optional.of(
                new EffectiveConfig(
                        effectivePolicy,
                        operation,
                        getOperationConfig(effectivePolicy, operation),
                        effectivePolicy.getSchedule(operation)));
    }

    /**
     * Get effective configs for all operations on a table.
     *
     * @param catalog the catalog name
     * @param tableId the table identifier
     * @return map of operation type to effective configuration
     */
    public Map<OperationType, EffectiveConfig> getAllEffectiveConfigs(
            String catalog, TableIdentifier tableId) {
        Map<OperationType, EffectiveConfig> configs = new EnumMap<>(OperationType.class);

        for (OperationType operation : OperationType.values()) {
            getEffectiveConfig(catalog, tableId, operation)
                    .ifPresent(config -> configs.put(operation, config));
        }

        return configs;
    }

    /**
     * Check if any maintenance is scheduled for a table.
     *
     * @param catalog the catalog name
     * @param tableId the table identifier
     * @return true if any maintenance operations are configured for this table
     */
    public boolean hasAnyMaintenance(String catalog, TableIdentifier tableId) {
        return !getAllEffectiveConfigs(catalog, tableId).isEmpty();
    }

    /**
     * Get a summary of all tables and their effective policies for UI display and debugging.
     *
     * @param catalog the catalog name
     * @param tables the list of tables to summarize
     * @return list of table policy summaries
     */
    public List<TablePolicySummary> summarizeAllTables(
            String catalog, List<TableIdentifier> tables) {

        return tables.stream()
                .map(
                        tableId -> {
                            Optional<MaintenancePolicy> policy =
                                    findEffectivePolicy(catalog, tableId);
                            Map<OperationType, EffectiveConfig> configs =
                                    getAllEffectiveConfigs(catalog, tableId);
                            return new TablePolicySummary(
                                    catalog,
                                    tableId,
                                    policy.map(MaintenancePolicy::name).orElse(null),
                                    policy.map(MaintenancePolicy::id).orElse(null),
                                    configs.keySet());
                        })
                .toList();
    }

    /**
     * Get the operation-specific configuration from a policy.
     *
     * @param policy the maintenance policy
     * @param operation the operation type
     * @return the configuration object for the operation
     */
    private Object getOperationConfig(MaintenancePolicy policy, OperationType operation) {
        return switch (operation) {
            case REWRITE_DATA_FILES -> policy.rewriteDataFiles();
            case EXPIRE_SNAPSHOTS -> policy.expireSnapshots();
            case ORPHAN_CLEANUP -> policy.orphanCleanup();
            case REWRITE_MANIFESTS -> policy.rewriteManifests();
        };
    }

    /**
     * Represents the effective configuration for an operation on a specific table.
     *
     * @param policy the source policy
     * @param operation the operation type
     * @param operationConfig the operation-specific configuration
     * @param schedule the schedule configuration
     */
    public record EffectiveConfig(
            MaintenancePolicy policy,
            OperationType operation,
            Object operationConfig,
            ScheduleConfig schedule) {

        @SuppressWarnings("unchecked")
        public <T> T config() {
            return (T) operationConfig;
        }

        public RewriteDataFilesConfig rewriteDataFilesConfig() {
            if (operation != OperationType.REWRITE_DATA_FILES) {
                throw new IllegalStateException("Not a compaction operation");
            }
            return (RewriteDataFilesConfig) operationConfig;
        }

        public ExpireSnapshotsConfig expireSnapshotsConfig() {
            if (operation != OperationType.EXPIRE_SNAPSHOTS) {
                throw new IllegalStateException("Not an expire snapshots operation");
            }
            return (ExpireSnapshotsConfig) operationConfig;
        }

        public OrphanCleanupConfig orphanCleanupConfig() {
            if (operation != OperationType.ORPHAN_CLEANUP) {
                throw new IllegalStateException("Not an orphan cleanup operation");
            }
            return (OrphanCleanupConfig) operationConfig;
        }

        public RewriteManifestsConfig rewriteManifestsConfig() {
            if (operation != OperationType.REWRITE_MANIFESTS) {
                throw new IllegalStateException("Not a rewrite manifests operation");
            }
            return (RewriteManifestsConfig) operationConfig;
        }
    }

    /**
     * Summary of a table's policy configuration.
     *
     * @param catalog the catalog name
     * @param tableId the table identifier
     * @param policyName the name of the effective policy, or null if none
     * @param policyId the ID of the effective policy, or null if none
     * @param enabledOperations the set of enabled operation types
     */
    public record TablePolicySummary(
            String catalog,
            TableIdentifier tableId,
            String policyName,
            String policyId,
            Set<OperationType> enabledOperations) {

        public boolean hasPolicy() {
            return policyName != null;
        }

        public String fullyQualifiedName() {
            return catalog + "." + tableId.getNamespace() + "." + tableId.getTableName();
        }
    }
}
