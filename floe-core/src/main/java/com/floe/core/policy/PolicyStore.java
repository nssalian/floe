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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Storage abstraction for maintenance policies. Implementations can persist policies to various
 * backends: - In-memory (for testing and simple deployments) - Database (for production
 * persistence) - Configuration files (for GitOps workflows)
 */
public interface PolicyStore {
    /**
     * Get a policy by its ID.
     *
     * @param id the policy ID
     * @return the policy if found, empty otherwise
     */
    Optional<MaintenancePolicy> getById(String id);

    /**
     * Get a policy by its name.
     *
     * @param name the policy name
     * @return the policy if found, empty otherwise
     */
    Optional<MaintenancePolicy> getByName(String name);

    /**
     * List all policies (no pagination).
     *
     * @return list of all policies
     */
    List<MaintenancePolicy> listAll();

    /**
     * List all policies with pagination.
     *
     * @param limit maximum number of policies to return
     * @param offset number of policies to skip
     * @return list of policies
     */
    List<MaintenancePolicy> listAll(int limit, int offset);

    /**
     * List enabled policies (no pagination).
     *
     * @return list of all enabled policies
     */
    List<MaintenancePolicy> listEnabled();

    /**
     * List enabled policies with pagination.
     *
     * @param limit maximum number of policies to return
     * @param offset number of policies to skip
     * @return list of enabled policies
     */
    List<MaintenancePolicy> listEnabled(int limit, int offset);

    /**
     * Count enabled policies.
     *
     * @return number of enabled policies
     */
    int countEnabled();

    /**
     * Find all policies that match a table, in priority order.
     *
     * @param catalog the catalog name
     * @param tableId the table identifier
     * @return list of matching policies, sorted by priority
     */
    default List<MaintenancePolicy> findMatchingPolicies(String catalog, TableIdentifier tableId) {
        return listEnabled().stream()
                .filter(policy -> policy.tablePattern().matches(catalog, tableId))
                .sorted((p1, p2) -> Integer.compare(p2.effectivePriority(), p1.effectivePriority()))
                .collect(Collectors.toList());
    }

    /**
     * Find the single most applicable policy for a table.
     *
     * @param catalog the catalog name
     * @param tableId the table identifier
     * @return the effective policy if found, empty otherwise
     */
    default Optional<MaintenancePolicy> findEffectivePolicy(
            String catalog, TableIdentifier tableId) {
        return listEnabled().stream()
                .filter(policy -> policy.tablePattern().matches(catalog, tableId))
                .max(Comparator.comparingInt(p -> p.tablePattern().specificity()));
    }

    /**
     * Find a policy by its name.
     *
     * @param name the policy name
     * @return the policy if found, empty otherwise
     */
    Optional<MaintenancePolicy> findByName(String name);

    /**
     * Find all policies matching a table pattern.
     *
     * @param pattern the table pattern to match
     * @return list of policies with matching patterns
     */
    List<MaintenancePolicy> findByPattern(TablePattern pattern);

    /**
     * Save a policy.
     *
     * @param policy the policy to save
     */
    void save(MaintenancePolicy policy);

    /**
     * Save multiple policies.
     *
     * @param policies the list of policies to save
     */
    void saveAll(List<MaintenancePolicy> policies);

    /**
     * Delete a policy by ID.
     *
     * @param id the policy ID
     * @return true if the policy was deleted, false if not found
     */
    boolean deleteById(String id);

    /**
     * Delete all policies matching a pattern.
     *
     * @param pattern the table pattern to match
     * @return true if any policies were deleted
     */
    boolean deleteByPattern(TablePattern pattern);

    /**
     * Check if a policy exists by name.
     *
     * @param name the policy name
     * @return true if a policy with this name exists
     */
    boolean existsByName(String name);

    /**
     * Check if a policy exists by ID.
     *
     * @param id the policy ID
     * @return true if a policy with this ID exists
     */
    boolean existsById(String id);

    /**
     * Count total policies in the store.
     *
     * @return total number of policies
     */
    int count();

    /** Clear all policies. For testing only. */
    void clear();
}
