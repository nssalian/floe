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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** In-memory implementation of PolicyStore for testing and simple deployments. */
public class InMemoryPolicyStore implements PolicyStore {

    private final ConcurrentHashMap<String, MaintenancePolicy> policies = new ConcurrentHashMap<>();

    @Override
    public Optional<MaintenancePolicy> getById(String id) {
        return Optional.ofNullable(policies.get(id));
    }

    @Override
    public Optional<MaintenancePolicy> getByName(String name) {
        return policies.values().stream().filter(p -> p.name().equals(name)).findFirst();
    }

    @Override
    public Optional<MaintenancePolicy> findByName(String name) {
        return getByName(name);
    }

    @Override
    public List<MaintenancePolicy> listAll() {
        return policies.values().stream()
                .sorted(
                        Comparator.comparingInt(MaintenancePolicy::priority)
                                .reversed()
                                .thenComparing(
                                        MaintenancePolicy::createdAt,
                                        Comparator.nullsLast(Comparator.reverseOrder())))
                .collect(Collectors.toList());
    }

    @Override
    public List<MaintenancePolicy> listAll(int limit, int offset) {
        return policies.values().stream()
                .sorted(
                        Comparator.comparingInt(MaintenancePolicy::priority)
                                .reversed()
                                .thenComparing(
                                        MaintenancePolicy::createdAt,
                                        Comparator.nullsLast(Comparator.reverseOrder())))
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<MaintenancePolicy> listEnabled() {
        return policies.values().stream()
                .filter(p -> Boolean.TRUE.equals(p.enabled()))
                .sorted(
                        Comparator.comparingInt(MaintenancePolicy::priority)
                                .reversed()
                                .thenComparing(
                                        MaintenancePolicy::createdAt,
                                        Comparator.nullsLast(Comparator.reverseOrder())))
                .collect(Collectors.toList());
    }

    @Override
    public List<MaintenancePolicy> listEnabled(int limit, int offset) {
        return policies.values().stream()
                .filter(p -> Boolean.TRUE.equals(p.enabled()))
                .sorted(
                        Comparator.comparingInt(MaintenancePolicy::priority)
                                .reversed()
                                .thenComparing(
                                        MaintenancePolicy::createdAt,
                                        Comparator.nullsLast(Comparator.reverseOrder())))
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public int countEnabled() {
        return (int)
                policies.values().stream().filter(p -> Boolean.TRUE.equals(p.enabled())).count();
    }

    @Override
    public List<MaintenancePolicy> findByPattern(TablePattern pattern) {
        return listAll().stream()
                .filter(policy -> patternsOverlap(pattern, policy.tablePattern()))
                .collect(Collectors.toList());
    }

    private boolean patternsOverlap(TablePattern query, TablePattern policy) {
        return (componentOverlaps(query.catalogPattern(), policy.catalogPattern())
                && componentOverlaps(query.namespacePattern(), policy.namespacePattern())
                && componentOverlaps(query.tablePattern(), policy.tablePattern()));
    }

    private boolean componentOverlaps(String query, String policy) {
        if (query == null || policy == null) return true;
        if (query.contains("*") || policy.contains("*")) return true;
        return query.equals(policy);
    }

    @Override
    public void save(MaintenancePolicy policy) {
        Objects.requireNonNull(policy, "Policy cannot be null");
        Objects.requireNonNull(policy.id(), "Policy ID cannot be null");
        Objects.requireNonNull(policy.name(), "Policy name cannot be null");

        // Check for duplicate names (excluding self)
        Optional<MaintenancePolicy> existing = getByName(policy.name());
        if (existing.isPresent() && !existing.get().id().equals(policy.id())) {
            throw new IllegalArgumentException(
                    "A policy with name '" + policy.name() + "' already exists");
        }

        policies.put(policy.id(), policy);
    }

    @Override
    public void saveAll(List<MaintenancePolicy> policiesToSave) {
        for (MaintenancePolicy policy : policiesToSave) {
            save(policy);
        }
    }

    @Override
    public boolean deleteById(String id) {
        return policies.remove(id) != null;
    }

    @Override
    public boolean deleteByPattern(TablePattern pattern) {
        List<MaintenancePolicy> toDelete = findByPattern(pattern);
        boolean deleted = false;
        for (MaintenancePolicy policy : toDelete) {
            if (deleteById(policy.id())) {
                deleted = true;
            }
        }
        return deleted;
    }

    @Override
    public boolean existsByName(String name) {
        return getByName(name).isPresent();
    }

    @Override
    public boolean existsById(String id) {
        return policies.containsKey(id);
    }

    @Override
    public int count() {
        return policies.size();
    }

    @Override
    public void clear() {
        policies.clear();
    }

    @Override
    public String toString() {
        return String.format("InMemoryPolicyStore[count=%d]", count());
    }
}
