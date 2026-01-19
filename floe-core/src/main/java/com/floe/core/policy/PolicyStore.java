package com.floe.core.policy;

import com.floe.core.catalog.TableIdentifier;
import java.util.List;
import java.util.Optional;

/**
 * Storage abstraction for maintenance policies. Implementations can persist policies to various
 * backends: - In-memory (for testing and simple deployments) - Database (for production
 * persistence) - Configuration files (for GitOps workflows)
 */
public interface PolicyStore {
    Optional<MaintenancePolicy> getById(String id);

    Optional<MaintenancePolicy> getByName(String name);

    /** List all policies (no pagination). */
    List<MaintenancePolicy> listAll();

    /** List all policies with pagination. */
    List<MaintenancePolicy> listAll(int limit, int offset);

    /** List enabled policies (no pagination). */
    List<MaintenancePolicy> listEnabled();

    /** List enabled policies with pagination. */
    List<MaintenancePolicy> listEnabled(int limit, int offset);

    /** Count enabled policies. */
    int countEnabled();

    List<MaintenancePolicy> findMatchingPolicies(String catalog, TableIdentifier tableId);

    Optional<MaintenancePolicy> findEffectivePolicy(String catalog, TableIdentifier tableId);

    Optional<MaintenancePolicy> findByName(String name);

    List<MaintenancePolicy> findByPattern(TablePattern pattern);

    void save(MaintenancePolicy policy);

    void saveAll(List<MaintenancePolicy> policies);

    boolean deleteById(String id);

    boolean deleteByPattern(TablePattern pattern);

    boolean existsByName(String name);

    boolean existsById(String id);

    int count();

    void clear();
}
