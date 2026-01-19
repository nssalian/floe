package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.catalog.TableIdentifier;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryPolicyStoreTest {

    private InMemoryPolicyStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryPolicyStore();
    }

    @Test
    void saveAndRetrieveById() {
        MaintenancePolicy policy = createPolicy("test-policy", "prod.analytics.*", 10);

        store.save(policy);

        Optional<MaintenancePolicy> retrieved = store.getById(policy.id());
        assertTrue(retrieved.isPresent());
        assertEquals("test-policy", retrieved.get().name());
    }

    @Test
    void saveAndRetrieveByName() {
        MaintenancePolicy policy = createPolicy("test-policy", "prod.analytics.*", 10);

        store.save(policy);

        Optional<MaintenancePolicy> retrieved = store.getByName("test-policy");
        assertTrue(retrieved.isPresent());
        assertEquals(policy.id(), retrieved.get().id());
    }

    @Test
    void updateExistingPolicy() {
        MaintenancePolicy original = createPolicy("test-policy", "prod.analytics.*", 10);
        store.save(original);

        MaintenancePolicy updated =
                MaintenancePolicy.builder()
                        .id(original.id())
                        .name("test-policy")
                        .tablePattern(TablePattern.parse("prod.streaming.*"))
                        .priority(20)
                        .build();
        store.save(updated);

        assertEquals(1, store.count());
        Optional<MaintenancePolicy> retrieved = store.getById(original.id());
        assertTrue(retrieved.isPresent());
        assertEquals(20, retrieved.get().priority());
    }

    @Test
    void rejectDuplicateName() {
        MaintenancePolicy policy1 = createPolicy("duplicate-name", "prod.*.*", 10);
        MaintenancePolicy policy2 =
                MaintenancePolicy.builder()
                        .name("duplicate-name") // Same name, different ID
                        .tablePattern(TablePattern.parse("dev.*.*"))
                        .build();

        store.save(policy1);

        assertThrows(IllegalArgumentException.class, () -> store.save(policy2));
    }

    @Test
    void allowRenamePolicy() {
        MaintenancePolicy original = createPolicy("original-name", "prod.*.*", 10);
        store.save(original);

        MaintenancePolicy renamed =
                MaintenancePolicy.builder()
                        .id(original.id())
                        .name("new-name")
                        .tablePattern(original.tablePattern())
                        .priority(original.priority())
                        .build();
        store.save(renamed);

        assertFalse(store.getByName("original-name").isPresent());
        assertTrue(store.getByName("new-name").isPresent());
    }

    @Test
    void listAll() {
        store.save(createPolicy("policy1", "prod.*.*", 10));
        store.save(createPolicy("policy2", "dev.*.*", 5));
        store.save(createPolicy("policy3", "staging.*.*", 1));

        List<MaintenancePolicy> all = store.listAll();
        assertEquals(3, all.size());
    }

    @Test
    void listEnabled() {
        MaintenancePolicy enabled = createPolicy("enabled", "prod.*.*", 10);
        MaintenancePolicy disabled =
                MaintenancePolicy.builder()
                        .name("disabled")
                        .tablePattern(TablePattern.parse("dev.*.*"))
                        .enabled(false)
                        .build();

        store.save(enabled);
        store.save(disabled);

        List<MaintenancePolicy> enabledPolicies = store.listEnabled();
        assertEquals(1, enabledPolicies.size());
        assertEquals("enabled", enabledPolicies.get(0).name());
    }

    @Test
    void findMatchingPolicies() {
        store.save(createPolicy("global", "*.*.*", 0));
        store.save(createPolicy("prod-all", "prod.*.*", 10));
        store.save(createPolicy("prod-analytics", "prod.analytics.*", 20));
        store.save(createPolicy("dev-all", "dev.*.*", 10));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        List<MaintenancePolicy> matching = store.findMatchingPolicies("prod", tableId);

        assertEquals(3, matching.size());
        // Should be sorted by priority: prod-analytics (20), prod-all (10), global (0)
        assertEquals("prod-analytics", matching.get(0).name());
        assertEquals("prod-all", matching.get(1).name());
        assertEquals("global", matching.get(2).name());
    }

    @Test
    void findEffectivePolicy() {
        store.save(createPolicy("global", "*.*.*", 0));
        store.save(createPolicy("prod-all", "prod.*.*", 10));
        store.save(createPolicy("prod-analytics", "prod.analytics.*", 20));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        Optional<MaintenancePolicy> effective = store.findEffectivePolicy("prod", tableId);

        assertTrue(effective.isPresent());
        assertEquals("prod-analytics", effective.get().name());
    }

    @Test
    void findEffectivePolicyBySpecificity() {
        // Same priority, different specificity
        store.save(createPolicyWithPriority("catalog-level", "prod.*.*", 10));
        store.save(createPolicyWithPriority("namespace-level", "prod.analytics.*", 10));
        store.save(createPolicyWithPriority("table-level", "prod.analytics.events", 10));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        Optional<MaintenancePolicy> effective = store.findEffectivePolicy("prod", tableId);

        assertTrue(effective.isPresent());
        assertEquals("table-level", effective.get().name());
    }

    @Test
    void delete() {
        MaintenancePolicy policy = createPolicy("to-delete", "prod.*.*", 10);
        store.save(policy);

        assertTrue(store.existsById(policy.id()));
        assertTrue(store.deleteById(policy.id()));
        assertFalse(store.existsById(policy.id()));
        assertFalse(store.existsByName("to-delete"));
    }

    @Test
    void deleteNonExistent() {
        assertFalse(store.deleteByPattern(TablePattern.parse("non-existent-id")));
    }

    @Test
    void clear() {
        store.save(createPolicy("policy1", "prod.*.*", 10));
        store.save(createPolicy("policy2", "dev.*.*", 5));

        assertEquals(2, store.count());

        store.clear();

        assertEquals(0, store.count());
    }

    @Test
    void saveAll() {
        List<MaintenancePolicy> policies =
                List.of(
                        createPolicy("policy1", "prod.*.*", 10),
                        createPolicy("policy2", "dev.*.*", 5),
                        createPolicy("policy3", "staging.*.*", 1));

        store.saveAll(policies);

        assertEquals(3, store.count());
    }

    private MaintenancePolicy createPolicy(String name, String pattern, int priority) {
        return MaintenancePolicy.builder()
                .name(name)
                .tablePattern(TablePattern.parse(pattern))
                .priority(priority)
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .rewriteDataFilesSchedule(ScheduleConfig.defaults())
                .build();
    }

    private MaintenancePolicy createPolicyWithPriority(String name, String pattern, int priority) {
        return MaintenancePolicy.builder()
                .name(name)
                .tablePattern(TablePattern.parse(pattern))
                .priority(priority)
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .rewriteDataFilesSchedule(ScheduleConfig.defaults())
                .build();
    }
}
