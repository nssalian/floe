package com.floe.server.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.floe.core.policy.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.*;

/** Integration tests for PostgresPolicyStore */
@Tag("integration")
@DisplayName("PostgresPolicyStore Integration Tests")
class PostgresPolicyStoreIT extends PostgresTestBase {

    private PostgresPolicyStore store;

    @BeforeEach
    void setUp() {
        store = new PostgresPolicyStore(getDataSource());
        store.initializeSchema();
        store.clear();
    }

    @Nested
    @DisplayName("save and retrieve")
    class SaveAndRetrieve {

        @Test
        @DisplayName("should save and retrieve policy by ID")
        void shouldSaveAndRetrieveById() {
            MaintenancePolicy policy = createTestPolicy("test-policy-1");

            store.save(policy);
            Optional<MaintenancePolicy> retrieved = store.getById(policy.id());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().id()).isEqualTo(policy.id());
            assertThat(retrieved.get().name()).isEqualTo(policy.name());
            assertThat(retrieved.get().enabled()).isTrue();
        }

        @Test
        @DisplayName("should save and retrieve policy by name")
        void shouldSaveAndRetrieveByName() {
            MaintenancePolicy policy = createTestPolicy("unique-name");

            store.save(policy);
            Optional<MaintenancePolicy> retrieved = store.getByName("unique-name");

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().name()).isEqualTo("unique-name");
        }

        @Test
        @DisplayName("should return empty when policy not found")
        void shouldReturnEmptyWhenNotFound() {
            Optional<MaintenancePolicy> retrieved = store.getById("non-existent-id");

            assertThat(retrieved).isEmpty();
        }

        @Test
        @DisplayName("should update existing policy on save")
        void shouldUpdateExistingPolicy() {
            MaintenancePolicy original = createTestPolicy("update-test");
            store.save(original);

            MaintenancePolicy updated =
                    new MaintenancePolicy(
                            original.id(),
                            original.name(),
                            "Updated description",
                            original.tablePattern(),
                            false,
                            original.rewriteDataFiles(),
                            original.rewriteDataFilesSchedule(),
                            original.expireSnapshots(),
                            original.expireSnapshotsSchedule(),
                            original.orphanCleanup(),
                            original.orphanCleanupSchedule(),
                            original.rewriteManifests(),
                            original.rewriteManifestsSchedule(),
                            99,
                            null, // healthThresholds
                            null, // triggerConditions
                            original.tags(),
                            original.createdAt(),
                            Instant.now());

            store.save(updated);
            Optional<MaintenancePolicy> retrieved = store.getById(original.id());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().description()).isEqualTo("Updated description");
            assertThat(retrieved.get().enabled()).isFalse();
            assertThat(retrieved.get().priority()).isEqualTo(99);
        }

        @Test
        @DisplayName("should reject duplicate policy names")
        void shouldRejectDuplicateNames() {
            MaintenancePolicy first = createTestPolicy("duplicate-name");
            store.save(first);

            MaintenancePolicy second =
                    new MaintenancePolicy(
                            UUID.randomUUID().toString(),
                            "duplicate-name",
                            "Different policy",
                            TablePattern.parse("catalog.namespace.table2"),
                            true,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            0,
                            null, // healthThresholds
                            null, // triggerConditions
                            Map.of(),
                            Instant.now(),
                            Instant.now());

            assertThatThrownBy(() -> store.save(second))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("already exists");
        }
    }

    @Nested
    @DisplayName("list operations")
    class ListOperations {

        @Test
        @DisplayName("should list all policies")
        void shouldListAllPolicies() {
            store.save(createTestPolicy("policy-1"));
            store.save(createTestPolicy("policy-2"));
            store.save(createTestPolicy("policy-3"));

            List<MaintenancePolicy> all = store.listAll();

            assertThat(all).hasSize(3);
        }

        @Test
        @DisplayName("should list all with pagination")
        void shouldListAllWithPagination() {
            for (int i = 0; i < 10; i++) {
                store.save(createTestPolicy("policy-" + i));
            }

            List<MaintenancePolicy> page1 = store.listAll(3, 0);
            List<MaintenancePolicy> page2 = store.listAll(3, 3);
            List<MaintenancePolicy> page3 = store.listAll(3, 6);
            List<MaintenancePolicy> page4 = store.listAll(3, 9);

            assertThat(page1).hasSize(3);
            assertThat(page2).hasSize(3);
            assertThat(page3).hasSize(3);
            assertThat(page4).hasSize(1);
        }

        @Test
        @DisplayName("should list only enabled policies")
        void shouldListEnabledPolicies() {
            store.save(createTestPolicy("enabled-1", true));
            store.save(createTestPolicy("enabled-2", true));
            store.save(createTestPolicy("disabled-1", false));

            List<MaintenancePolicy> enabled = store.listEnabled();

            assertThat(enabled).hasSize(2);
            assertThat(enabled).allMatch(MaintenancePolicy::enabled);
        }

        @Test
        @DisplayName("should count enabled policies")
        void shouldCountEnabled() {
            store.save(createTestPolicy("enabled-1", true));
            store.save(createTestPolicy("enabled-2", true));
            store.save(createTestPolicy("disabled-1", false));

            int count = store.countEnabled();

            assertThat(count).isEqualTo(2);
        }

        @Test
        @DisplayName("should count all policies")
        void shouldCountAll() {
            store.save(createTestPolicy("policy-1"));
            store.save(createTestPolicy("policy-2"));

            int count = store.count();

            assertThat(count).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("delete operations")
    class DeleteOperations {

        @Test
        @DisplayName("should delete policy by ID")
        void shouldDeleteById() {
            MaintenancePolicy policy = createTestPolicy("to-delete");
            store.save(policy);

            boolean deleted = store.deleteById(policy.id());

            assertThat(deleted).isTrue();
            assertThat(store.getById(policy.id())).isEmpty();
        }

        @Test
        @DisplayName("should return false when deleting non-existent policy")
        void shouldReturnFalseWhenDeletingNonExistent() {
            boolean deleted = store.deleteById("non-existent");

            assertThat(deleted).isFalse();
        }

        @Test
        @DisplayName("should clear all policies")
        void shouldClearAll() {
            store.save(createTestPolicy("policy-1"));
            store.save(createTestPolicy("policy-2"));

            store.clear();

            assertThat(store.count()).isZero();
        }
    }

    @Nested
    @DisplayName("existence checks")
    class ExistenceChecks {

        @Test
        @DisplayName("should check existence by ID")
        void shouldCheckExistsById() {
            MaintenancePolicy policy = createTestPolicy("exists-check");
            store.save(policy);

            assertThat(store.existsById(policy.id())).isTrue();
            assertThat(store.existsById("non-existent")).isFalse();
        }

        @Test
        @DisplayName("should check existence by name")
        void shouldCheckExistsByName() {
            store.save(createTestPolicy("exists-by-name"));

            assertThat(store.existsByName("exists-by-name")).isTrue();
            assertThat(store.existsByName("non-existent")).isFalse();
        }
    }

    @Nested
    @DisplayName("configuration persistence")
    class ConfigurationPersistence {

        @Test
        @DisplayName("should persist rewrite data files config")
        void shouldPersistRewriteDataFilesConfig() {
            RewriteDataFilesConfig config =
                    RewriteDataFilesConfig.builder()
                            .strategy("BINPACK")
                            .targetFileSizeBytes(268435456L)
                            .maxFileGroupSizeBytes(536870912L)
                            .maxConcurrentFileGroupRewrites(2)
                            .build();

            MaintenancePolicy policy =
                    createTestPolicyWithConfigs("rdf-config", config, null, null, null);
            store.save(policy);

            Optional<MaintenancePolicy> retrieved = store.getById(policy.id());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().rewriteDataFiles()).isNotNull();
            assertThat(retrieved.get().rewriteDataFiles().targetFileSizeBytes())
                    .isEqualTo(268435456L);
            assertThat(retrieved.get().rewriteDataFiles().maxConcurrentFileGroupRewrites())
                    .isEqualTo(2);
        }

        @Test
        @DisplayName("should persist expire snapshots config")
        void shouldPersistExpireSnapshotsConfig() {
            ExpireSnapshotsConfig config =
                    ExpireSnapshotsConfig.builder()
                            .retainLast(10)
                            .maxSnapshotAge(java.time.Duration.ofDays(7))
                            .build();

            MaintenancePolicy policy =
                    createTestPolicyWithConfigs("es-config", null, config, null, null);
            store.save(policy);

            Optional<MaintenancePolicy> retrieved = store.getById(policy.id());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().expireSnapshots()).isNotNull();
            assertThat(retrieved.get().expireSnapshots().retainLast()).isEqualTo(10);
            assertThat(retrieved.get().expireSnapshots().maxSnapshotAge())
                    .isEqualTo(java.time.Duration.ofDays(7));
        }

        @Test
        @DisplayName("should persist orphan cleanup config")
        void shouldPersistOrphanCleanupConfig() {
            OrphanCleanupConfig config =
                    OrphanCleanupConfig.builder()
                            .retentionPeriodInDays(java.time.Duration.ofDays(3))
                            .build();

            MaintenancePolicy policy =
                    createTestPolicyWithConfigs("oc-config", null, null, config, null);
            store.save(policy);

            Optional<MaintenancePolicy> retrieved = store.getById(policy.id());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().orphanCleanup()).isNotNull();
            assertThat(retrieved.get().orphanCleanup().retentionPeriodInDays())
                    .isEqualTo(java.time.Duration.ofDays(3));
        }

        @Test
        @DisplayName("should persist schedule config")
        void shouldPersistScheduleConfig() {
            ScheduleConfig schedule =
                    ScheduleConfig.builder().cronExpression("0 2 * * *").enabled(true).build();

            MaintenancePolicy policy =
                    new MaintenancePolicy(
                            UUID.randomUUID().toString(),
                            "scheduled-policy",
                            "Policy with schedule",
                            TablePattern.parse("catalog.ns.table"),
                            true,
                            RewriteDataFilesConfig.defaults(),
                            schedule,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            0,
                            null, // healthThresholds
                            null, // triggerConditions
                            Map.of(),
                            Instant.now(),
                            Instant.now());

            store.save(policy);
            Optional<MaintenancePolicy> retrieved = store.getById(policy.id());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().rewriteDataFilesSchedule()).isNotNull();
            assertThat(retrieved.get().rewriteDataFilesSchedule().cronExpression())
                    .isEqualTo("0 2 * * *");
            assertThat(retrieved.get().rewriteDataFilesSchedule().enabled()).isTrue();
        }

        @Test
        @DisplayName("should persist tags")
        void shouldPersistTags() {
            Map<String, String> tags = Map.of("env", "production", "team", "data-platform");

            MaintenancePolicy policy =
                    new MaintenancePolicy(
                            UUID.randomUUID().toString(),
                            "tagged-policy",
                            null,
                            TablePattern.parse("catalog.ns.table"),
                            true,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            0,
                            null, // healthThresholds
                            null, // triggerConditions
                            tags,
                            Instant.now(),
                            Instant.now());

            store.save(policy);
            Optional<MaintenancePolicy> retrieved = store.getById(policy.id());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().tags()).containsEntry("env", "production");
            assertThat(retrieved.get().tags()).containsEntry("team", "data-platform");
        }
    }

    @Nested
    @DisplayName("batch operations")
    class BatchOperations {

        @Test
        @DisplayName("should save all policies in batch")
        void shouldSaveAllInBatch() {
            List<MaintenancePolicy> policies =
                    List.of(
                            createTestPolicy("batch-1"),
                            createTestPolicy("batch-2"),
                            createTestPolicy("batch-3"));

            store.saveAll(policies);

            assertThat(store.count()).isEqualTo(3);
            assertThat(store.getByName("batch-1")).isPresent();
            assertThat(store.getByName("batch-2")).isPresent();
            assertThat(store.getByName("batch-3")).isPresent();
        }
    }

    @Nested
    @DisplayName("ordering")
    class Ordering {

        @Test
        @DisplayName("should order by priority descending")
        void shouldOrderByPriorityDescending() {
            store.save(createTestPolicyWithPriority("low-priority", 1));
            store.save(createTestPolicyWithPriority("high-priority", 100));
            store.save(createTestPolicyWithPriority("medium-priority", 50));

            List<MaintenancePolicy> all = store.listAll();

            assertThat(all.get(0).name()).isEqualTo("high-priority");
            assertThat(all.get(1).name()).isEqualTo("medium-priority");
            assertThat(all.get(2).name()).isEqualTo("low-priority");
        }
    }

    // Helper Methods

    private MaintenancePolicy createTestPolicy(String name) {
        return createTestPolicy(name, true);
    }

    private MaintenancePolicy createTestPolicy(String name, boolean enabled) {
        return new MaintenancePolicy(
                UUID.randomUUID().toString(),
                name,
                "Test policy: " + name,
                TablePattern.parse("test_catalog.test_namespace.*"),
                enabled,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                0,
                null, // healthThresholds
                null, // triggerConditions
                Map.of(),
                Instant.now(),
                Instant.now());
    }

    private MaintenancePolicy createTestPolicyWithPriority(String name, int priority) {
        return new MaintenancePolicy(
                UUID.randomUUID().toString(),
                name,
                "Test policy: " + name,
                TablePattern.parse("test_catalog.test_namespace.*"),
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                priority,
                null, // healthThresholds
                null, // triggerConditions
                Map.of(),
                Instant.now(),
                Instant.now());
    }

    private MaintenancePolicy createTestPolicyWithConfigs(
            String name,
            RewriteDataFilesConfig rdf,
            ExpireSnapshotsConfig es,
            OrphanCleanupConfig oc,
            RewriteManifestsConfig rm) {
        return new MaintenancePolicy(
                UUID.randomUUID().toString(),
                name,
                "Test policy with configs",
                TablePattern.parse("catalog.namespace.table"),
                true,
                rdf,
                null,
                es,
                null,
                oc,
                null,
                rm,
                null,
                0,
                null, // healthThresholds
                null, // triggerConditions
                Map.of(),
                Instant.now(),
                Instant.now());
    }
}
