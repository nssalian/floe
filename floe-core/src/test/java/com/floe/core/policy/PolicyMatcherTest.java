package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.catalog.TableIdentifier;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PolicyMatcherTest {

    private InMemoryPolicyStore store;
    private PolicyMatcher matcher;

    @BeforeEach
    void setUp() {
        store = new InMemoryPolicyStore();
        matcher = new PolicyMatcher(store);
    }

    @Test
    void findEffectivePolicyReturnsHighestPriority() {
        store.save(createFullPolicy("global", "*.*.*", 0));
        store.save(createFullPolicy("prod", "prod.*.*", 10));
        store.save(createFullPolicy("prod-analytics", "prod.analytics.*", 20));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        Optional<MaintenancePolicy> effective = matcher.findEffectivePolicy("prod", tableId);

        assertTrue(effective.isPresent());
        assertEquals("prod-analytics", effective.get().name());
    }

    @Test
    void findEffectivePolicyUsesSpecificityAsTiebreaker() {
        // Same priority, different specificity
        store.save(createFullPolicy("catalog-level", "prod.*.*", 10));
        store.save(createFullPolicy("namespace-level", "prod.analytics.*", 10));
        store.save(createFullPolicy("table-level", "prod.analytics.events", 10));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        Optional<MaintenancePolicy> effective = matcher.findEffectivePolicy("prod", tableId);

        assertTrue(effective.isPresent());
        assertEquals("table-level", effective.get().name());
    }

    @Test
    void findEffectivePolicyReturnsEmptyWhenNoMatch() {
        store.save(createFullPolicy("prod-only", "prod.*.*", 10));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        Optional<MaintenancePolicy> effective = matcher.findEffectivePolicy("dev", tableId);

        assertFalse(effective.isPresent());
    }

    @Test
    void findEffectivePolicyIgnoresDisabledPolicies() {
        MaintenancePolicy disabled =
                MaintenancePolicy.builder()
                        .name("disabled-high-priority")
                        .tablePattern(TablePattern.parse("prod.*.*"))
                        .priority(100)
                        .enabled(false)
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(ScheduleConfig.defaults())
                        .build();

        store.save(disabled);
        store.save(createFullPolicy("enabled-low-priority", "prod.*.*", 1));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        Optional<MaintenancePolicy> effective = matcher.findEffectivePolicy("prod", tableId);

        assertTrue(effective.isPresent());
        assertEquals("enabled-low-priority", effective.get().name());
    }

    @Test
    void findAllMatchingPolicies() {
        store.save(createFullPolicy("global", "*.*.*", 0));
        store.save(createFullPolicy("prod", "prod.*.*", 10));
        store.save(createFullPolicy("prod-analytics", "prod.analytics.*", 20));
        store.save(createFullPolicy("dev", "dev.*.*", 10));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        List<MaintenancePolicy> matching = matcher.findAllMatchingPolicies("prod", tableId);

        assertEquals(3, matching.size());
        assertEquals("prod-analytics", matching.get(0).name());
        assertEquals("prod", matching.get(1).name());
        assertEquals("global", matching.get(2).name());
    }

    @Test
    void getEffectiveConfigForRewriteDataFiles() {
        store.save(createFullPolicy("rewrite-policy", "prod.*.*", 10));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        Optional<PolicyMatcher.EffectiveConfig> config =
                matcher.getEffectiveConfig("prod", tableId, OperationType.REWRITE_DATA_FILES);

        assertTrue(config.isPresent());
        assertEquals(OperationType.REWRITE_DATA_FILES, config.get().operation());
        assertNotNull(config.get().rewriteDataFilesConfig());
        assertEquals("rewrite-policy", config.get().policy().name());
    }

    @Test
    void getEffectiveConfigReturnsEmptyForUnconfiguredOperation() {
        // Policy without orphan cleanup
        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .name("rewrite-only")
                        .tablePattern(TablePattern.parse("prod.*.*"))
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(ScheduleConfig.defaults())
                        .build();
        store.save(policy);

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        Optional<PolicyMatcher.EffectiveConfig> config =
                matcher.getEffectiveConfig("prod", tableId, OperationType.ORPHAN_CLEANUP);

        assertFalse(config.isPresent());
    }

    @Test
    void getAllEffectiveConfigs() {
        store.save(createFullPolicy("full-policy", "prod.*.*", 10));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        Map<OperationType, PolicyMatcher.EffectiveConfig> configs =
                matcher.getAllEffectiveConfigs("prod", tableId);

        assertEquals(4, configs.size());
        assertTrue(configs.containsKey(OperationType.REWRITE_DATA_FILES));
        assertTrue(configs.containsKey(OperationType.EXPIRE_SNAPSHOTS));
        assertTrue(configs.containsKey(OperationType.ORPHAN_CLEANUP));
        assertTrue(configs.containsKey(OperationType.REWRITE_MANIFESTS));
    }

    @Test
    void hasAnyMaintenance() {
        store.save(createFullPolicy("prod-policy", "prod.*.*", 10));

        TableIdentifier prodTable = TableIdentifier.of("prod", "analytics", "events");
        TableIdentifier devTable = TableIdentifier.of("prod", "analytics", "events");

        assertTrue(matcher.hasAnyMaintenance("prod", prodTable));
        assertFalse(matcher.hasAnyMaintenance("dev", devTable));
    }

    @Test
    void summarizeAllTables() {
        store.save(createFullPolicy("prod-policy", "prod.*.*", 10));
        store.save(createFullPolicy("dev-policy", "dev.*.*", 5));

        List<TableIdentifier> tables =
                List.of(
                        TableIdentifier.of("prod", "analytics", "events"),
                        TableIdentifier.of("prod", "analytics", "users"),
                        TableIdentifier.of("prod", "raw", "logs"));

        List<PolicyMatcher.TablePolicySummary> summaries =
                matcher.summarizeAllTables("prod", tables);

        assertEquals(3, summaries.size());
        for (PolicyMatcher.TablePolicySummary summary : summaries) {
            assertTrue(summary.hasPolicy());
            assertEquals("prod-policy", summary.policyName());
            assertEquals(
                    Set.of(
                            OperationType.REWRITE_DATA_FILES,
                            OperationType.EXPIRE_SNAPSHOTS,
                            OperationType.ORPHAN_CLEANUP,
                            OperationType.REWRITE_MANIFESTS),
                    summary.enabledOperations());
        }
    }

    @Test
    void tablePolicySummaryNoPolicy() {
        List<TableIdentifier> tables = List.of(TableIdentifier.of("prod", "analytics", "events"));

        List<PolicyMatcher.TablePolicySummary> summaries =
                matcher.summarizeAllTables("prod", tables);

        assertEquals(1, summaries.size());
        assertFalse(summaries.get(0).hasPolicy());
        assertNull(summaries.get(0).policyName());
        assertTrue(summaries.get(0).enabledOperations().isEmpty());
    }

    @Test
    void effectiveConfigTypedAccessors() {
        store.save(createFullPolicy("test-policy", "prod.*.*", 10));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");

        PolicyMatcher.EffectiveConfig rewriteConfig =
                matcher.getEffectiveConfig("prod", tableId, OperationType.REWRITE_DATA_FILES).get();
        assertNotNull(rewriteConfig.rewriteDataFilesConfig());

        PolicyMatcher.EffectiveConfig expireConfig =
                matcher.getEffectiveConfig("prod", tableId, OperationType.EXPIRE_SNAPSHOTS).get();
        assertNotNull(expireConfig.expireSnapshotsConfig());

        PolicyMatcher.EffectiveConfig orphanConfig =
                matcher.getEffectiveConfig("prod", tableId, OperationType.ORPHAN_CLEANUP).get();
        assertNotNull(orphanConfig.orphanCleanupConfig());

        PolicyMatcher.EffectiveConfig manifestConfig =
                matcher.getEffectiveConfig("prod", tableId, OperationType.REWRITE_MANIFESTS).get();
        assertNotNull(manifestConfig.rewriteManifestsConfig());
    }

    @Test
    void effectiveConfigWrongAccessorThrows() {
        store.save(createFullPolicy("test-policy", "prod.*.*", 10));

        TableIdentifier tableId = TableIdentifier.of("prod", "analytics", "events");
        PolicyMatcher.EffectiveConfig rewriteConfig =
                matcher.getEffectiveConfig("prod", tableId, OperationType.REWRITE_DATA_FILES).get();

        assertThrows(IllegalStateException.class, rewriteConfig::expireSnapshotsConfig);
    }

    private MaintenancePolicy createFullPolicy(String name, String pattern, int priority) {
        return MaintenancePolicy.builder()
                .name(name)
                .tablePattern(TablePattern.parse(pattern))
                .priority(priority)
                .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                .rewriteDataFilesSchedule(ScheduleConfig.defaults())
                .expireSnapshots(ExpireSnapshotsConfig.defaults())
                .expireSnapshotsSchedule(ScheduleConfig.defaults())
                .orphanCleanup(OrphanCleanupConfig.defaults())
                .orphanCleanupSchedule(ScheduleConfig.defaults())
                .rewriteManifests(RewriteManifestsConfig.defaults())
                .rewriteManifestsSchedule(ScheduleConfig.defaults())
                .build();
    }
}
