package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MaintenancePolicyTest {

    @Test
    void defaultPolicyShouldHaveExpectedValues() {
        MaintenancePolicy policy = MaintenancePolicy.defaultPolicy();

        assertEquals("default", policy.id());
        assertEquals("Default Policy", policy.name());
        assertFalse(policy.enabled());
        assertEquals(0, policy.priority());
        assertNotNull(policy.tablePattern());
        assertNull(policy.rewriteDataFiles());
        assertNull(policy.expireSnapshots());
        assertNull(policy.orphanCleanup());
        assertNull(policy.rewriteManifests());
    }

    @Test
    void builderShouldCreatePolicyWithAllFields() {
        Instant now = Instant.now();
        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .id("policy-123")
                        .name("Production Policy")
                        .description("Policy for production tables")
                        .tablePattern(TablePattern.parse("iceberg.prod.*"))
                        .enabled(true)
                        .priority(10)
                        .tags(Map.of("env", "prod"))
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(ScheduleConfig.defaults())
                        .expireSnapshots(ExpireSnapshotsConfig.defaults())
                        .expireSnapshotsSchedule(ScheduleConfig.defaults())
                        .createdAt(now)
                        .updatedAt(now)
                        .build();

        assertEquals("policy-123", policy.id());
        assertEquals("Production Policy", policy.name());
        assertEquals("Policy for production tables", policy.description());
        assertEquals("iceberg.prod.*", policy.tablePattern().toString());
        assertTrue(policy.enabled());
        assertEquals(10, policy.priority());
        assertEquals(Map.of("env", "prod"), policy.tags());
        assertNotNull(policy.rewriteDataFiles());
        assertNotNull(policy.rewriteDataFilesSchedule());
        assertNotNull(policy.expireSnapshots());
        assertNotNull(policy.expireSnapshotsSchedule());
        assertEquals(now, policy.createdAt());
        assertEquals(now, policy.updatedAt());
    }

    @Test
    void builderShouldGenerateIdIfNotProvided() {
        MaintenancePolicy policy = MaintenancePolicy.builder().name("Test Policy").build();

        assertNotNull(policy.id());
        assertFalse(policy.id().isEmpty());
    }

    @Test
    void effectivePriorityShouldIncludePatternSpecificity() {
        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .priority(5)
                        .tablePattern(TablePattern.parse("iceberg.db.orders"))
                        .build();

        int effectivePriority = policy.effectivePriority();

        // Priority * 1000 + specificity
        assertTrue(effectivePriority > 5000);
    }

    @Test
    void getNameOrDefaultShouldReturnNameWhenSet() {
        MaintenancePolicy policy =
                MaintenancePolicy.builder().id("policy-123").name("Custom Name").build();

        assertEquals("Custom Name", policy.getNameOrDefault());
    }

    @Test
    void getNameOrDefaultShouldReturnDefaultWhenNull() {
        MaintenancePolicy policy = MaintenancePolicy.builder().id("policy-123").name(null).build();

        assertEquals("Policy-policy-123", policy.getNameOrDefault());
    }

    @Test
    void getNameOrDefaultShouldReturnDefaultWhenBlank() {
        MaintenancePolicy policy = MaintenancePolicy.builder().id("policy-123").name("   ").build();

        assertEquals("Policy-policy-123", policy.getNameOrDefault());
    }

    @Test
    void isActiveShouldReturnEnabledStatus() {
        MaintenancePolicy enabled = MaintenancePolicy.builder().enabled(true).build();
        MaintenancePolicy disabled = MaintenancePolicy.builder().enabled(false).build();

        assertTrue(enabled.isActive());
        assertFalse(disabled.isActive());
    }

    @Test
    void hasAnyOperationsShouldReturnTrueWhenOperationsConfigured() {
        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .build();

        assertTrue(policy.hasAnyOperations());
    }

    @Test
    void hasAnyOperationsShouldReturnFalseWhenNoOperations() {
        MaintenancePolicy policy = MaintenancePolicy.builder().build();

        assertFalse(policy.hasAnyOperations());
    }

    @Test
    void isOperationEnabledShouldRequireConfigAndSchedule() {
        ScheduleConfig enabledSchedule = ScheduleConfig.builder().enabled(true).build();

        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(enabledSchedule)
                        .build();

        assertTrue(policy.isOperationEnabled(OperationType.REWRITE_DATA_FILES));
        assertFalse(policy.isOperationEnabled(OperationType.EXPIRE_SNAPSHOTS));
    }

    @Test
    void isOperationEnabledShouldReturnFalseWhenScheduleDisabled() {
        ScheduleConfig disabledSchedule = ScheduleConfig.builder().enabled(false).build();

        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(disabledSchedule)
                        .build();

        assertFalse(policy.isOperationEnabled(OperationType.REWRITE_DATA_FILES));
    }

    @Test
    void hasOperationConfigShouldCheckConfigOnly() {
        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .expireSnapshots(ExpireSnapshotsConfig.defaults())
                        .build();

        assertTrue(policy.hasOperationConfig(OperationType.REWRITE_DATA_FILES));
        assertTrue(policy.hasOperationConfig(OperationType.EXPIRE_SNAPSHOTS));
        assertFalse(policy.hasOperationConfig(OperationType.ORPHAN_CLEANUP));
        assertFalse(policy.hasOperationConfig(OperationType.REWRITE_MANIFESTS));
    }

    @Test
    void getScheduleShouldReturnCorrectSchedule() {
        ScheduleConfig rewriteSchedule = ScheduleConfig.defaults();
        ScheduleConfig expireSchedule = ScheduleConfig.nightlyAt2am();

        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .rewriteDataFilesSchedule(rewriteSchedule)
                        .expireSnapshotsSchedule(expireSchedule)
                        .build();

        assertEquals(rewriteSchedule, policy.getSchedule(OperationType.REWRITE_DATA_FILES));
        assertEquals(expireSchedule, policy.getSchedule(OperationType.EXPIRE_SNAPSHOTS));
        assertNull(policy.getSchedule(OperationType.ORPHAN_CLEANUP));
        assertNull(policy.getSchedule(OperationType.REWRITE_MANIFESTS));
    }

    @Test
    void allOperationTypesShouldBeHandled() {
        ScheduleConfig schedule = ScheduleConfig.builder().enabled(true).build();

        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .rewriteDataFiles(RewriteDataFilesConfig.defaults())
                        .rewriteDataFilesSchedule(schedule)
                        .expireSnapshots(ExpireSnapshotsConfig.defaults())
                        .expireSnapshotsSchedule(schedule)
                        .orphanCleanup(OrphanCleanupConfig.defaults())
                        .orphanCleanupSchedule(schedule)
                        .rewriteManifests(RewriteManifestsConfig.defaults())
                        .rewriteManifestsSchedule(schedule)
                        .build();

        assertTrue(policy.isOperationEnabled(OperationType.REWRITE_DATA_FILES));
        assertTrue(policy.isOperationEnabled(OperationType.EXPIRE_SNAPSHOTS));
        assertTrue(policy.isOperationEnabled(OperationType.ORPHAN_CLEANUP));
        assertTrue(policy.isOperationEnabled(OperationType.REWRITE_MANIFESTS));

        assertTrue(policy.hasOperationConfig(OperationType.REWRITE_DATA_FILES));
        assertTrue(policy.hasOperationConfig(OperationType.EXPIRE_SNAPSHOTS));
        assertTrue(policy.hasOperationConfig(OperationType.ORPHAN_CLEANUP));
        assertTrue(policy.hasOperationConfig(OperationType.REWRITE_MANIFESTS));

        assertNotNull(policy.getSchedule(OperationType.REWRITE_DATA_FILES));
        assertNotNull(policy.getSchedule(OperationType.EXPIRE_SNAPSHOTS));
        assertNotNull(policy.getSchedule(OperationType.ORPHAN_CLEANUP));
        assertNotNull(policy.getSchedule(OperationType.REWRITE_MANIFESTS));
    }

    @Test
    void recordShouldSupportEquality() {
        Instant now = Instant.now();
        MaintenancePolicy policy1 =
                new MaintenancePolicy(
                        "policy-123",
                        "Name",
                        "Desc",
                        TablePattern.matchAll(),
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        5,
                        Map.of(),
                        now,
                        now);
        MaintenancePolicy policy2 =
                new MaintenancePolicy(
                        "policy-123",
                        "Name",
                        "Desc",
                        TablePattern.matchAll(),
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        5,
                        Map.of(),
                        now,
                        now);

        assertEquals(policy1, policy2);
        assertEquals(policy1.hashCode(), policy2.hashCode());
    }

    @Test
    void differentPoliciesShouldNotBeEqual() {
        MaintenancePolicy policy1 = MaintenancePolicy.builder().id("policy-1").build();
        MaintenancePolicy policy2 = MaintenancePolicy.builder().id("policy-2").build();

        assertNotEquals(policy1, policy2);
    }
}
