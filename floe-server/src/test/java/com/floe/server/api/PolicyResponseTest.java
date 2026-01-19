package com.floe.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.policy.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PolicyResponseTest {

    private final Instant createdAt = Instant.parse("2024-01-15T10:00:00Z");
    private final Instant updatedAt = Instant.parse("2024-01-15T12:00:00Z");

    @Test
    void shouldCreateWithAllFields() {
        PolicyResponse.OperationSummary rewriteSummary =
                new PolicyResponse.OperationSummary(true, "P1D");

        PolicyResponse response =
                new PolicyResponse(
                        "policy-123",
                        "Daily Compact",
                        "Compacts tables daily",
                        "iceberg.db.*",
                        true,
                        10,
                        Map.of("env", "prod"),
                        createdAt,
                        updatedAt,
                        rewriteSummary,
                        null,
                        null,
                        null);

        assertEquals("policy-123", response.id());
        assertEquals("Daily Compact", response.name());
        assertEquals("Compacts tables daily", response.description());
        assertEquals("iceberg.db.*", response.tablePattern());
        assertTrue(response.enabled());
        assertEquals(10, response.priority());
        assertEquals(Map.of("env", "prod"), response.tags());
        assertEquals(createdAt, response.createdAt());
        assertEquals(updatedAt, response.updatedAt());
        assertNotNull(response.rewriteDataFiles());
        assertNull(response.expireSnapshots());
    }

    @Test
    void fromShouldConvertMaintenancePolicyWithAllOperations() {
        RewriteDataFilesConfig rewriteConfig =
                RewriteDataFilesConfig.builder()
                        .strategy("BINPACK")
                        .targetFileSizeBytes(512 * 1024 * 1024L)
                        .build();

        ScheduleConfig rewriteSchedule =
                new ScheduleConfig(
                        Duration.ofDays(1), null, null, null, null, Duration.ofHours(1), 0, true);

        ExpireSnapshotsConfig expireConfig =
                new ExpireSnapshotsConfig(5, Duration.ofDays(7), false, null);

        ScheduleConfig expireSchedule =
                new ScheduleConfig(
                        Duration.ofDays(7), null, null, null, null, Duration.ofHours(1), 0, true);

        MaintenancePolicy policy =
                new MaintenancePolicy(
                        "policy-123",
                        "Full Maintenance",
                        "All operations enabled",
                        TablePattern.parse("iceberg.db.*"),
                        true,
                        rewriteConfig,
                        rewriteSchedule,
                        expireConfig,
                        expireSchedule,
                        null,
                        null,
                        null,
                        null,
                        10,
                        Map.of("env", "prod"),
                        createdAt,
                        updatedAt);

        PolicyResponse response = PolicyResponse.from(policy);

        assertEquals("policy-123", response.id());
        assertEquals("Full Maintenance", response.name());
        assertEquals("All operations enabled", response.description());
        assertEquals("iceberg.db.*", response.tablePattern());
        assertTrue(response.enabled());
        assertEquals(10, response.priority());

        assertNotNull(response.rewriteDataFiles());
        assertTrue(response.rewriteDataFiles().enabled());
        assertEquals("P1D", response.rewriteDataFiles().schedule());

        assertNotNull(response.expireSnapshots());
        assertTrue(response.expireSnapshots().enabled());
        assertEquals("P7D", response.expireSnapshots().schedule());

        assertNull(response.orphanCleanup());
        assertNull(response.rewriteManifests());
    }

    @Test
    void fromShouldHandlePolicyWithNoOperations() {
        MaintenancePolicy policy =
                new MaintenancePolicy(
                        "policy-123",
                        "Empty Policy",
                        "No operations",
                        TablePattern.parse("iceberg.db.*"),
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
                        createdAt,
                        updatedAt);

        PolicyResponse response = PolicyResponse.from(policy);

        assertEquals("policy-123", response.id());
        assertNull(response.rewriteDataFiles());
        assertNull(response.expireSnapshots());
        assertNull(response.orphanCleanup());
        assertNull(response.rewriteManifests());
    }

    @Test
    void fromShouldHandleCronSchedule() {
        RewriteDataFilesConfig rewriteConfig = RewriteDataFilesConfig.builder().build();

        ScheduleConfig cronSchedule =
                new ScheduleConfig(
                        null, "0 0 * * *", null, null, null, Duration.ofHours(1), 0, true);

        MaintenancePolicy policy =
                new MaintenancePolicy(
                        "policy-123",
                        "Cron Policy",
                        "Uses cron",
                        TablePattern.parse("iceberg.db.*"),
                        true,
                        rewriteConfig,
                        cronSchedule,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        5,
                        Map.of(),
                        createdAt,
                        updatedAt);

        PolicyResponse response = PolicyResponse.from(policy);

        assertNotNull(response.rewriteDataFiles());
        assertEquals("0 0 * * *", response.rewriteDataFiles().schedule());
    }

    @Test
    void fromShouldHandleScheduleWithNoIntervalOrCron() {
        RewriteDataFilesConfig rewriteConfig = RewriteDataFilesConfig.builder().build();

        ScheduleConfig noSchedule =
                new ScheduleConfig(null, null, null, null, null, Duration.ofHours(1), 0, true);

        MaintenancePolicy policy =
                new MaintenancePolicy(
                        "policy-123",
                        "No Schedule Policy",
                        "No interval or cron",
                        TablePattern.parse("iceberg.db.*"),
                        true,
                        rewriteConfig,
                        noSchedule,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        5,
                        Map.of(),
                        createdAt,
                        updatedAt);

        PolicyResponse response = PolicyResponse.from(policy);

        assertNotNull(response.rewriteDataFiles());
        assertNull(response.rewriteDataFiles().schedule());
    }

    @Test
    void operationSummaryShouldStoreEnabledAndSchedule() {
        PolicyResponse.OperationSummary summary = new PolicyResponse.OperationSummary(true, "P7D");

        assertTrue(summary.enabled());
        assertEquals("P7D", summary.schedule());
    }

    @Test
    void operationSummaryShouldHandleNullSchedule() {
        PolicyResponse.OperationSummary summary = new PolicyResponse.OperationSummary(false, null);

        assertFalse(summary.enabled());
        assertNull(summary.schedule());
    }

    @Test
    void recordShouldSupportEquality() {
        PolicyResponse response1 =
                new PolicyResponse(
                        "policy-123",
                        "Policy",
                        "Desc",
                        "iceberg.*",
                        true,
                        5,
                        Map.of(),
                        createdAt,
                        updatedAt,
                        null,
                        null,
                        null,
                        null);
        PolicyResponse response2 =
                new PolicyResponse(
                        "policy-123",
                        "Policy",
                        "Desc",
                        "iceberg.*",
                        true,
                        5,
                        Map.of(),
                        createdAt,
                        updatedAt,
                        null,
                        null,
                        null,
                        null);

        assertEquals(response1, response2);
        assertEquals(response1.hashCode(), response2.hashCode());
    }

    @Test
    void differentResponsesShouldNotBeEqual() {
        PolicyResponse response1 =
                new PolicyResponse(
                        "policy-123",
                        "Policy1",
                        "Desc",
                        "iceberg.*",
                        true,
                        5,
                        Map.of(),
                        createdAt,
                        updatedAt,
                        null,
                        null,
                        null,
                        null);
        PolicyResponse response2 =
                new PolicyResponse(
                        "policy-456",
                        "Policy2",
                        "Desc",
                        "iceberg.*",
                        true,
                        5,
                        Map.of(),
                        createdAt,
                        updatedAt,
                        null,
                        null,
                        null,
                        null);

        assertNotEquals(response1, response2);
    }
}
