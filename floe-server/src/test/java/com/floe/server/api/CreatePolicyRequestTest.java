package com.floe.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.policy.MaintenancePolicy;
import com.floe.server.api.dto.*;
import java.time.DayOfWeek;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CreatePolicyRequestTest {

    @Test
    void shouldCreateWithRequiredFieldsOnly() {
        CreatePolicyRequest request =
                new CreatePolicyRequest(
                        "Daily Compact",
                        null,
                        "iceberg.db.*",
                        10,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        assertEquals("Daily Compact", request.name());
        assertNull(request.description());
        assertEquals("iceberg.db.*", request.tablePattern());
        assertEquals(10, request.priority());
        assertNull(request.tags());
    }

    @Test
    void toPolicyShouldCreateValidMaintenancePolicy() {
        CreatePolicyRequest request =
                new CreatePolicyRequest(
                        "Daily Compact",
                        "Compacts tables daily",
                        "iceberg.db.*",
                        10,
                        Map.of("env", "prod"),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        MaintenancePolicy policy = request.toPolicy();

        assertNotNull(policy.id());
        assertEquals("Daily Compact", policy.name());
        assertEquals("Compacts tables daily", policy.description());
        assertEquals("iceberg.db.*", policy.tablePattern().toString());
        assertTrue(policy.enabled());
        assertEquals(10, policy.priority());
        assertEquals(Map.of("env", "prod"), policy.tags());
        assertNotNull(policy.createdAt());
        assertNotNull(policy.updatedAt());
    }

    @Test
    void toPolicyShouldHandleNullTags() {
        CreatePolicyRequest request =
                new CreatePolicyRequest(
                        "Policy",
                        null,
                        "iceberg.*",
                        5,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        MaintenancePolicy policy = request.toPolicy();

        assertEquals(Map.of(), policy.tags());
    }

    @Test
    void rewriteDataFilesConfigDtoShouldConvertToConfig() {
        RewriteDataFilesConfigDto dto =
                new RewriteDataFilesConfigDto(
                        "SORT",
                        List.of("col1", "col2"),
                        List.of("col3"),
                        512 * 1024 * 1024L,
                        10 * 1024 * 1024 * 1024L,
                        4,
                        true,
                        10,
                        3,
                        "date > '2024-01-01'",
                        "FILES_DESC",
                        true,
                        true,
                        1);

        var config = dto.toConfig();

        assertEquals("SORT", config.strategy());
        assertEquals(List.of("col1", "col2"), config.sortOrder());
        assertEquals(List.of("col3"), config.zOrderColumns());
        assertEquals(512 * 1024 * 1024L, config.targetFileSizeBytes());
        assertEquals(10 * 1024 * 1024 * 1024L, config.maxFileGroupSizeBytes());
        assertEquals(4, config.maxConcurrentFileGroupRewrites());
        assertTrue(config.partialProgressEnabled());
        assertEquals(10, config.partialProgressMaxCommits());
        assertEquals(3, config.partialProgressMaxFailedCommits());
        assertEquals("date > '2024-01-01'", config.filter());
        assertEquals("FILES_DESC", config.rewriteJobOrder());
        assertTrue(config.useStartingSequenceNumber());
        assertTrue(config.removeDanglingDeletes());
        assertEquals(1, config.outputSpecId());
    }

    @Test
    void rewriteDataFilesConfigDtoShouldUseDefaultStrategy() {
        RewriteDataFilesConfigDto dto =
                new RewriteDataFilesConfigDto(
                        null, null, null, null, null, null, null, null, null, null, null, null,
                        null, null);

        var config = dto.toConfig();

        assertEquals("BINPACK", config.strategy());
    }

    @Test
    void expireSnapshotsConfigDtoShouldConvertToConfig() {
        ExpireSnapshotsConfigDto dto = new ExpireSnapshotsConfigDto(10, "P14D", true, null);

        var config = dto.toConfig();

        assertEquals(10, config.retainLast());
        assertEquals(java.time.Duration.ofDays(14), config.maxSnapshotAge());
        assertTrue(config.cleanExpiredMetadata());
    }

    @Test
    void expireSnapshotsConfigDtoShouldUseDefaults() {
        ExpireSnapshotsConfigDto dto = new ExpireSnapshotsConfigDto(null, null, null, null);

        var config = dto.toConfig();

        assertEquals(5, config.retainLast());
        assertEquals(java.time.Duration.ofDays(7), config.maxSnapshotAge());
        assertFalse(config.cleanExpiredMetadata());
    }

    @Test
    void expireSnapshotsConfigDtoShouldConvertExpireSnapshotId() {
        ExpireSnapshotsConfigDto dto = new ExpireSnapshotsConfigDto(5, "P7D", false, 9876543210L);

        var config = dto.toConfig();

        assertEquals(5, config.retainLast());
        assertEquals(java.time.Duration.ofDays(7), config.maxSnapshotAge());
        assertFalse(config.cleanExpiredMetadata());
        assertEquals(9876543210L, config.expireSnapshotId());
    }

    @Test
    void expireSnapshotsConfigDtoShouldHandleNullExpireSnapshotId() {
        ExpireSnapshotsConfigDto dto = new ExpireSnapshotsConfigDto(10, "P14D", true, null);

        var config = dto.toConfig();

        assertNull(config.expireSnapshotId());
    }

    @Test
    void orphanCleanupConfigDtoShouldConvertToConfig() {
        OrphanCleanupConfigDto dto =
                new OrphanCleanupConfigDto(
                        7,
                        "/data/orphans",
                        "IGNORE",
                        Map.of("s3a", "s3"),
                        Map.of("bucket.s3.amazonaws.com", "bucket"));

        var config = dto.toConfig();

        assertEquals(java.time.Duration.ofDays(7), config.retentionPeriodInDays());
        assertEquals("/data/orphans", config.location());
        assertEquals("IGNORE", config.prefixMismatchMode());
        assertEquals(Map.of("s3a", "s3"), config.equalSchemes());
        assertEquals(Map.of("bucket.s3.amazonaws.com", "bucket"), config.equalAuthorities());
    }

    @Test
    void orphanCleanupConfigDtoShouldUseDefaults() {
        OrphanCleanupConfigDto dto = new OrphanCleanupConfigDto(null, null, null, null, null);

        var config = dto.toConfig();

        assertEquals(java.time.Duration.ofDays(3), config.retentionPeriodInDays());
        assertNull(config.location());
        assertEquals("ERROR", config.prefixMismatchMode());
        assertEquals(Map.of(), config.equalSchemes());
        assertEquals(Map.of(), config.equalAuthorities());
    }

    @Test
    void rewriteManifestsConfigDtoShouldConvertToConfig() {
        RewriteManifestsConfigDto dto =
                new RewriteManifestsConfigDto(2, "/staging", List.of("date"), null);

        var config = dto.toConfig();

        assertEquals(2, config.specId());
        assertEquals("/staging", config.stagingLocation());
        assertEquals(List.of("date"), config.sortBy());
    }

    @Test
    void rewriteManifestsConfigDtoShouldConvertRewriteIf() {
        var partitionSummary =
                new RewriteManifestsConfigDto.ManifestFilterDto.PartitionFieldSummaryDto(
                        true, false, "00", "ff");

        var filterDto =
                new RewriteManifestsConfigDto.ManifestFilterDto(
                        "/data/manifest.avro",
                        4096L,
                        0,
                        "DATA",
                        100L,
                        50L,
                        999L,
                        10,
                        5,
                        2,
                        1000L,
                        500L,
                        200L,
                        1L,
                        "deadbeef",
                        List.of(partitionSummary));

        RewriteManifestsConfigDto dto =
                new RewriteManifestsConfigDto(1, "/staging", List.of("date"), filterDto);

        var config = dto.toConfig();

        assertEquals(1, config.specId());
        assertEquals("/staging", config.stagingLocation());
        assertNotNull(config.rewriteIf());
        assertEquals("/data/manifest.avro", config.rewriteIf().path());
        assertEquals(4096L, config.rewriteIf().length());
        assertEquals(0, config.rewriteIf().specId());
        assertEquals("DATA", config.rewriteIf().content());
        assertEquals(100L, config.rewriteIf().sequenceNumber());
        assertEquals(50L, config.rewriteIf().minSequenceNumber());
        assertEquals(999L, config.rewriteIf().snapshotId());
        assertEquals(10, config.rewriteIf().addedFilesCount());
        assertEquals(5, config.rewriteIf().existingFilesCount());
        assertEquals(2, config.rewriteIf().deletedFilesCount());
        assertEquals(1000L, config.rewriteIf().addedRowsCount());
        assertEquals(500L, config.rewriteIf().existingRowsCount());
        assertEquals(200L, config.rewriteIf().deletedRowsCount());
        assertEquals(1L, config.rewriteIf().firstRowId());
        assertEquals("deadbeef", config.rewriteIf().keyMetadata());
        assertNotNull(config.rewriteIf().partitionSummaries());
        assertEquals(1, config.rewriteIf().partitionSummaries().size());
        assertTrue(config.rewriteIf().partitionSummaries().get(0).containsNull());
    }

    @Test
    void rewriteManifestsConfigDtoShouldHandleNullRewriteIf() {
        RewriteManifestsConfigDto dto =
                new RewriteManifestsConfigDto(1, "/staging", List.of("date"), null);

        var config = dto.toConfig();

        assertNull(config.rewriteIf());
    }

    @Test
    void manifestFilterDtoPartitionSummaryShouldConvertToConfig() {
        var summaryDto =
                new RewriteManifestsConfigDto.ManifestFilterDto.PartitionFieldSummaryDto(
                        true, false, "00000000", "ffffffff");

        var summaryConfig = summaryDto.toConfig();

        assertTrue(summaryConfig.containsNull());
        assertFalse(summaryConfig.containsNan());
        assertEquals("00000000", summaryConfig.lowerBound());
        assertEquals("ffffffff", summaryConfig.upperBound());
    }

    @Test
    void scheduleConfigDtoShouldConvertToConfig() {
        ScheduleConfigDto dto =
                new ScheduleConfigDto(
                        7, null, "02:00", "06:00", "MONDAY,WEDNESDAY,FRIDAY", 2, 5, true);

        var config = dto.toConfig();

        assertEquals(java.time.Duration.ofDays(7), config.intervalInDays());
        assertNull(config.cronExpression());
        assertEquals(java.time.LocalTime.of(2, 0), config.windowStart());
        assertEquals(java.time.LocalTime.of(6, 0), config.windowEnd());
        assertTrue(config.allowedDays().contains(DayOfWeek.MONDAY));
        assertTrue(config.allowedDays().contains(DayOfWeek.WEDNESDAY));
        assertTrue(config.allowedDays().contains(DayOfWeek.FRIDAY));
        assertEquals(java.time.Duration.ofHours(2), config.timeoutInHours());
        assertEquals(5, config.priority());
        assertTrue(config.enabled());
    }

    @Test
    void scheduleConfigDtoShouldHandleCronExpression() {
        ScheduleConfigDto dto =
                new ScheduleConfigDto(null, "0 2 * * *", null, null, null, null, null, null);

        var config = dto.toConfig();

        assertNull(config.intervalInDays());
        assertEquals("0 2 * * *", config.cronExpression());
        assertTrue(config.enabled());
    }

    @Test
    void scheduleConfigDtoShouldHandleNullAllowedDays() {
        ScheduleConfigDto dto = new ScheduleConfigDto(1, null, null, null, null, null, null, null);

        var config = dto.toConfig();

        assertNull(config.allowedDays());
    }

    @Test
    void toPolicyShouldIncludeOperationConfigs() {
        RewriteDataFilesConfigDto rewriteDto =
                new RewriteDataFilesConfigDto(
                        "BINPACK",
                        null,
                        null,
                        512L * 1024 * 1024,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        ScheduleConfigDto scheduleDto =
                new ScheduleConfigDto(1, null, null, null, null, null, null, null);

        CreatePolicyRequest request =
                new CreatePolicyRequest(
                        "Test Policy",
                        "Description",
                        "iceberg.db.*",
                        10,
                        null,
                        rewriteDto,
                        scheduleDto,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        MaintenancePolicy policy = request.toPolicy();

        assertNotNull(policy.rewriteDataFiles());
        assertEquals("BINPACK", policy.rewriteDataFiles().strategy());
        assertNotNull(policy.rewriteDataFilesSchedule());
        assertEquals(
                java.time.Duration.ofDays(1), policy.rewriteDataFilesSchedule().intervalInDays());
    }

    @Test
    void recordShouldSupportEquality() {
        CreatePolicyRequest request1 =
                new CreatePolicyRequest(
                        "Policy",
                        "Desc",
                        "iceberg.*",
                        5,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        CreatePolicyRequest request2 =
                new CreatePolicyRequest(
                        "Policy",
                        "Desc",
                        "iceberg.*",
                        5,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());
    }
}
