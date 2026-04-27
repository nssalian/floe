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

package com.floe.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.policy.*;
import com.floe.server.api.dto.*;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class UpdatePolicyRequestTest {

    private final Instant createdAt = Instant.parse("2024-01-15T10:00:00Z");
    private final Instant updatedAt = Instant.parse("2024-01-15T12:00:00Z");

    private MaintenancePolicy createExistingPolicy() {
        return new MaintenancePolicy(
                "policy-123",
                "Original Name",
                "Original Description",
                TablePattern.parse("iceberg.db.*"),
                true,
                RewriteDataFilesConfig.builder().strategy("BINPACK").build(),
                new ScheduleConfig(
                        Duration.ofDays(1), null, null, null, null, Duration.ofHours(1), 0, true),
                new ExpireSnapshotsConfig(5, Duration.ofDays(7), false, null),
                null,
                null,
                null,
                null,
                null,
                10,
                null, // healthThresholds
                null, // triggerConditions
                Map.of("env", "prod"),
                createdAt,
                updatedAt);
    }

    @Test
    void applyToShouldUpdateOnlyProvidedFields() {
        MaintenancePolicy existing = createExistingPolicy();

        UpdatePolicyRequest request =
                new UpdatePolicyRequest(
                        "New Name",
                        null, // keep existing description
                        null, // keep existing tablePattern
                        20, // update priority
                        null, // keep existing enabled
                        null, // keep existing tags
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null, // healthThresholds
                        null // triggerConditions
                        );

        MaintenancePolicy updated = request.applyTo(existing);

        assertEquals("policy-123", updated.id());
        assertEquals("New Name", updated.name());
        assertEquals("Original Description", updated.description());
        assertEquals("iceberg.db.*", updated.tablePattern().toString());
        assertEquals(20, updated.priority());
        assertTrue(updated.enabled());
        assertEquals(Map.of("env", "prod"), updated.tags());
        assertEquals(createdAt, updated.createdAt());
        assertNotEquals(updatedAt, updated.updatedAt()); // updatedAt should be refreshed
    }

    @Test
    void applyToShouldUpdateAllProvidedFields() {
        MaintenancePolicy existing = createExistingPolicy();

        UpdatePolicyRequest request =
                new UpdatePolicyRequest(
                        "New Name",
                        "New Description",
                        "iceberg.newdb.*",
                        5,
                        false,
                        Map.of("env", "staging"),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null, // healthThresholds
                        null // triggerConditions
                        );

        MaintenancePolicy updated = request.applyTo(existing);

        assertEquals("New Name", updated.name());
        assertEquals("New Description", updated.description());
        assertEquals("iceberg.newdb.*", updated.tablePattern().toString());
        assertEquals(5, updated.priority());
        assertFalse(updated.enabled());
        assertEquals(Map.of("env", "staging"), updated.tags());
    }

    @Test
    void applyToShouldUpdateOperationConfigs() {
        MaintenancePolicy existing = createExistingPolicy();

        RewriteDataFilesConfigDto newRewriteConfig =
                new RewriteDataFilesConfigDto(
                        "SORT",
                        null,
                        null,
                        1024L * 1024 * 1024,
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

        UpdatePolicyRequest request =
                new UpdatePolicyRequest(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        newRewriteConfig,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null, // healthThresholds
                        null // triggerConditions
                        );

        MaintenancePolicy updated = request.applyTo(existing);

        assertNotNull(updated.rewriteDataFiles());
        assertEquals("SORT", updated.rewriteDataFiles().strategy());
        assertEquals(1024L * 1024 * 1024, updated.rewriteDataFiles().targetFileSizeBytes());
    }

    @Test
    void applyToShouldPreserveExistingOperationConfigs() {
        MaintenancePolicy existing = createExistingPolicy();

        UpdatePolicyRequest request =
                new UpdatePolicyRequest(
                        "New Name",
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null, // healthThresholds
                        null // triggerConditions
                        );

        MaintenancePolicy updated = request.applyTo(existing);

        assertNotNull(updated.rewriteDataFiles());
        assertEquals("BINPACK", updated.rewriteDataFiles().strategy());
        assertNotNull(updated.expireSnapshots());
        assertEquals(5, updated.expireSnapshots().retainLast());
    }

    @Test
    void rewriteDataFilesConfigDtoShouldConvertToConfig() {
        RewriteDataFilesConfigDto dto =
                new RewriteDataFilesConfigDto(
                        "SORT",
                        List.of("col1", "col2"),
                        List.of("col3"),
                        512L * 1024 * 1024,
                        10L * 1024 * 1024 * 1024,
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
        assertEquals(512L * 1024 * 1024, config.targetFileSizeBytes());
        assertEquals(10L * 1024 * 1024 * 1024, config.maxFileGroupSizeBytes());
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
        assertEquals(Duration.ofDays(14), config.maxSnapshotAge());
        assertTrue(config.cleanExpiredMetadata());
    }

    @Test
    void expireSnapshotsConfigDtoShouldUseDefaults() {
        ExpireSnapshotsConfigDto dto = new ExpireSnapshotsConfigDto(null, null, null, null);

        var config = dto.toConfig();

        assertEquals(5, config.retainLast());
        assertEquals(Duration.ofDays(7), config.maxSnapshotAge());
        assertFalse(config.cleanExpiredMetadata());
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

        assertEquals(Duration.ofDays(7), config.retentionPeriodInDays());
        assertEquals("/data/orphans", config.location());
        assertEquals("IGNORE", config.prefixMismatchMode());
        assertEquals(Map.of("s3a", "s3"), config.equalSchemes());
        assertEquals(Map.of("bucket.s3.amazonaws.com", "bucket"), config.equalAuthorities());
    }

    @Test
    void orphanCleanupConfigDtoShouldUseDefaults() {
        OrphanCleanupConfigDto dto = new OrphanCleanupConfigDto(null, null, null, null, null);

        var config = dto.toConfig();

        assertEquals(Duration.ofDays(3), config.retentionPeriodInDays());
        assertNull(config.location());
        assertEquals("ERROR", config.prefixMismatchMode());
        assertEquals(Map.of(), config.equalSchemes());
        assertEquals(Map.of(), config.equalAuthorities());
    }

    @Test
    void rewriteManifestsConfigDtoShouldConvertToConfig() {
        RewriteManifestsConfigDto dto =
                new RewriteManifestsConfigDto(1, "/data/staging", List.of("date", "region"), null);

        var config = dto.toConfig();

        assertEquals(1, config.specId());
        assertEquals("/data/staging", config.stagingLocation());
        assertEquals(List.of("date", "region"), config.sortBy());
    }

    @Test
    void scheduleConfigDtoShouldConvertToConfig() {
        ScheduleConfigDto dto =
                new ScheduleConfigDto(
                        7, null, "02:00", "06:00", "MONDAY, WEDNESDAY, FRIDAY", 2, 5, true);

        var config = dto.toConfig();

        assertEquals(Duration.ofDays(7), config.intervalInDays());
        assertNull(config.cronExpression());
        assertEquals(java.time.LocalTime.of(2, 0), config.windowStart());
        assertEquals(java.time.LocalTime.of(6, 0), config.windowEnd());
        assertTrue(config.allowedDays().contains(DayOfWeek.MONDAY));
        assertTrue(config.allowedDays().contains(DayOfWeek.WEDNESDAY));
        assertTrue(config.allowedDays().contains(DayOfWeek.FRIDAY));
        assertEquals(Duration.ofHours(2), config.timeoutInHours());
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
    }

    @Test
    void scheduleConfigDtoShouldHandleEmptyAllowedDays() {
        ScheduleConfigDto dto = new ScheduleConfigDto(1, null, null, null, null, null, null, null);

        var config = dto.toConfig();

        assertNull(config.allowedDays());
    }

    @Test
    void applyToShouldUpdateScheduleConfigs() {
        MaintenancePolicy existing = createExistingPolicy();

        ScheduleConfigDto newSchedule =
                new ScheduleConfigDto(7, null, null, null, null, null, null, null);

        UpdatePolicyRequest request =
                new UpdatePolicyRequest(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        newSchedule,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null, // healthThresholds
                        null // triggerConditions
                        );

        MaintenancePolicy updated = request.applyTo(existing);

        assertNotNull(updated.rewriteDataFilesSchedule());
        assertEquals(Duration.ofDays(7), updated.rewriteDataFilesSchedule().intervalInDays());
    }

    @Test
    void recordShouldSupportEquality() {
        UpdatePolicyRequest request1 =
                new UpdatePolicyRequest(
                        "Name",
                        "Desc",
                        "iceberg.*",
                        5,
                        true,
                        Map.of(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null, // healthThresholds
                        null // triggerConditions
                        );
        UpdatePolicyRequest request2 =
                new UpdatePolicyRequest(
                        "Name",
                        "Desc",
                        "iceberg.*",
                        5,
                        true,
                        Map.of(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null, // healthThresholds
                        null // triggerConditions
                        );

        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());
    }
}
