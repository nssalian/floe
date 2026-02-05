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

package com.floe.server.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.floe.core.auth.AuditLogRepository;
import com.floe.server.config.FloeConfig;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuditLogRetentionJobTest {

    @Mock private FloeConfig config;

    @Mock private FloeConfig.Security securityConfig;

    @Mock private FloeConfig.Security.Audit auditConfig;

    @Mock private AuditLogRepository auditLogRepository;

    private AuditLogRetentionJob job;

    @BeforeEach
    void setUp() throws Exception {
        job = new AuditLogRetentionJob();

        // Inject mocks via reflection
        var configField = AuditLogRetentionJob.class.getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(job, config);

        var repoField = AuditLogRetentionJob.class.getDeclaredField("auditLogRepository");
        repoField.setAccessible(true);
        repoField.set(job, auditLogRepository);

        when(config.security()).thenReturn(securityConfig);
        when(securityConfig.audit()).thenReturn(auditConfig);
    }

    @Nested
    class RunRetentionPolicy {

        @Test
        void shouldSkipWhenDatabaseDisabled() {
            when(auditConfig.databaseEnabled()).thenReturn(false);

            job.runRetentionPolicy();

            verifyNoInteractions(auditLogRepository);
        }

        @Test
        void shouldDeleteOldLogsWhenDatabaseEnabled() {
            when(auditConfig.databaseEnabled()).thenReturn(true);
            when(auditConfig.retentionDays()).thenReturn(365);
            when(auditConfig.archivalEnabled()).thenReturn(false);
            when(auditLogRepository.deleteOlderThan(any())).thenReturn(100L);

            job.runRetentionPolicy();

            ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
            verify(auditLogRepository).deleteOlderThan(cutoffCaptor.capture());

            Instant cutoff = cutoffCaptor.getValue();
            // Cutoff should be approximately 365 days ago
            Instant expected = Instant.now().minusSeconds(365 * 24 * 60 * 60);
            // Allow 1 minute tolerance
            assertTrue(Math.abs(cutoff.getEpochSecond() - expected.getEpochSecond()) < 60);
        }

        @Test
        void shouldUseConfiguredRetentionDays() {
            when(auditConfig.databaseEnabled()).thenReturn(true);
            when(auditConfig.retentionDays()).thenReturn(30);
            when(auditConfig.archivalEnabled()).thenReturn(false);
            when(auditLogRepository.deleteOlderThan(any())).thenReturn(50L);

            job.runRetentionPolicy();

            ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
            verify(auditLogRepository).deleteOlderThan(cutoffCaptor.capture());

            Instant cutoff = cutoffCaptor.getValue();
            Instant expected = Instant.now().minusSeconds(30 * 24 * 60 * 60);
            assertTrue(Math.abs(cutoff.getEpochSecond() - expected.getEpochSecond()) < 60);
        }

        @Test
        void shouldHandleExceptionGracefully() {
            when(auditConfig.databaseEnabled()).thenReturn(true);
            when(auditConfig.retentionDays()).thenReturn(30);
            when(auditConfig.archivalEnabled()).thenReturn(false);
            when(auditLogRepository.deleteOlderThan(any()))
                    .thenThrow(new RuntimeException("DB error"));

            // Should not throw
            assertDoesNotThrow(() -> job.runRetentionPolicy());
        }

        @Test
        void shouldSkipArchivalWhenBucketNotConfigured() {
            when(auditConfig.databaseEnabled()).thenReturn(true);
            when(auditConfig.retentionDays()).thenReturn(30);
            when(auditConfig.archivalEnabled()).thenReturn(true);
            when(auditConfig.archivalThresholdDays()).thenReturn(90);
            when(auditConfig.archivalBucket()).thenReturn(Optional.empty());
            when(auditLogRepository.deleteOlderThan(any())).thenReturn(10L);

            job.runRetentionPolicy();

            // Should still delete logs even if archival skipped
            verify(auditLogRepository).deleteOlderThan(any());
        }

        @Test
        void shouldSkipArchivalWhenBucketEmpty() {
            when(auditConfig.databaseEnabled()).thenReturn(true);
            when(auditConfig.retentionDays()).thenReturn(30);
            when(auditConfig.archivalEnabled()).thenReturn(true);
            when(auditConfig.archivalThresholdDays()).thenReturn(90);
            when(auditConfig.archivalBucket()).thenReturn(Optional.of(""));
            when(auditLogRepository.deleteOlderThan(any())).thenReturn(10L);

            job.runRetentionPolicy();

            verify(auditLogRepository).deleteOlderThan(any());
        }
    }

    @Nested
    class ManualRetention {

        @Test
        void shouldReturnDeletedCount() {
            when(auditConfig.retentionDays()).thenReturn(365);
            when(auditLogRepository.deleteOlderThan(any())).thenReturn(42L);

            long deleted = job.runManualRetention();

            assertEquals(42L, deleted);
        }

        @Test
        void shouldUseConfiguredRetentionDays() {
            when(auditConfig.retentionDays()).thenReturn(7);
            when(auditLogRepository.deleteOlderThan(any())).thenReturn(100L);

            job.runManualRetention();

            ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
            verify(auditLogRepository).deleteOlderThan(cutoffCaptor.capture());

            Instant cutoff = cutoffCaptor.getValue();
            Instant expected = Instant.now().minusSeconds(7 * 24 * 60 * 60);
            assertTrue(Math.abs(cutoff.getEpochSecond() - expected.getEpochSecond()) < 60);
        }

        @Test
        void shouldReturnZeroWhenNoLogsDeleted() {
            when(auditConfig.retentionDays()).thenReturn(365);
            when(auditLogRepository.deleteOlderThan(any())).thenReturn(0L);

            long deleted = job.runManualRetention();

            assertEquals(0L, deleted);
        }
    }
}
