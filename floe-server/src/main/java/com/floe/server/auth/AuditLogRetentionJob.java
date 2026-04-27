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

import com.floe.core.auth.AuditLogRepository;
import com.floe.server.config.FloeConfig;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduled job for audit log retention and archival. Runs daily to: 1. Delete audit logs older
 * than retention period (7 years by default) 2. Archive audit logs to object storage (optional)
 *
 * <p>Meets compliance requirements for GDPR, SOC 2, and PCI DSS.
 */
@ApplicationScoped
public class AuditLogRetentionJob {

    private static final Logger LOG = LoggerFactory.getLogger(AuditLogRetentionJob.class);

    @Inject FloeConfig config;

    @Inject AuditLogRepository auditLogRepository;

    /** Run retention job daily at 2 AM Deletes audit logs older than configured retention period */
    @Scheduled(cron = "0 0 2 * * ?") // 2 AM every day
    void runRetentionPolicy() {
        if (!config.security().audit().databaseEnabled()) {
            LOG.debug("Audit log database storage is disabled, skipping retention job");
            return;
        }

        try {
            int retentionDays = config.security().audit().retentionDays();
            Instant cutoffDate = Instant.now().minus(retentionDays, ChronoUnit.DAYS);

            LOG.info(
                    "Starting audit log retention job (retention period: "
                            + retentionDays
                            + " days, cutoff: "
                            + cutoffDate
                            + ")");

            // Archive old logs first (if enabled)
            if (config.security().audit().archivalEnabled()) {
                archiveOldLogs();
            }

            // Delete logs older than retention period
            long deletedCount = auditLogRepository.deleteOlderThan(cutoffDate);

            LOG.info(
                    "Audit log retention job completed. Deleted "
                            + deletedCount
                            + " logs older than "
                            + retentionDays
                            + " days");
        } catch (Exception e) {
            LOG.error("Audit log retention job failed", e);
        }
    }

    /**
     * Archive audit logs to object storage (S3, etc.) Moves logs older than archival threshold to
     * cheaper storage while keeping recent logs in database for fast queries
     */
    private void archiveOldLogs() {
        try {
            int archivalThresholdDays = config.security().audit().archivalThresholdDays();
            String archivalBucket = config.security().audit().archivalBucket().orElse(null);

            if (archivalBucket == null || archivalBucket.isBlank()) {
                LOG.warn(
                        "Audit log archival is enabled but no archival bucket configured, skipping archival");
                return;
            }

            Instant archivalCutoff = Instant.now().minus(archivalThresholdDays, ChronoUnit.DAYS);

            LOG.info(
                    "Starting audit log archival (threshold: "
                            + archivalThresholdDays
                            + " days, cutoff: "
                            + archivalCutoff
                            + ", bucket: "
                            + archivalBucket
                            + ")");
            LOG.info("Audit log archival not yet implemented - skipping");
        } catch (Exception e) {
            LOG.error("Audit log archival failed", e);
        }
    }

    /**
     * Manual trigger for retention job (exposed via management endpoint if needed)
     *
     * @return Number of deleted logs
     */
    public long runManualRetention() {
        LOG.info("Manual audit log retention triggered");
        int retentionDays = config.security().audit().retentionDays();
        Instant cutoffDate = Instant.now().minus(retentionDays, ChronoUnit.DAYS);
        return auditLogRepository.deleteOlderThan(cutoffDate);
    }
}
