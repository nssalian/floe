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

package com.floe.core.auth;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Repository interface for audit log persistence. Implementations must ensure append-only semantics
 * (no updates or deletes).
 */
public interface AuditLogRepository {
    /**
     * Save a new audit log entry (append-only)
     *
     * @param auditLog The audit log to save
     * @return The saved audit log with generated ID
     */
    AuditLog save(AuditLog auditLog);

    /**
     * Find audit log by ID
     *
     * @param id The audit log ID
     * @return Optional containing the audit log if found
     */
    Optional<AuditLog> findById(Long id);

    /**
     * Find audit logs within a time range
     *
     * @param start Start timestamp (inclusive)
     * @param end End timestamp (inclusive)
     * @param limit Maximum number of results
     * @return List of audit logs ordered by timestamp DESC
     */
    List<AuditLog> findByTimeRange(Instant start, Instant end, int limit);

    /**
     * Find audit logs for a specific user
     *
     * @param userId User identifier
     * @param limit Maximum number of results
     * @return List of audit logs ordered by timestamp DESC
     */
    List<AuditLog> findByUserId(String userId, int limit);

    /**
     * Find audit logs by event type
     *
     * @param eventType Event type (from AuditEvent enum)
     * @param limit Maximum number of results
     * @return List of audit logs ordered by timestamp DESC
     */
    List<AuditLog> findByEventType(String eventType, int limit);

    /**
     * Find audit logs by severity
     *
     * @param severity Severity level (INFO, WARN, ERROR)
     * @param limit Maximum number of results
     * @return List of audit logs ordered by timestamp DESC
     */
    List<AuditLog> findBySeverity(String severity, int limit);

    /**
     * Find recent audit logs (last N days)
     *
     * @param days Number of days to look back
     * @param limit Maximum number of results
     * @return List of audit logs ordered by timestamp DESC
     */
    List<AuditLog> findRecent(int days, int limit);

    /**
     * Count audit logs by event type within time range
     *
     * @param eventType Event type
     * @param start Start timestamp
     * @param end End timestamp
     * @return Count of matching audit logs
     */
    long countByEventType(String eventType, Instant start, Instant end);

    /**
     * Delete old audit logs (for archival/retention policies) WARNING: Should only be called by
     * automated retention jobs, not exposed via API
     *
     * @param olderThan Delete logs older than this timestamp
     * @return Number of deleted records
     */
    long deleteOlderThan(Instant olderThan);
}
