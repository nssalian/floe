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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floe.core.auth.AuditLog;
import com.floe.core.auth.AuditLogRepository;
import com.floe.core.exception.FloeDataAccessException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL implementation of AuditLogRepository. Provides append-only storage for audit logs with
 * immutability enforced at database level via triggers.
 */
@ApplicationScoped
public class PostgresAuditLogRepository implements AuditLogRepository {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresAuditLogRepository.class);

    @Inject DataSource dataSource;

    @Inject ObjectMapper objectMapper;

    @Override
    public AuditLog save(AuditLog auditLog) {
        String sql =
                """
            INSERT INTO audit_logs (
                timestamp, event_type, event_description, severity,
                user_id, username, auth_method,
                resource, http_method, ip_address, user_agent, details
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            RETURNING id, created_at
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, Timestamp.from(auditLog.timestamp()));
            stmt.setString(2, auditLog.eventType());
            stmt.setString(3, auditLog.eventDescription());
            stmt.setString(4, auditLog.severity());
            stmt.setString(5, auditLog.userId());
            stmt.setString(6, auditLog.username());
            stmt.setString(7, auditLog.authMethod());
            stmt.setString(8, auditLog.resource());
            stmt.setString(9, auditLog.httpMethod());
            stmt.setString(10, auditLog.ipAddress());
            stmt.setString(11, auditLog.userAgent());

            // Convert details map to JSON string
            String detailsJson =
                    auditLog.details() != null
                            ? objectMapper.writeValueAsString(auditLog.details())
                            : null;
            stmt.setString(12, detailsJson);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long id = rs.getLong("id");
                    Instant createdAt = rs.getTimestamp("created_at").toInstant();

                    return new AuditLog(
                            id,
                            auditLog.timestamp(),
                            auditLog.eventType(),
                            auditLog.eventDescription(),
                            auditLog.severity(),
                            auditLog.userId(),
                            auditLog.username(),
                            auditLog.authMethod(),
                            auditLog.resource(),
                            auditLog.httpMethod(),
                            auditLog.ipAddress(),
                            auditLog.userAgent(),
                            auditLog.details(),
                            createdAt);
                }
            }

            throw new FloeDataAccessException(
                    "insert", "audit_logs", "Failed to retrieve generated ID after insert", null);
        } catch (Exception e) {
            LOG.error("Failed to save audit log", e);
            throw new FloeDataAccessException("insert", "audit_logs", e);
        }
    }

    @Override
    public Optional<AuditLog> findById(Long id) {
        String sql = "SELECT * FROM audit_logs WHERE id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRowToAuditLog(rs));
                }
                return Optional.empty();
            }
        } catch (Exception e) {
            LOG.error("Failed to find audit log by id", e);
            throw new FloeDataAccessException("query", "audit_logs", e);
        }
    }

    @Override
    public List<AuditLog> findByTimeRange(Instant start, Instant end, int limit) {
        String sql =
                """
            SELECT * FROM audit_logs
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, Timestamp.from(start));
            stmt.setTimestamp(2, Timestamp.from(end));
            stmt.setInt(3, limit);

            return executeQuery(stmt);
        } catch (Exception e) {
            LOG.error("Failed to find audit logs by time range", e);
            throw new FloeDataAccessException("query", "audit_logs", e);
        }
    }

    @Override
    public List<AuditLog> findByUserId(String userId, int limit) {
        String sql =
                """
            SELECT * FROM audit_logs
            WHERE user_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, userId);
            stmt.setInt(2, limit);

            return executeQuery(stmt);
        } catch (Exception e) {
            LOG.error("Failed to find audit logs by user id", e);
            throw new FloeDataAccessException("query", "audit_logs", e);
        }
    }

    @Override
    public List<AuditLog> findByEventType(String eventType, int limit) {
        String sql =
                """
            SELECT * FROM audit_logs
            WHERE event_type = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, eventType);
            stmt.setInt(2, limit);

            return executeQuery(stmt);
        } catch (Exception e) {
            LOG.error("Failed to find audit logs by event type", e);
            throw new FloeDataAccessException("query", "audit_logs", e);
        }
    }

    @Override
    public List<AuditLog> findBySeverity(String severity, int limit) {
        String sql =
                """
            SELECT * FROM audit_logs
            WHERE severity = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, severity);
            stmt.setInt(2, limit);

            return executeQuery(stmt);
        } catch (Exception e) {
            LOG.error("Failed to find audit logs by severity", e);
            throw new FloeDataAccessException("query", "audit_logs", e);
        }
    }

    @Override
    public List<AuditLog> findRecent(int days, int limit) {
        String sql =
                """
            SELECT * FROM audit_logs
            WHERE timestamp >= NOW() - INTERVAL '? days'
            ORDER BY timestamp DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, days);
            stmt.setInt(2, limit);

            return executeQuery(stmt);
        } catch (Exception e) {
            LOG.error("Failed to find recent audit logs", e);
            throw new FloeDataAccessException("query", "audit_logs", e);
        }
    }

    @Override
    public long countByEventType(String eventType, Instant start, Instant end) {
        String sql =
                """
            SELECT COUNT(*) FROM audit_logs
            WHERE event_type = ? AND timestamp BETWEEN ? AND ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, eventType);
            stmt.setTimestamp(2, Timestamp.from(start));
            stmt.setTimestamp(3, Timestamp.from(end));

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return 0;
            }
        } catch (Exception e) {
            LOG.error("Failed to count audit logs by event type", e);
            throw new FloeDataAccessException("count", "audit_logs", e);
        }
    }

    @Override
    public long deleteOlderThan(Instant olderThan) {
        // WARNING: This bypasses the immutability trigger
        // Should only be called by automated retention jobs
        String disableTriggers = "SET session_replication_role = replica";
        String enableTriggers = "SET session_replication_role = DEFAULT";
        String deleteSql = "DELETE FROM audit_logs WHERE timestamp < ?";

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                // Temporarily disable triggers
                stmt.execute(disableTriggers);
            }

            long deletedCount;
            try (PreparedStatement stmt = conn.prepareStatement(deleteSql)) {
                stmt.setTimestamp(1, Timestamp.from(olderThan));
                deletedCount = stmt.executeUpdate();
            }

            try (Statement stmt = conn.createStatement()) {
                // Re-enable triggers
                stmt.execute(enableTriggers);
            }

            conn.commit();
            LOG.info("Deleted " + deletedCount + " audit logs older than " + olderThan);
            return deletedCount;
        } catch (Exception e) {
            LOG.error("Failed to delete old audit logs", e);
            throw new FloeDataAccessException("delete", "audit_logs", e);
        }
    }

    private List<AuditLog> executeQuery(PreparedStatement stmt) throws Exception {
        List<AuditLog> results = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                results.add(mapRowToAuditLog(rs));
            }
        }
        return results;
    }

    private AuditLog mapRowToAuditLog(ResultSet rs) throws Exception {
        Long id = rs.getLong("id");

        Timestamp timestampTs = rs.getTimestamp("timestamp");
        Instant timestamp = timestampTs != null ? timestampTs.toInstant() : null;

        String eventType = rs.getString("event_type");
        String eventDescription = rs.getString("event_description");
        String severity = rs.getString("severity");
        String userId = rs.getString("user_id");
        String username = rs.getString("username");
        String authMethod = rs.getString("auth_method");
        String resource = rs.getString("resource");
        String httpMethod = rs.getString("http_method");
        String ipAddress = rs.getString("ip_address");
        String userAgent = rs.getString("user_agent");

        // Parse JSONB details
        String detailsJson = rs.getString("details");
        Map<String, Object> details = null;
        if (detailsJson != null) {
            details =
                    objectMapper.readValue(
                            detailsJson, new TypeReference<Map<String, Object>>() {});
        }

        Timestamp createdAtTs = rs.getTimestamp("created_at");
        Instant createdAt = createdAtTs != null ? createdAtTs.toInstant() : null;

        return new AuditLog(
                id,
                timestamp,
                eventType,
                eventDescription,
                severity,
                userId,
                username,
                authMethod,
                resource,
                httpMethod,
                ipAddress,
                userAgent,
                details,
                createdAt);
    }
}
