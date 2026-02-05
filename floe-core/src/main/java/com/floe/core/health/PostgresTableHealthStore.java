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

package com.floe.core.health;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.floe.core.exception.FloeDataAccessException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres-backed TableHealthStore implementation.
 *
 * <p>Stores health reports in a Postgres table with JSONB for the full report data, enabling
 * efficient queries for health history and latest reports.
 */
public class PostgresTableHealthStore implements TableHealthStore {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresTableHealthStore.class);

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    public PostgresTableHealthStore(DataSource dataSource) {
        this.dataSource = dataSource;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new Jdk8Module());
        this.objectMapper.registerModule(new JavaTimeModule());
        // Write dates as ISO-8601 strings for readability
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        // For records: use fields directly instead of getters to avoid JavaBean-style
        // getters like getTableName() being serialized as "tableName" alongside "table"
        this.objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        this.objectMapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
        this.objectMapper.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);
    }

    /** Initialize the database schema if it doesn't exist. */
    public void initializeSchema() {
        String createTable =
                """
            CREATE TABLE IF NOT EXISTS table_health_reports (
                id UUID PRIMARY KEY,
                catalog VARCHAR(255) NOT NULL,
                namespace VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                assessed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                report JSONB NOT NULL
            )
            """;

        String createTableIndex =
                """
            CREATE INDEX IF NOT EXISTS idx_health_reports_table
            ON table_health_reports(catalog, namespace, table_name, assessed_at DESC)
            """;

        String createAssessedAtIndex =
                """
            CREATE INDEX IF NOT EXISTS idx_health_reports_assessed_at
            ON table_health_reports(assessed_at DESC)
            """;

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(createTable);
            stmt.execute(createTableIndex);
            stmt.execute(createAssessedAtIndex);

            LOG.info("PostgresTableHealthStore schema initialized");
        } catch (SQLException e) {
            throw new FloeDataAccessException("initialize", "health store schema", e);
        }
    }

    @Override
    public void pruneOlderThan(long cutoffEpochMillis) {
        String sql = "DELETE FROM table_health_reports WHERE assessed_at < ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, new Timestamp(cutoffEpochMillis));
            int deleted = stmt.executeUpdate();
            LOG.debug("Pruned {} health reports older than cutoff", deleted);
        } catch (SQLException e) {
            throw new FloeDataAccessException("delete", "health reports older than cutoff", e);
        }
    }

    @Override
    public void save(HealthReport report) {
        String sql =
                """
            INSERT INTO table_health_reports
            (id, catalog, namespace, table_name, assessed_at, report)
            VALUES (?, ?, ?, ?, ?, ?::jsonb)
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, UUID.randomUUID());
            stmt.setString(2, report.tableIdentifier().catalog());
            stmt.setString(3, report.tableIdentifier().namespace());
            stmt.setString(4, report.tableIdentifier().table());
            stmt.setTimestamp(5, Timestamp.from(report.assessedAt()));
            stmt.setString(6, toJson(report));

            stmt.executeUpdate();
            LOG.debug(
                    "Saved health report for table {}", report.tableIdentifier().toQualifiedName());
        } catch (SQLException e) {
            throw new FloeDataAccessException(
                    "save",
                    "health report for table " + report.tableIdentifier().toQualifiedName(),
                    e);
        }
    }

    @Override
    public List<HealthReport> findLatest(int limit) {
        String sql =
                """
            SELECT catalog, namespace, table_name, assessed_at, report
            FROM (
                SELECT DISTINCT ON (catalog, namespace, table_name)
                    catalog, namespace, table_name, assessed_at, report
                FROM table_health_reports
                ORDER BY catalog, namespace, table_name, assessed_at DESC
            ) latest
            ORDER BY assessed_at DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, limit);
            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "latest health reports", e);
        }
    }

    @Override
    public List<HealthReport> findHistory(
            String catalog, String namespace, String tableName, int limit) {
        String sql =
                """
            SELECT catalog, namespace, table_name, assessed_at, report
            FROM table_health_reports
            WHERE catalog = ? AND namespace = ? AND table_name = ?
            ORDER BY assessed_at DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, namespace);
            stmt.setString(3, tableName);
            stmt.setInt(4, limit);
            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException(
                    "query",
                    "health history for table " + catalog + "." + namespace + "." + tableName,
                    e);
        }
    }

    @Override
    public void pruneHistory(String catalog, String namespace, String tableName, int maxReports) {
        if (maxReports <= 0) {
            return;
        }

        String sql =
                """
            DELETE FROM table_health_reports
            WHERE id IN (
                SELECT id FROM table_health_reports
                WHERE catalog = ? AND namespace = ? AND table_name = ?
                ORDER BY assessed_at DESC
                OFFSET ?
            )
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, namespace);
            stmt.setString(3, tableName);
            stmt.setInt(4, maxReports);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new FloeDataAccessException(
                    "prune",
                    "health history for table " + catalog + "." + namespace + "." + tableName,
                    e);
        }
    }

    @Override
    public long count() {
        String sql = "SELECT COUNT(*) FROM table_health_reports";

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new FloeDataAccessException("count", "health reports", e);
        }
    }

    @Override
    public void clear() {
        String sql = "DELETE FROM table_health_reports";

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(sql);
            LOG.info("Cleared {} health reports", deleted);
        } catch (SQLException e) {
            throw new FloeDataAccessException("clear", "health reports", e);
        }
    }

    /**
     * Execute a query and map results to HealthReport list.
     *
     * @param stmt the prepared statement to execute
     * @return list of health reports
     * @throws SQLException if query fails
     */
    private List<HealthReport> executeQuery(PreparedStatement stmt) throws SQLException {
        List<HealthReport> results = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                results.add(mapRowToReport(rs));
            }
        }
        return results;
    }

    /**
     * Map a ResultSet row to a HealthReport.
     *
     * @param rs the result set positioned at the row to map
     * @return the mapped health report
     * @throws SQLException if reading fails or JSON parsing fails
     */
    private HealthReport mapRowToReport(ResultSet rs) throws SQLException {
        try {
            String reportJson = rs.getString("report");
            return fromJson(reportJson, HealthReport.class);
        } catch (JsonProcessingException e) {
            throw new SQLException("Failed to deserialize health report", e);
        }
    }

    /**
     * Serialize an object to JSON string.
     *
     * @param obj the object to serialize
     * @return JSON string
     */
    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new FloeDataAccessException(
                    "serialize", "health report to JSON", new RuntimeException(e));
        }
    }

    /**
     * Deserialize a JSON string to an object.
     *
     * @param json the JSON string
     * @param clazz the target class
     * @param <T> the target type
     * @return the deserialized object
     * @throws JsonProcessingException if parsing fails
     */
    private <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException {
        return objectMapper.readValue(json, clazz);
    }
}
