package com.floe.core.operation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.floe.core.exception.FloeConfigurationException;
import com.floe.core.exception.FloeDataAccessException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Postgres-backed OperationStore implementation. */
public class PostgresOperationStore implements OperationStore {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresOperationStore.class);

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    public PostgresOperationStore(DataSource dataSource) {
        this.dataSource = dataSource;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new Jdk8Module());
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /** Initialize the database schema if it doesn't exist. */
    public void initializeSchema() {
        String createTable =
                """
            CREATE TABLE IF NOT EXISTS maintenance_operations (
                id UUID PRIMARY KEY,
                catalog VARCHAR(255) NOT NULL,
                namespace VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                policy_name VARCHAR(255),
                policy_id UUID,
                engine_type VARCHAR(50),
                execution_id VARCHAR(255),
                schedule_id VARCHAR(255),
                policy_version VARCHAR(255),
                status VARCHAR(50) NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                completed_at TIMESTAMP WITH TIME ZONE,
                results JSONB,
                normalized_metrics JSONB,
                error_message TEXT,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )
            """;

        String alterTableAddColumns =
                """
            ALTER TABLE maintenance_operations
            ADD COLUMN IF NOT EXISTS engine_type VARCHAR(50),
            ADD COLUMN IF NOT EXISTS execution_id VARCHAR(255),
            ADD COLUMN IF NOT EXISTS schedule_id VARCHAR(255),
            ADD COLUMN IF NOT EXISTS policy_version VARCHAR(255),
            ADD COLUMN IF NOT EXISTS normalized_metrics JSONB
            """;

        String createTableIndex =
                """
            CREATE INDEX IF NOT EXISTS idx_operations_table
            ON maintenance_operations(catalog, namespace, table_name)
            """;

        String createStatusIndex =
                """
            CREATE INDEX IF NOT EXISTS idx_operations_status
            ON maintenance_operations(status)
            """;

        String createStartedAtIndex =
                """
            CREATE INDEX IF NOT EXISTS idx_operations_started_at
            ON maintenance_operations(started_at DESC)
            """;

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(createTable);
            stmt.execute(alterTableAddColumns);
            stmt.execute(createTableIndex);
            stmt.execute(createStatusIndex);
            stmt.execute(createStartedAtIndex);

            LOG.info("PostgresOperationStore schema initialized");
        } catch (SQLException e) {
            throw new FloeDataAccessException("initialize", "operation store schema", e);
        }
    }

    @Override
    public OperationRecord createOperation(OperationRecord record) {
        OperationRecord toStore = record;

        if (record.id() == null) {
            toStore = record.toBuilder().id(UUID.randomUUID()).build();
        }
        if (record.createdAt() == null) {
            toStore = toStore.toBuilder().createdAt(Instant.now()).build();
        }

        String sql =
                """
            INSERT INTO maintenance_operations
            (id, catalog, namespace, table_name, policy_name, policy_id,
             engine_type, execution_id, schedule_id, policy_version,
             status, started_at, completed_at, results, normalized_metrics, error_message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, toStore.id());
            stmt.setString(2, toStore.catalog());
            stmt.setString(3, toStore.namespace());
            stmt.setString(4, toStore.tableName());
            stmt.setString(5, toStore.policyName());
            stmt.setObject(6, toStore.policyId());
            stmt.setString(7, toStore.engineType());
            stmt.setString(8, toStore.executionId());
            stmt.setString(9, toStore.scheduleId());
            stmt.setString(10, toStore.policyVersion());
            stmt.setString(11, toStore.status().name());
            stmt.setTimestamp(12, Timestamp.from(toStore.startedAt()));
            stmt.setTimestamp(
                    13,
                    toStore.completedAt() != null ? Timestamp.from(toStore.completedAt()) : null);
            stmt.setString(14, toJson(toStore.results()));
            stmt.setString(15, toJson(toStore.normalizedMetrics()));
            stmt.setString(16, toStore.errorMessage());
            stmt.setTimestamp(17, Timestamp.from(toStore.createdAt()));

            stmt.executeUpdate();
            LOG.debug("Created operation record: {}", toStore.id());

            return toStore;
        } catch (SQLException e) {
            throw new FloeDataAccessException("create", "operation record id=" + toStore.id(), e);
        }
    }

    @Override
    public void updateStatus(UUID id, OperationStatus status, OperationResults results) {
        String sql =
                """
            UPDATE maintenance_operations
            SET status = ?, results = ?::jsonb, normalized_metrics = ?::jsonb, completed_at = ?
            WHERE id = ?
            """;

        Timestamp completedAt = status.isTerminal() ? Timestamp.from(Instant.now()) : null;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, status.name());
            stmt.setString(2, toJson(results));
            stmt.setString(3, toJson(results != null ? results.aggregatedMetrics() : null));
            stmt.setTimestamp(4, completedAt);
            stmt.setObject(5, id);

            int updated = stmt.executeUpdate();
            if (updated == 0) {
                return;
            }

            findById(id);
        } catch (SQLException e) {
            throw new FloeDataAccessException("update status", "operation id=" + id, e);
        }
    }

    @Override
    public void markFailed(UUID id, String errorMessage) {
        String sql =
                """
            UPDATE maintenance_operations
            SET status = ?, error_message = ?, completed_at = ?
            WHERE id = ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, OperationStatus.FAILED.name());
            stmt.setString(2, errorMessage);
            stmt.setTimestamp(3, Timestamp.from(Instant.now()));
            stmt.setObject(4, id);

            int updated = stmt.executeUpdate();
            if (updated == 0) {
                return;
            }

            findById(id);
        } catch (SQLException e) {
            throw new FloeDataAccessException("mark failed", "operation id=" + id, e);
        }
    }

    @Override
    public Optional<OperationRecord> markRunning(UUID id) {
        String sql =
                """
            UPDATE maintenance_operations
            SET status = ?, started_at = ?
            WHERE id = ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, OperationStatus.RUNNING.name());
            stmt.setTimestamp(2, Timestamp.from(Instant.now()));
            stmt.setObject(3, id);

            int updated = stmt.executeUpdate();
            if (updated == 0) {
                return Optional.empty();
            }

            return findById(id);
        } catch (SQLException e) {
            throw new FloeDataAccessException("mark running", "operation id=" + id, e);
        }
    }

    @Override
    public void updatePolicyInfo(UUID id, String policyName, UUID policyId, String policyVersion) {
        String sql =
                """
            UPDATE maintenance_operations
            SET policy_name = ?, policy_id = ?, policy_version = ?
            WHERE id = ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, policyName);
            stmt.setObject(2, policyId);
            stmt.setString(3, policyVersion);
            stmt.setObject(4, id);

            int updated = stmt.executeUpdate();
            if (updated == 0) {
                return;
            }

            findById(id);
        } catch (SQLException e) {
            throw new FloeDataAccessException("update policy info", "operation id=" + id, e);
        }
    }

    @Override
    public Optional<OperationRecord> findById(UUID id) {
        String sql = "SELECT * FROM maintenance_operations WHERE id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRowToRecord(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "operation id=" + id, e);
        }
    }

    @Override
    public List<OperationRecord> findByTable(
            String catalog, String namespace, String tableName, int limit) {
        String sql =
                """
            SELECT * FROM maintenance_operations
            WHERE catalog = ? AND namespace = ? AND table_name = ?
            ORDER BY started_at DESC
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
                    "operations for table " + catalog + "." + namespace + "." + tableName,
                    e);
        }
    }

    @Override
    public List<OperationRecord> findRecent(int limit) {
        String sql =
                """
            SELECT * FROM maintenance_operations
            ORDER BY started_at DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, limit);
            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "recent operations", e);
        }
    }

    @Override
    public List<OperationRecord> findRecent(int limit, int offset) {
        String sql =
                """
            SELECT * FROM maintenance_operations
            ORDER BY started_at DESC
            LIMIT ? OFFSET ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, limit);
            stmt.setInt(2, offset);
            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "recent operations (paginated)", e);
        }
    }

    @Override
    public List<OperationRecord> findByStatus(OperationStatus status, int limit) {
        String sql =
                """
            SELECT * FROM maintenance_operations
            WHERE status = ?
            ORDER BY started_at DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, status.name());
            stmt.setInt(2, limit);

            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "operations by status=" + status, e);
        }
    }

    @Override
    public List<OperationRecord> findByStatus(OperationStatus status, int limit, int offset) {
        String sql =
                """
            SELECT * FROM maintenance_operations
            WHERE status = ?
            ORDER BY started_at DESC
            LIMIT ? OFFSET ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, status.name());
            stmt.setInt(2, limit);
            stmt.setInt(3, offset);

            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException(
                    "query", "operations by status=" + status + " (paginated)", e);
        }
    }

    @Override
    public long countByStatus(OperationStatus status) {
        String sql = "SELECT COUNT(*) FROM maintenance_operations WHERE status = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, status.name());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return 0;
            }
        } catch (SQLException e) {
            throw new FloeDataAccessException("count", "operations by status=" + status, e);
        }
    }

    @Override
    public List<OperationRecord> findInTimeRange(Instant start, Instant end, int limit) {
        String sql =
                """
            SELECT * FROM maintenance_operations
            WHERE started_at >= ? AND started_at < ?
            ORDER BY started_at DESC
            LIMIT ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, Timestamp.from(start));
            stmt.setTimestamp(2, Timestamp.from(end));
            stmt.setInt(3, limit);

            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "operations in time range", e);
        }
    }

    // ========== Aggregation Operations ==========

    @Override
    public OperationStats getStats(Duration window) {
        Instant windowStart = Instant.now().minus(window);
        Instant windowEnd = Instant.now();

        String sql =
                """
            SELECT status, COUNT(*) as count
            FROM maintenance_operations
            WHERE started_at >= ?
            GROUP BY status
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, Timestamp.from(windowStart));

            Map<OperationStatus, Long> countsByStatus = new EnumMap<>(OperationStatus.class);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    OperationStatus status = OperationStatus.valueOf(rs.getString("status"));
                    long count = rs.getLong("count");
                    countsByStatus.put(status, count);
                }
            }

            return buildStats(countsByStatus, windowStart, windowEnd);
        } catch (SQLException e) {
            throw new FloeDataAccessException("aggregate", "operation stats", e);
        }
    }

    @Override
    public OperationStats getStatsForTable(
            String catalog, String namespace, String tableName, Duration window) {
        Instant windowStart = Instant.now().minus(window);
        Instant windowEnd = Instant.now();

        String sql =
                """
            SELECT status, COUNT(*) as count
            FROM maintenance_operations
            WHERE catalog = ? AND namespace = ? AND table_name = ? AND started_at >= ?
            GROUP BY status
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, namespace);
            stmt.setString(3, tableName);
            stmt.setTimestamp(4, Timestamp.from(windowStart));

            Map<OperationStatus, Long> countsByStatus = new EnumMap<>(OperationStatus.class);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    OperationStatus status = OperationStatus.valueOf(rs.getString("status"));
                    long count = rs.getLong("count");
                    countsByStatus.put(status, count);
                }
            }

            return buildStats(countsByStatus, windowStart, windowEnd);
        } catch (SQLException e) {
            throw new FloeDataAccessException(
                    "aggregate",
                    "operation stats for table " + catalog + "." + namespace + "." + tableName,
                    e);
        }
    }

    @Override
    public Optional<Instant> findLastOperationTime(
            String catalog, String namespace, String tableName, String operationType) {
        String sql =
                """
            SELECT completed_at, started_at FROM maintenance_operations
            WHERE catalog = ? AND namespace = ? AND table_name = ?
              AND results IS NOT NULL
              AND results->'operations' @> ?::jsonb
            ORDER BY started_at DESC
            LIMIT 1
            """;

        // Build the JSON filter for operation type
        String operationTypeFilter = "[{\"operationType\":\"" + operationType + "\"}]";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, namespace);
            stmt.setString(3, tableName);
            stmt.setString(4, operationTypeFilter);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Timestamp completedAt = rs.getTimestamp("completed_at");
                    Timestamp startedAt = rs.getTimestamp("started_at");
                    return Optional.of(
                            completedAt != null ? completedAt.toInstant() : startedAt.toInstant());
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new FloeDataAccessException(
                    "query",
                    "last operation time for " + catalog + "." + namespace + "." + tableName,
                    e);
        }
    }

    private OperationStats buildStats(
            Map<OperationStatus, Long> countsByStatus, Instant windowStart, Instant windowEnd) {
        long total = countsByStatus.values().stream().mapToLong(Long::longValue).sum();

        return OperationStats.builder()
                .totalOperations(total)
                .successCount(countsByStatus.getOrDefault(OperationStatus.SUCCESS, 0L))
                .failedCount(countsByStatus.getOrDefault(OperationStatus.FAILED, 0L))
                .partialFailureCount(
                        countsByStatus.getOrDefault(OperationStatus.PARTIAL_FAILURE, 0L))
                .runningCount(
                        countsByStatus.getOrDefault(OperationStatus.RUNNING, 0L)
                                + countsByStatus.getOrDefault(OperationStatus.PENDING, 0L))
                .noPolicyCount(countsByStatus.getOrDefault(OperationStatus.NO_POLICY, 0L))
                .noOperationsCount(countsByStatus.getOrDefault(OperationStatus.NO_OPERATIONS, 0L))
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .zeroChangeRuns(0)
                .consecutiveFailures(0)
                .consecutiveZeroChangeRuns(0)
                .averageBytesRewritten(0)
                .averageFilesRewritten(0)
                .lastRunAt(null)
                .build();
    }

    // ========== Maintenance Operations ==========

    @Override
    public int deleteOlderThan(Duration olderThan) {
        Instant cutoff = Instant.now().minus(olderThan);

        String sql = "DELETE FROM maintenance_operations WHERE started_at < ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, Timestamp.from(cutoff));
            int deleted = stmt.executeUpdate();

            LOG.info("Deleted {} operation records older than {}", deleted, cutoff);
            return deleted;
        } catch (SQLException e) {
            throw new FloeDataAccessException("delete", "old operations", e);
        }
    }

    @Override
    public long count() {
        String sql = "SELECT COUNT(*) FROM maintenance_operations";

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new FloeDataAccessException("count", "operations", e);
        }
    }

    @Override
    public void clear() {
        String sql = "DELETE FROM maintenance_operations";

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(sql);
            LOG.info("Cleared {} operation records", deleted);
        } catch (SQLException e) {
            throw new FloeDataAccessException("clear", "operations", e);
        }
    }

    // ========== Helper Methods ==========

    /**
     * Execute a query and map results to OperationRecord list.
     *
     * @param stmt the prepared statement to execute
     * @return list of operation records
     * @throws SQLException if query fails
     */
    private List<OperationRecord> executeQuery(PreparedStatement stmt) throws SQLException {
        List<OperationRecord> results = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                results.add(mapRowToRecord(rs));
            }
        }
        return results;
    }

    /**
     * Map a ResultSet row to an OperationRecord.
     *
     * @param rs the result set positioned at the row to map
     * @return the mapped operation record
     * @throws SQLException if reading fails or JSON parsing fails
     */
    private OperationRecord mapRowToRecord(ResultSet rs) throws SQLException {
        try {
            UUID id = rs.getObject("id", UUID.class);
            String catalog = rs.getString("catalog");
            String namespace = rs.getString("namespace");
            String tableName = rs.getString("table_name");
            String policyName = rs.getString("policy_name");
            UUID policyId = rs.getObject("policy_id", UUID.class);
            String engineType = rs.getString("engine_type");
            String executionId = rs.getString("execution_id");
            String scheduleId = rs.getString("schedule_id");
            String policyVersion = rs.getString("policy_version");
            OperationStatus status = OperationStatus.valueOf(rs.getString("status"));

            Timestamp startedAtTs = rs.getTimestamp("started_at");
            Instant startedAt = startedAtTs != null ? startedAtTs.toInstant() : null;

            Timestamp completedAtTs = rs.getTimestamp("completed_at");
            Instant completedAt = completedAtTs != null ? completedAtTs.toInstant() : null;

            String resultsJson = rs.getString("results");
            OperationResults results = fromJson(resultsJson, OperationResults.class);
            String normalizedJson = rs.getString("normalized_metrics");
            Map<String, Object> normalizedMetrics = fromJson(normalizedJson, Map.class);

            String errorMessage = rs.getString("error_message");

            Timestamp createdAtTs = rs.getTimestamp("created_at");
            Instant createdAt = createdAtTs != null ? createdAtTs.toInstant() : null;

            return new OperationRecord(
                    id,
                    catalog,
                    namespace,
                    tableName,
                    policyName,
                    policyId,
                    engineType,
                    executionId,
                    scheduleId,
                    policyVersion,
                    status,
                    startedAt,
                    completedAt,
                    results,
                    normalizedMetrics,
                    errorMessage,
                    createdAt);
        } catch (JsonProcessingException e) {
            throw new SQLException("Failed to deserialize operation record", e);
        }
    }

    /**
     * Serialize an object to JSON string.
     *
     * @param obj the object to serialize
     * @return JSON string, or null if obj is null
     */
    private String toJson(Object obj) {
        if (obj == null) return null;
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new FloeConfigurationException(
                    "JSON serialization", obj.getClass().getSimpleName(), e);
        }
    }

    /**
     * Deserialize a JSON string to an object.
     *
     * @param json the JSON string
     * @param clazz the target class
     * @param <T> the target type
     * @return the deserialized object, or null if json is null/blank
     * @throws JsonProcessingException if parsing fails
     */
    private <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException {
        if (json == null || json.isBlank()) return null;
        return objectMapper.readValue(json, clazz);
    }
}
