package com.floe.core.scheduler;

import com.floe.core.exception.FloeDataAccessException;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres-backed ScheduleExecutionStore implementation.
 *
 * <p>Stores schedule execution records for tracking when policies/operations were last executed.
 */
public class PostgresScheduleExecutionStore implements ScheduleExecutionStore {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresScheduleExecutionStore.class);

    private final DataSource dataSource;

    public PostgresScheduleExecutionStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Initialize the database schema if it doesn't exist. */
    public void initializeSchema() {
        String createTable =
                """
            CREATE TABLE IF NOT EXISTS schedule_executions (
                policy_id VARCHAR(255) NOT NULL,
                operation_type VARCHAR(50) NOT NULL,
                table_key VARCHAR(767) NOT NULL,
                last_run_at TIMESTAMP WITH TIME ZONE,
                next_run_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                PRIMARY KEY (policy_id, operation_type, table_key)
            )
            """;

        String createPolicyIndex =
                """
            CREATE INDEX IF NOT EXISTS idx_schedule_executions_policy
            ON schedule_executions(policy_id)
            """;

        String createTableKeyIndex =
                """
            CREATE INDEX IF NOT EXISTS idx_schedule_executions_table_key
            ON schedule_executions(table_key)
            """;

        String createNextRunAtIndex =
                """
            CREATE INDEX IF NOT EXISTS idx_schedule_executions_next_run_at
            ON schedule_executions(next_run_at)
            """;

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(createTable);
            stmt.execute(createPolicyIndex);
            stmt.execute(createTableKeyIndex);
            stmt.execute(createNextRunAtIndex);

            LOG.info("PostgresScheduleExecutionStore schema initialized");
        } catch (SQLException e) {
            throw new FloeDataAccessException("schema initialization", "schedule_executions", e);
        }
    }

    @Override
    public Optional<ScheduleExecutionRecord> getRecord(
            String policyId, String operationType, String tableKey) {
        String sql =
                """
            SELECT policy_id, operation_type, table_key, last_run_at, next_run_at
            FROM schedule_executions
            WHERE policy_id = ? AND operation_type = ? AND table_key = ?
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, policyId);
            stmt.setString(2, operationType);
            stmt.setString(3, tableKey);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRowToRecord(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "schedule_executions", e);
        }
    }

    @Override
    public void recordExecution(
            String policyId,
            String operationType,
            String tableKey,
            Instant executedAt,
            Instant nextRunAt) {
        String sql =
                """
            INSERT INTO schedule_executions
                (policy_id, operation_type, table_key, last_run_at, next_run_at, updated_at)
            VALUES (?, ?, ?, ?, ?, NOW())
            ON CONFLICT (policy_id, operation_type, table_key)
            DO UPDATE SET
                last_run_at = EXCLUDED.last_run_at,
                next_run_at = EXCLUDED.next_run_at,
                updated_at = NOW()
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, policyId);
            stmt.setString(2, operationType);
            stmt.setString(3, tableKey);
            stmt.setTimestamp(4, executedAt != null ? Timestamp.from(executedAt) : null);
            stmt.setTimestamp(5, nextRunAt != null ? Timestamp.from(nextRunAt) : null);

            stmt.executeUpdate();
            LOG.debug(
                    "Recorded execution for policy={}, op={}, table={}",
                    policyId,
                    operationType,
                    tableKey);
        } catch (SQLException e) {
            throw new FloeDataAccessException("upsert", "schedule_executions", e);
        }
    }

    @Override
    public List<ScheduleExecutionRecord> findDueRecords(Instant beforeTime) {
        String sql =
                """
            SELECT policy_id, operation_type, table_key, last_run_at, next_run_at
            FROM schedule_executions
            WHERE next_run_at IS NULL OR next_run_at <= ?
            ORDER BY next_run_at ASC NULLS FIRST
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, Timestamp.from(beforeTime));

            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "schedule_executions", e);
        }
    }

    @Override
    public List<ScheduleExecutionRecord> findByPolicy(String policyId) {
        String sql =
                """
            SELECT policy_id, operation_type, table_key, last_run_at, next_run_at
            FROM schedule_executions
            WHERE policy_id = ?
            ORDER BY table_key, operation_type
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, policyId);

            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "schedule_executions", e);
        }
    }

    @Override
    public int deleteByPolicy(String policyId) {
        String sql = "DELETE FROM schedule_executions WHERE policy_id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, policyId);
            int deleted = stmt.executeUpdate();

            LOG.info("Deleted {} schedule execution records for policy {}", deleted, policyId);
            return deleted;
        } catch (SQLException e) {
            throw new FloeDataAccessException("delete", "schedule_executions", e);
        }
    }

    @Override
    public int deleteByTable(String tableKey) {
        String sql = "DELETE FROM schedule_executions WHERE table_key = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableKey);
            int deleted = stmt.executeUpdate();

            LOG.info("Deleted {} schedule execution records for table {}", deleted, tableKey);
            return deleted;
        } catch (SQLException e) {
            throw new FloeDataAccessException("delete", "schedule_executions", e);
        }
    }

    @Override
    public void clear() {
        String sql = "DELETE FROM schedule_executions";

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(sql);
            LOG.info("Cleared {} schedule execution records", deleted);
        } catch (SQLException e) {
            throw new FloeDataAccessException("delete", "schedule_executions", e);
        }
    }

    @Override
    public long count() {
        String sql = "SELECT COUNT(*) FROM schedule_executions";

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "schedule_executions", e);
        }
    }

    private List<ScheduleExecutionRecord> executeQuery(PreparedStatement stmt) throws SQLException {
        List<ScheduleExecutionRecord> results = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                results.add(mapRowToRecord(rs));
            }
        }
        return results;
    }

    private ScheduleExecutionRecord mapRowToRecord(ResultSet rs) throws SQLException {
        String policyId = rs.getString("policy_id");
        String operationType = rs.getString("operation_type");
        String tableKey = rs.getString("table_key");

        Timestamp lastRunAtTs = rs.getTimestamp("last_run_at");
        Instant lastRunAt = lastRunAtTs != null ? lastRunAtTs.toInstant() : null;

        Timestamp nextRunAtTs = rs.getTimestamp("next_run_at");
        Instant nextRunAt = nextRunAtTs != null ? nextRunAtTs.toInstant() : null;

        return new ScheduleExecutionRecord(policyId, operationType, tableKey, lastRunAt, nextRunAt);
    }
}
