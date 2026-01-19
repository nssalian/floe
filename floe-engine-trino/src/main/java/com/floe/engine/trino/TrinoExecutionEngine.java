package com.floe.engine.trino;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.*;
import com.floe.core.maintenance.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execution engine that runs Iceberg maintenance operations via Trino SQL.
 *
 * <p>Uses Trino's ALTER TABLE EXECUTE commands:
 *
 * <ul>
 *   <li>optimize (compaction)
 *   <li>expire_snapshots
 *   <li>remove_orphan_files
 *   <li>optimize_manifests
 * </ul>
 */
public class TrinoExecutionEngine implements ExecutionEngine {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoExecutionEngine.class);

    /** Maximum connection pool size */
    private static final int MAX_POOL_SIZE = 10;

    /** Connection timeout in milliseconds */
    private static final long CONNECTION_TIMEOUT_MS = 30_000;

    /** Idle timeout in milliseconds (10 minutes) */
    private static final long IDLE_TIMEOUT_MS = 600_000;

    private final TrinoEngineConfig config;
    private final ConcurrentMap<String, ExecutionTracker> activeExecutions;
    private final AtomicBoolean shutdown;
    private final HikariDataSource dataSource;

    public TrinoExecutionEngine(TrinoEngineConfig config) {
        this.config = config;
        this.activeExecutions = new ConcurrentHashMap<>();
        this.shutdown = new AtomicBoolean(false);
        this.dataSource = createDataSource(config);

        LOG.info(
                "TrinoExecutionEngine initialized: url={}, catalog={}, poolSize={}",
                config.jdbcUrl(),
                config.catalog(),
                MAX_POOL_SIZE);
    }

    private HikariDataSource createDataSource(TrinoEngineConfig config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.jdbcUrl());
        hikariConfig.setUsername(config.username());
        if (config.password() != null && !config.password().isBlank()) {
            hikariConfig.setPassword(config.password());
        }
        hikariConfig.setPoolName("trino-engine-pool");
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setMaximumPoolSize(MAX_POOL_SIZE);
        hikariConfig.setConnectionTimeout(CONNECTION_TIMEOUT_MS);
        hikariConfig.setIdleTimeout(IDLE_TIMEOUT_MS);
        hikariConfig.setAutoCommit(true);

        return new HikariDataSource(hikariConfig);
    }

    @Override
    public String getEngineName() {
        return "Trino Execution Engine";
    }

    @Override
    public EngineType getEngineType() {
        return EngineType.TRINO;
    }

    @Override
    public boolean isOperational() {
        if (shutdown.get()) {
            return false;
        }
        try (Connection conn = getConnection()) {
            return conn.isValid(10);
        } catch (Exception e) {
            LOG.warn("Trino health check failed", e);
            return false;
        }
    }

    @Override
    public EngineCapabilities getCapabilities() {
        return EngineCapabilities.trino();
    }

    @Override
    public CompletableFuture<ExecutionResult> execute(
            TableIdentifier table, MaintenanceOperation operation, ExecutionContext context) {
        if (shutdown.get()) {
            return CompletableFuture.completedFuture(
                    ExecutionResult.failure(
                            context.executionId(),
                            table,
                            operation.getType(),
                            Instant.now(),
                            Instant.now(),
                            "Engine is shutdown",
                            ""));
        }

        String executionId = context.executionId();
        LOG.info("Submitting execution {}: {} on {}", executionId, operation.getType(), table);

        ExecutionTracker tracker = new ExecutionTracker(executionId, table, operation.getType());
        activeExecutions.put(executionId, tracker);

        CompletableFuture<ExecutionResult> future =
                CompletableFuture.supplyAsync(
                        () -> {
                            Instant startTime = Instant.now();
                            tracker.setStatus(ExecutionStatus.RUNNING);

                            try {
                                String sql = buildSql(table, operation);
                                LOG.debug("Executing SQL: {}", sql);

                                Map<String, Object> metrics =
                                        executeQuery(sql, context.timeoutSeconds());

                                Instant endTime = Instant.now();
                                tracker.setStatus(ExecutionStatus.SUCCEEDED);

                                LOG.info(
                                        "Execution {} completed successfully in {}ms",
                                        executionId,
                                        Duration.between(startTime, endTime).toMillis());

                                return ExecutionResult.success(
                                        executionId,
                                        table,
                                        operation.getType(),
                                        startTime,
                                        endTime,
                                        metrics);
                            } catch (Exception e) {
                                Instant endTime = Instant.now();
                                tracker.setStatus(ExecutionStatus.FAILED);
                                String errorMessage =
                                        e.getMessage() != null ? e.getMessage() : "Unknown error";
                                String stackTrace = getStackTrace(e);
                                LOG.error("Execution {} failed: {}", executionId, errorMessage, e);
                                return ExecutionResult.failure(
                                        executionId,
                                        table,
                                        operation.getType(),
                                        startTime,
                                        endTime,
                                        errorMessage,
                                        stackTrace);
                            } finally {
                                activeExecutions.remove(executionId);
                            }
                        });

        tracker.setFuture(future);
        return future;
    }

    @Override
    public Optional<ExecutionStatus> getStatus(String executionId) {
        ExecutionTracker tracker = activeExecutions.get(executionId);
        if (tracker == null) {
            return Optional.empty();
        }
        return Optional.of(tracker.getStatus());
    }

    @Override
    public boolean cancelExecution(String executionId) {
        ExecutionTracker tracker = activeExecutions.get(executionId);
        if (tracker == null) {
            return false;
        }

        // Cancel the future
        if (tracker.getFuture() != null) {
            tracker.getFuture().cancel(true);
        }

        tracker.setStatus(ExecutionStatus.CANCELLED);
        activeExecutions.remove(executionId);
        LOG.info("Execution {} cancelled", executionId);
        return true;
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            LOG.info("Shutting down TrinoExecutionEngine...");

            // Cancel all active executions
            for (ExecutionTracker tracker : activeExecutions.values()) {
                if (tracker.getFuture() != null) {
                    tracker.getFuture().cancel(true);
                }
            }
            activeExecutions.clear();

            // Close the connection pool
            if (dataSource != null && !dataSource.isClosed()) {
                dataSource.close();
                LOG.info("Trino connection pool closed.");
            }

            LOG.info("TrinoExecutionEngine shutdown complete.");
        }
    }

    /** Builds the appropriate SQL command for the given operation. */
    private String buildSql(TableIdentifier table, MaintenanceOperation operation) {
        String fullTableName =
                String.format(
                        "%s.%s.%s", config.catalog(), table.namespace(), table.getTableName());

        return switch (operation.getType()) {
            case REWRITE_DATA_FILES ->
                    buildOptimizeSql(fullTableName, (RewriteDataFilesOperation) operation);
            case EXPIRE_SNAPSHOTS ->
                    buildExpireSnapshotsSql(fullTableName, (ExpireSnapshotsOperation) operation);
            case ORPHAN_CLEANUP ->
                    buildRemoveOrphanFilesSql(fullTableName, (OrphanCleanupOperation) operation);
            case REWRITE_MANIFESTS -> buildOptimizeManifestsSql(fullTableName);
        };
    }

    private String buildOptimizeSql(String tableName, RewriteDataFilesOperation op) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(tableName).append(" EXECUTE optimize");

        // Add file size threshold if specified
        if (op.targetFileSizeBytes().isPresent()) {
            long thresholdMb = op.targetFileSizeBytes().get() / (1024L * 1024L);
            sql.append("(file_size_threshold => '").append(thresholdMb).append("MB')");
        }

        // Add WHERE clause if filter specified
        if (op.filter().isPresent()) {
            sql.append(" WHERE ").append(op.filter());
        }

        return sql.toString();
    }

    private String buildExpireSnapshotsSql(String tableName, ExpireSnapshotsOperation op) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(tableName).append(" EXECUTE expire_snapshots(");

        List<String> params = new ArrayList<>();

        // retention_threshold
        if (op.maxSnapshotAge().isPresent()) {
            long days = op.maxSnapshotAge().get().toDays();
            if (days < 1) {
                long hours = op.maxSnapshotAge().get().toHours();
                params.add("retention_threshold => '" + hours + "h'");
            } else {
                params.add("retention_threshold => '" + days + "d'");
            }
        } else {
            params.add("retention_threshold => '7d'");
        }

        // retain_last
        if (op.retainLast() > 0) {
            params.add("retain_last => " + op.retainLast());
        }

        // clean_expired_metadata
        if (op.cleanExpiredMetadata()) {
            params.add("clean_expired_metadata => true");
        }

        sql.append(String.join(", ", params));
        sql.append(")");

        return sql.toString();
    }

    private String buildRemoveOrphanFilesSql(String tableName, OrphanCleanupOperation op) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(tableName).append(" EXECUTE remove_orphan_files");

        if (op.olderThan() != null) {
            long days = op.olderThan().toDays();
            if (days < 1) {
                long hours = op.olderThan().toHours();
                sql.append("(retention_threshold => '").append(hours).append("h')");
            } else {
                sql.append("(retention_threshold => '").append(days).append("d')");
            }
        } else {
            sql.append("(retention_threshold => '7d')");
        }

        return sql.toString();
    }

    private String buildOptimizeManifestsSql(String tableName) {
        return "ALTER TABLE " + tableName + " EXECUTE optimize_manifests";
    }

    /** Executes the given SQL and returns metrics. */
    private Map<String, Object> executeQuery(String sql, int timeoutSeconds) throws SQLException {
        Map<String, Object> metrics = new HashMap<>();

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(timeoutSeconds);

            // Set any session properties
            for (Map.Entry<String, String> entry : config.sessionProperties().entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                validateSessionProperty(key, value);
                stmt.execute("SET SESSION " + key + " = " + value);
            }

            long startTime = System.currentTimeMillis();
            boolean hasResults = stmt.execute(sql);
            long duration = System.currentTimeMillis() - startTime;

            metrics.put("durationMs", duration);
            metrics.put("sql", sql);

            if (hasResults) {
                try (ResultSet rs = stmt.getResultSet()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    while (rs.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            String colName = metaData.getColumnName(i);
                            Object value = rs.getObject(i);
                            metrics.put(colName, value);
                        }
                    }
                }
            }
        }

        return metrics;
    }

    /** Validates session property key and value to prevent SQL injection. */
    private void validateSessionProperty(String key, String value) {
        // Key must be a valid identifier: letters, digits, underscores, dots
        if (key == null || !key.matches("^[a-zA-Z][a-zA-Z0-9_.]*$")) {
            throw new IllegalArgumentException(
                    "Invalid session property key: " + key + ". Must be a valid identifier.");
        }

        // Value must be safe: alphanumeric/underscore/dot, or a properly quoted string, or a number
        if (value == null) {
            throw new IllegalArgumentException(
                    "Session property value cannot be null for key: " + key);
        }

        // Allow: numbers, booleans, identifiers, or single-quoted strings without embedded quotes
        boolean isNumber = value.matches("^-?[0-9]+(\\.[0-9]+)?$");
        boolean isBoolean = value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false");
        boolean isIdentifier = value.matches("^[a-zA-Z][a-zA-Z0-9_.]*$");
        boolean isQuotedString = value.matches("^'[^']*'$");

        if (!isNumber && !isBoolean && !isIdentifier && !isQuotedString) {
            throw new IllegalArgumentException(
                    "Invalid session property value for key '"
                            + key
                            + "': "
                            + value
                            + ". Must be a number, boolean, identifier, or single-quoted string.");
        }
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    /** Tracks the status and future of an execution. */
    private static class ExecutionTracker {

        private final String executionId;
        private final TableIdentifier table;
        private final MaintenanceOperation.Type operationType;
        private volatile ExecutionStatus status;
        private volatile CompletableFuture<ExecutionResult> future;

        ExecutionTracker(
                String executionId,
                TableIdentifier table,
                MaintenanceOperation.Type operationType) {
            this.executionId = executionId;
            this.table = table;
            this.operationType = operationType;
            this.status = ExecutionStatus.PENDING;
        }

        ExecutionStatus getStatus() {
            return status;
        }

        void setStatus(ExecutionStatus status) {
            this.status = status;
        }

        CompletableFuture<ExecutionResult> getFuture() {
            return future;
        }

        void setFuture(CompletableFuture<ExecutionResult> future) {
            this.future = future;
        }

        @Override
        public String toString() {
            return String.format(
                    "ExecutionTracker{executionId='%s', table=%s, operationType=%s, status=%s}",
                    executionId, table, operationType, status);
        }
    }
}
