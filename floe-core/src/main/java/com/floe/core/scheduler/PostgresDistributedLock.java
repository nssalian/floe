package com.floe.core.scheduler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Postgres-based distributed lock using advisory locks. */
public class PostgresDistributedLock implements DistributedLock {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDistributedLock.class);

    private final DataSource dataSource;
    private final ConcurrentMap<String, PostgresLockHandle> activeLocks = new ConcurrentHashMap<>();

    public PostgresDistributedLock(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Optional<LockHandle> tryAcquire(String lockName, Duration ttl) {
        // Check if we already hold this lock
        PostgresLockHandle existing = activeLocks.get(lockName);
        if (existing != null && existing.isValid()) {
            LOG.debug("Already holding lock: {}", lockName);
            return Optional.of(existing);
        }

        long lockId = computeLockId(lockName);
        Connection conn = null;

        try {
            conn = dataSource.getConnection();
            // Disable auto-commit to keep the connection open for the lock duration
            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                stmt.setLong(1, lockId);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next() && rs.getBoolean(1)) {
                        PostgresLockHandle handle =
                                new PostgresLockHandle(lockName, lockId, conn, ttl);
                        activeLocks.put(lockName, handle);
                        LOG.debug("Acquired lock: {} (id={})", lockName, lockId);
                        return Optional.of(handle);
                    }
                }
            }

            // Failed to acquire - close the connection
            try {
                conn.close();
            } catch (SQLException closeEx) {
                LOG.warn(
                        "Error closing connection after failed lock acquisition: {}",
                        closeEx.getMessage());
            }
            LOG.debug("Failed to acquire lock: {} (held by another process)", lockName);
            return Optional.empty();
        } catch (SQLException e) {
            LOG.error("Error acquiring lock {}: {}", lockName, e.getMessage());
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException closeEx) {
                    LOG.warn("Error closing connection after exception: {}", closeEx.getMessage());
                }
            }
            return Optional.empty();
        }
    }

    @Override
    public boolean isLocked(String lockName) {
        long lockId = computeLockId(lockName);

        String sql =
                """
            SELECT EXISTS (
                SELECT 1 FROM pg_locks
                WHERE locktype = 'advisory'
                AND objid = ?
            )
            """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, lockId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean(1);
                }
            }
        } catch (SQLException e) {
            LOG.warn("Error checking lock status for {}: {}", lockName, e.getMessage());
        }

        return false;
    }

    /**
     * Compute a stable 64-bit lock ID from a lock name. Uses a simple hash to avoid collisions
     * between different lock names.
     */
    private long computeLockId(String lockName) {
        // Use a well-distributed hash
        long hash = 0;
        for (char c : lockName.toCharArray()) {
            hash = 31 * hash + c;
        }
        // Ensure positive value
        return Math.abs(hash);
    }

    /** Handle for a Postgres advisory lock. */
    private class PostgresLockHandle implements LockHandle {

        private final String lockName;
        private final long lockId;
        private final Connection connection;
        private final Duration ttl;
        private volatile boolean released = false;

        PostgresLockHandle(String lockName, long lockId, Connection connection, Duration ttl) {
            this.lockName = lockName;
            this.lockId = lockId;
            this.connection = connection;
            this.ttl = ttl;
        }

        @Override
        public String lockName() {
            return lockName;
        }

        @Override
        public boolean isValid() {
            if (released) {
                return false;
            }
            try {
                return !connection.isClosed();
            } catch (SQLException e) {
                return false;
            }
        }

        @Override
        public boolean extend(Duration extension) {
            // Postgres advisory locks don't have TTL, so this is a no-op
            LOG.debug("Lock extension requested for {} (no-op for advisory locks)", lockName);
            return isValid();
        }

        @Override
        public synchronized void close() {
            if (released) {
                return;
            }
            released = true;
            activeLocks.remove(lockName);

            try {
                // Explicitly release the advisory lock
                try (PreparedStatement stmt =
                        connection.prepareStatement("SELECT pg_advisory_unlock(?)")) {
                    stmt.setLong(1, lockId);
                    stmt.execute();
                }
                connection.close();
                LOG.debug("Released lock: {} (id={})", lockName, lockId);
            } catch (SQLException e) {
                LOG.warn("Error releasing lock {}: {}", lockName, e.getMessage());
            }
        }
    }
}
