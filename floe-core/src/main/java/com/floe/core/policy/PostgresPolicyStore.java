package com.floe.core.policy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.exception.FloeConfigurationException;
import com.floe.core.exception.FloeDataAccessException;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Postgres-backed implementation of the PolicyStore. */
public class PostgresPolicyStore implements PolicyStore {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresPolicyStore.class);
    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    public PostgresPolicyStore(DataSource dataSource) {
        this.dataSource = dataSource;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new Jdk8Module());
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /** Initialize the database schema for storing policies. */
    public void initializeSchema() {
        LOG.info("Initializing Postgres policy store schema.");
        String createTableSql =
                """
            CREATE TABLE IF NOT EXISTS maintenance_policies (
                id VARCHAR(64) PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                description TEXT,
                enabled BOOLEAN NOT NULL DEFAULT true,
                table_pattern VARCHAR(512) NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,

                rewrite_data_files_config JSONB,
                rewrite_data_files_schedule JSONB,
                expire_snapshots_config JSONB,
                expire_snapshots_schedule JSONB,
                orphan_cleanup_config JSONB,
                orphan_cleanup_schedule JSONB,
                rewrite_manifests_config JSONB,
                rewrite_manifests_schedule JSONB,

                tags JSONB,

                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )
            """;
        String createIndexesSql =
                """
            CREATE INDEX IF NOT EXISTS idx_policies_enabled ON maintenance_policies(enabled);
            CREATE INDEX IF NOT EXISTS idx_policies_name ON maintenance_policies(name);
            CREATE INDEX IF NOT EXISTS idx_policies_pattern ON maintenance_policies(table_pattern);
            """;
        try (Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(createTableSql);
            statement.execute(createIndexesSql);
            LOG.info("Postgres policy store schema initialized successfully.");
        } catch (Exception e) {
            LOG.error("Error initializing Postgres policy store schema.", e);
            throw new FloeDataAccessException("initialize", "schema", e);
        }
    }

    @Override
    public Optional<MaintenancePolicy> getById(String id) {
        String sql = "SELECT * FROM maintenance_policies WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRowToPolicy(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("Error retrieving policy by ID: {}", id, e);
            throw new FloeDataAccessException("query", "policy", e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<MaintenancePolicy> getByName(String name) {
        String sql = "SELECT * FROM maintenance_policies WHERE name = ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, name);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRowToPolicy(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("Error retrieving policy by name: {}", name, e);
            throw new FloeDataAccessException("query", "policy", e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<MaintenancePolicy> findByName(String name) {
        return getByName(name);
    }

    @Override
    public List<MaintenancePolicy> listAll() {
        String sql = "SELECT * FROM maintenance_policies ORDER BY priority DESC, created_at DESC";
        List<MaintenancePolicy> policies = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql);
                ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                policies.add(mapRowToPolicy(rs));
            }
        } catch (SQLException e) {
            LOG.error("Error listing all policies", e);
            throw new FloeDataAccessException("list", "policies", e);
        }
        return policies;
    }

    @Override
    public List<MaintenancePolicy> listAll(int limit, int offset) {
        String sql =
                """
            SELECT * FROM maintenance_policies
            ORDER BY priority DESC, created_at DESC
            LIMIT ? OFFSET ?
            """;
        List<MaintenancePolicy> policies = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, limit);
            statement.setInt(2, offset);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    policies.add(mapRowToPolicy(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("Error listing policies with pagination", e);
            throw new FloeDataAccessException("list", "policies", e);
        }
        return policies;
    }

    @Override
    public List<MaintenancePolicy> listEnabled() {
        String sql =
                "SELECT * FROM maintenance_policies WHERE enabled = TRUE ORDER BY priority DESC, created_at DESC";
        List<MaintenancePolicy> policies = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql);
                ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                policies.add(mapRowToPolicy(rs));
            }
        } catch (SQLException e) {
            LOG.error("Error listing enabled policies", e);
            throw new FloeDataAccessException("list", "enabled policies", e);
        }
        return policies;
    }

    @Override
    public List<MaintenancePolicy> listEnabled(int limit, int offset) {
        String sql =
                """
            SELECT * FROM maintenance_policies
            WHERE enabled = TRUE
            ORDER BY priority DESC, created_at DESC
            LIMIT ? OFFSET ?
            """;
        List<MaintenancePolicy> policies = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, limit);
            statement.setInt(2, offset);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    policies.add(mapRowToPolicy(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("Error listing enabled policies with pagination", e);
            throw new FloeDataAccessException("list", "enabled policies", e);
        }
        return policies;
    }

    @Override
    public int countEnabled() {
        String sql = "SELECT COUNT(*) FROM maintenance_policies WHERE enabled = TRUE";
        try (Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            LOG.error("Error counting enabled policies", e);
            throw new FloeDataAccessException("count", "enabled policies", e);
        }
        return 0;
    }

    @Override
    public List<MaintenancePolicy> findMatchingPolicies(String catalog, TableIdentifier tableId) {
        return listEnabled().stream()
                .filter(policy -> policy.tablePattern().matches(catalog, tableId))
                .sorted((p1, p2) -> Integer.compare(p2.effectivePriority(), p1.effectivePriority()))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<MaintenancePolicy> findEffectivePolicy(
            String catalog, TableIdentifier tableId) {
        return listEnabled().stream()
                .filter(policy -> policy.tablePattern().matches(catalog, tableId))
                .max(Comparator.comparingInt(p -> p.tablePattern().specificity()));
    }

    @Override
    public List<MaintenancePolicy> findByPattern(TablePattern pattern) {
        return listAll().stream()
                .filter(policy -> patternsOverlap(pattern, policy.tablePattern()))
                .collect(Collectors.toList());
    }

    private boolean patternsOverlap(TablePattern query, TablePattern policy) {
        return (componentOverlaps(query.catalogPattern(), policy.catalogPattern())
                && componentOverlaps(query.namespacePattern(), policy.namespacePattern())
                && componentOverlaps(query.tablePattern(), policy.tablePattern()));
    }

    private boolean componentOverlaps(String query, String policy) {
        if (query == null || policy == null) return true;
        if (query.contains("*") || policy.contains("*")) return true;
        return query.equals(policy);
    }

    @Override
    public void save(MaintenancePolicy policy) {
        Objects.requireNonNull(policy, "Policy cannot be null");
        Objects.requireNonNull(policy.id(), "Policy ID cannot be null");
        Objects.requireNonNull(policy.name(), "Policy name cannot be null");
        String upsertSql =
                """
            INSERT INTO maintenance_policies (
                id, name, description, enabled, table_pattern, priority,
                rewrite_data_files_config, rewrite_data_files_schedule,
                expire_snapshots_config, expire_snapshots_schedule,
                orphan_cleanup_config, orphan_cleanup_schedule,
                rewrite_manifests_config, rewrite_manifests_schedule,
                tags, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                enabled = EXCLUDED.enabled,
                table_pattern = EXCLUDED.table_pattern,
                priority = EXCLUDED.priority,
                rewrite_data_files_config = EXCLUDED.rewrite_data_files_config,
                rewrite_data_files_schedule = EXCLUDED.rewrite_data_files_schedule,
                expire_snapshots_config = EXCLUDED.expire_snapshots_config,
                expire_snapshots_schedule = EXCLUDED.expire_snapshots_schedule,
                orphan_cleanup_config = EXCLUDED.orphan_cleanup_config,
                orphan_cleanup_schedule = EXCLUDED.orphan_cleanup_schedule,
                rewrite_manifests_config = EXCLUDED.rewrite_manifests_config,
                rewrite_manifests_schedule = EXCLUDED.rewrite_manifests_schedule,
                tags = EXCLUDED.tags,
                updated_at = EXCLUDED.updated_at
            """;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(upsertSql)) {
            Instant now = Instant.now();
            statement.setString(1, policy.id());
            statement.setString(2, policy.name());
            statement.setString(3, policy.description());
            statement.setBoolean(4, policy.enabled());
            statement.setString(5, policy.tablePattern().toString());
            statement.setInt(6, policy.priority());
            statement.setString(7, toJson(policy.rewriteDataFiles()));
            statement.setString(8, toJson(policy.rewriteDataFilesSchedule()));
            statement.setString(9, toJson(policy.expireSnapshots()));
            statement.setString(10, toJson(policy.expireSnapshotsSchedule()));
            statement.setString(11, toJson(policy.orphanCleanup()));
            statement.setString(12, toJson(policy.orphanCleanupSchedule()));
            statement.setString(13, toJson(policy.rewriteManifests()));
            statement.setString(14, toJson(policy.rewriteManifestsSchedule()));
            statement.setString(15, toJson(policy.tags()));
            statement.setTimestamp(
                    16, Timestamp.from(policy.createdAt() != null ? policy.createdAt() : now));
            statement.setTimestamp(17, Timestamp.from(now));

            statement.executeUpdate();
            LOG.debug("Saved policy: {} ({})", policy.name(), policy.id());
        } catch (SQLException e) {
            if (e.getMessage() != null && e.getMessage().contains("unique constraint")) {
                throw new IllegalArgumentException(
                        "A policy with name '" + policy.name() + "' already exists");
            }
            LOG.error("Failed to save policy: {}", policy.name(), e);
            throw new FloeDataAccessException("save", "policy '" + policy.name() + "'", e);
        }
    }

    @Override
    public void saveAll(List<MaintenancePolicy> policies) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                for (MaintenancePolicy policy : policies) {
                    save(policy);
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            LOG.error("Failed to save policies batch", e);
            throw new FloeDataAccessException("save", "policies batch", e);
        }
    }

    @Override
    public boolean deleteById(String id) {
        String sql = "DELETE FROM maintenance_policies WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, id);
            int rowsAffected = statement.executeUpdate();
            if (rowsAffected > 0) {
                LOG.debug("Deleted policy with ID: {}", id);
                return true;
            }
        } catch (SQLException e) {
            LOG.error("Error deleting policy by ID: {}", id, e);
            throw new FloeDataAccessException("delete", "policy id=" + id, e);
        }
        return false;
    }

    @Override
    public boolean deleteByPattern(TablePattern pattern) {
        List<MaintenancePolicy> toDelete = findByPattern(pattern);
        boolean deleted = false;
        for (MaintenancePolicy policy : toDelete) {
            if (deleteById(policy.id())) {
                deleted = true;
            }
        }
        return deleted;
    }

    @Override
    public boolean existsById(String id) {
        String sql = "SELECT 1 FROM maintenance_policies WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            LOG.error("Error checking existence of policy by ID: {}", id, e);
            throw new FloeDataAccessException("check existence", "policy", e);
        }
    }

    @Override
    public boolean existsByName(String name) {
        String sql = "SELECT 1 FROM maintenance_policies WHERE name = ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, name);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            LOG.error("Error checking existence of policy by name: {}", name, e);
            throw new FloeDataAccessException("check existence", "policy", e);
        }
    }

    @Override
    public int count() {
        String sql = "SELECT COUNT(*) FROM maintenance_policies";
        try (Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            LOG.error("Error counting policies", e);
            throw new FloeDataAccessException("count", "policies", e);
        }
        return 0;
    }

    @Override
    public void clear() {
        String sql = "DELETE FROM maintenance_policies";
        try (Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement()) {
            int rowsDeleted = statement.executeUpdate(sql);
            LOG.debug("Cleared all policies, deleted {} rows", rowsDeleted);
        } catch (SQLException e) {
            LOG.error("Error clearing all policies", e);
            throw new FloeDataAccessException("clear", "policies", e);
        }
    }

    // Helper Methods

    private MaintenancePolicy mapRowToPolicy(ResultSet rs) throws SQLException {
        try {
            return new MaintenancePolicy(
                    rs.getString("id"),
                    rs.getString("name"),
                    rs.getString("description"),
                    TablePattern.parse(rs.getString("table_pattern")),
                    rs.getBoolean("enabled"),
                    fromJson(
                            rs.getString("rewrite_data_files_config"),
                            RewriteDataFilesConfig.class),
                    fromJson(rs.getString("rewrite_data_files_schedule"), ScheduleConfig.class),
                    fromJson(rs.getString("expire_snapshots_config"), ExpireSnapshotsConfig.class),
                    fromJson(rs.getString("expire_snapshots_schedule"), ScheduleConfig.class),
                    fromJson(rs.getString("orphan_cleanup_config"), OrphanCleanupConfig.class),
                    fromJson(rs.getString("orphan_cleanup_schedule"), ScheduleConfig.class),
                    fromJson(
                            rs.getString("rewrite_manifests_config"), RewriteManifestsConfig.class),
                    fromJson(rs.getString("rewrite_manifests_schedule"), ScheduleConfig.class),
                    rs.getInt("priority"),
                    fromJsonMap(rs.getString("tags")),
                    rs.getTimestamp("created_at").toInstant(),
                    rs.getTimestamp("updated_at").toInstant());
        } catch (JsonProcessingException e) {
            throw new SQLException("Failed to parse JSON config", e);
        }
    }

    private <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException {
        if (json == null || json.isBlank()) {
            return null;
        }
        return objectMapper.readValue(json, clazz);
    }

    private String toJson(Object obj) {
        if (obj == null) return null;
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new FloeConfigurationException(
                    "JSON serialization", obj.getClass().getSimpleName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> fromJsonMap(String json) throws JsonProcessingException {
        if (json == null || json.isBlank()) {
            return Collections.emptyMap();
        }
        return objectMapper.readValue(json, Map.class);
    }

    @Override
    public String toString() {
        return String.format("PostgresPolicyStore[count=%d]", count());
    }
}
