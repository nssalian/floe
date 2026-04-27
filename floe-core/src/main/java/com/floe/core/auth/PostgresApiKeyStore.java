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
 * Postgres-backed ApiKeyStore implementation.
 *
 * <p>Stores API keys in a relational schema with proper indexing for fast lookups by hash.
 */
public record PostgresApiKeyStore(DataSource dataSource) implements ApiKeyStore {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresApiKeyStore.class);

    /** Initialize the database schema if it doesn't exist. */
    public void initializeSchema() {
        String createTable =
                """
                        CREATE TABLE IF NOT EXISTS api_keys (
                            id VARCHAR(36) PRIMARY KEY,
                            key_hash VARCHAR(64) NOT NULL UNIQUE,
                            name VARCHAR(255) NOT NULL UNIQUE,
                            role VARCHAR(50) NOT NULL,
                            enabled BOOLEAN NOT NULL DEFAULT true,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                            expires_at TIMESTAMP WITH TIME ZONE,
                            last_used_at TIMESTAMP WITH TIME ZONE,
                            created_by VARCHAR(36),
                            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                        )
                        """;

        String createHashIndex =
                """
                        CREATE INDEX IF NOT EXISTS idx_api_keys_hash
                        ON api_keys(key_hash)
                        """;

        String createNameIndex =
                """
                        CREATE INDEX IF NOT EXISTS idx_api_keys_name
                        ON api_keys(name)
                        """;

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(createTable);
            stmt.execute(createHashIndex);
            stmt.execute(createNameIndex);

            LOG.info("PostgresApiKeyStore schema initialized");
        } catch (SQLException e) {
            throw new FloeDataAccessException("schema initialization", "api_keys", e);
        }
    }

    @Override
    public Optional<ApiKey> findByKeyHash(String keyHash) {
        String sql = "SELECT * FROM api_keys WHERE key_hash = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, keyHash);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRowToApiKey(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "api key", e);
        }
    }

    @Override
    public Optional<ApiKey> findById(String id) {
        String sql = "SELECT * FROM api_keys WHERE id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRowToApiKey(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "api key", e);
        }
    }

    @Override
    public Optional<ApiKey> findByName(String name) {
        String sql = "SELECT * FROM api_keys WHERE name = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, name);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRowToApiKey(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "api key", e);
        }
    }

    @Override
    public List<ApiKey> listAll() {
        String sql = "SELECT * FROM api_keys ORDER BY created_at DESC";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "api key", e);
        }
    }

    @Override
    public List<ApiKey> listEnabled() {
        String sql = "SELECT * FROM api_keys WHERE enabled = true ORDER BY created_at DESC";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            return executeQuery(stmt);
        } catch (SQLException e) {
            throw new FloeDataAccessException("query", "api key", e);
        }
    }

    @Override
    public void save(ApiKey apiKey) {
        String sql =
                """
                        INSERT INTO api_keys
                            (id, key_hash, name, role, enabled, created_at, expires_at, last_used_at, created_by, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            role = EXCLUDED.role,
                            enabled = EXCLUDED.enabled,
                            expires_at = EXCLUDED.expires_at,
                            last_used_at = EXCLUDED.last_used_at,
                            updated_at = NOW()
                        """;

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, apiKey.id());
            stmt.setString(2, apiKey.keyHash());
            stmt.setString(3, apiKey.name());
            stmt.setString(4, apiKey.role().name());
            stmt.setBoolean(5, apiKey.enabled());
            stmt.setTimestamp(6, Timestamp.from(apiKey.createdAt()));
            stmt.setTimestamp(
                    7, apiKey.expiresAt() != null ? Timestamp.from(apiKey.expiresAt()) : null);
            stmt.setTimestamp(
                    8, apiKey.lastUsedAt() != null ? Timestamp.from(apiKey.lastUsedAt()) : null);
            stmt.setString(9, apiKey.createdBy());

            stmt.executeUpdate();
            LOG.debug("Saved API key: {} ({})", apiKey.name(), apiKey.id());
        } catch (SQLException e) {
            if (e.getMessage() != null && e.getMessage().contains("api_keys_name_key")) {
                throw new IllegalArgumentException(
                        "API key with name '" + apiKey.name() + "' already exists");
            }
            throw new FloeDataAccessException("save", "api key", e);
        }
    }

    @Override
    public void updateLastUsed(String id) {
        String sql = "UPDATE api_keys SET last_used_at = NOW() WHERE id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.warn("Failed to update last_used_at for key {}: {}", id, e.getMessage());
        }
    }

    @Override
    public boolean deleteById(String id) {
        String sql = "DELETE FROM api_keys WHERE id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id);
            int deleted = stmt.executeUpdate();

            if (deleted > 0) {
                LOG.info("Deleted API key: {}", id);
                return true;
            }
            return false;
        } catch (SQLException e) {
            throw new FloeDataAccessException("delete", "api key", e);
        }
    }

    @Override
    public boolean existsByName(String name) {
        String sql = "SELECT 1 FROM api_keys WHERE name = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, name);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new FloeDataAccessException("existence check", "api key", e);
        }
    }

    @Override
    public boolean existsById(String id) {
        String sql = "SELECT 1 FROM api_keys WHERE id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new FloeDataAccessException("existence check", "api key", e);
        }
    }

    @Override
    public long count() {
        String sql = "SELECT COUNT(*) FROM api_keys";

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new FloeDataAccessException("count", "api key", e);
        }
    }

    @Override
    public void clear() {
        String sql = "DELETE FROM api_keys";

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(sql);
            LOG.info("Cleared {} API keys", deleted);
        } catch (SQLException e) {
            throw new FloeDataAccessException("clear", "api key", e);
        }
    }

    private List<ApiKey> executeQuery(PreparedStatement stmt) throws SQLException {
        List<ApiKey> results = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                results.add(mapRowToApiKey(rs));
            }
        }
        return results;
    }

    private ApiKey mapRowToApiKey(ResultSet rs) throws SQLException {
        String id = rs.getString("id");
        String keyHash = rs.getString("key_hash");
        String name = rs.getString("name");
        Role role = Role.valueOf(rs.getString("role"));
        boolean enabled = rs.getBoolean("enabled");

        Timestamp createdAtTs = rs.getTimestamp("created_at");
        Instant createdAt = createdAtTs != null ? createdAtTs.toInstant() : null;

        Timestamp expiresAtTs = rs.getTimestamp("expires_at");
        Instant expiresAt = expiresAtTs != null ? expiresAtTs.toInstant() : null;

        Timestamp lastUsedAtTs = rs.getTimestamp("last_used_at");
        Instant lastUsedAt = lastUsedAtTs != null ? lastUsedAtTs.toInstant() : null;

        String createdBy = rs.getString("created_by");

        return new ApiKey(
                id, keyHash, name, role, enabled, createdAt, expiresAt, lastUsedAt, createdBy);
    }
}
