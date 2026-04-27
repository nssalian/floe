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

package com.floe.core.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floe.core.exception.FloeConfigurationException;
import com.floe.core.exception.FloeDataAccessException;
import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Postgres-backed implementation of CatalogConfigStore.
 *
 * <p>Stores non-sensitive catalog configuration for display in the UI.
 */
public class PostgresCatalogConfigStore implements CatalogConfigStore {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCatalogConfigStore.class);
    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    public PostgresCatalogConfigStore(DataSource dataSource) {
        this.dataSource = dataSource;
        this.objectMapper = new ObjectMapper();
    }

    /** Initialize the database schema for catalog configs. */
    public void initializeSchema() {
        LOG.info("Initializing Postgres catalog config store schema.");
        String createTableSql =
                """
            CREATE TABLE IF NOT EXISTS catalog_configs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(255) NOT NULL,
                type VARCHAR(50) NOT NULL,
                uri VARCHAR(1024),
                warehouse VARCHAR(1024),
                properties JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                active BOOLEAN DEFAULT false
            )
            """;
        String createIndexSql =
                """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_configs_active
            ON catalog_configs (active) WHERE active = true
            """;
        String createNameTypeIndexSql =
                """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_configs_name_type
            ON catalog_configs (name, type)
            """;
        try (Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(createTableSql);
            statement.execute(createIndexSql);
            statement.execute(createNameTypeIndexSql);
            LOG.info("Postgres catalog config store schema initialized successfully.");
        } catch (SQLException e) {
            LOG.error("Error initializing Postgre catalog config store schema.", e);
            throw new FloeDataAccessException("schema initialization", "catalog_configs", e);
        }
    }

    @Override
    public CatalogConfig upsertAndActivate(CatalogConfig config) {
        // First, deactivate all existing configs
        deactivateAll();

        // Check if config with same name and type exists
        Optional<CatalogConfig> existing = findByNameAndType(config.name(), config.type());

        if (existing.isPresent()) {
            // Update existing
            return update(existing.get().id(), config);
        }
        return insert(config);
    }

    @Override
    public Optional<CatalogConfig> findActive() {
        String sql = "SELECT * FROM catalog_configs WHERE active = true LIMIT 1";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return Optional.of(mapRow(rs));
            }
        } catch (SQLException e) {
            LOG.error("Error finding active catalog config", e);
            throw new FloeDataAccessException("query", "catalog config", e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<CatalogConfig> findByNameAndType(String name, String type) {
        String sql = "SELECT * FROM catalog_configs WHERE name = ? AND type = ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, name);
            stmt.setString(2, type);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("Error finding catalog config by name {} and type {}", name, type, e);
            throw new FloeDataAccessException("query", "catalog config", e);
        }
        return Optional.empty();
    }

    private void deactivateAll() {
        String sql = "UPDATE catalog_configs SET active = false WHERE active = true";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            int updated = stmt.executeUpdate();
            if (updated > 0) {
                LOG.debug("Deactivated {} catalog config(s)", updated);
            }
        } catch (SQLException e) {
            LOG.error("Error deactivating catalog configs", e);
            throw new FloeDataAccessException("update", "catalog config", e);
        }
    }

    private CatalogConfig insert(CatalogConfig config) {
        String sql =
                """
            INSERT INTO catalog_configs (id, name, type, uri, warehouse, properties, created_at, updated_at, active)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, true)
            RETURNING *
            """;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            UUID id = config.id() != null ? config.id() : UUID.randomUUID();
            Instant now = Instant.now();

            stmt.setObject(1, id);
            stmt.setString(2, config.name());
            stmt.setString(3, config.type());
            stmt.setString(4, config.uri());
            stmt.setString(5, config.warehouse());
            stmt.setString(6, objectMapper.writeValueAsString(config.properties()));
            stmt.setTimestamp(7, Timestamp.from(now));
            stmt.setTimestamp(8, Timestamp.from(now));

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    CatalogConfig saved = mapRow(rs);
                    LOG.info(
                            "Created catalog config: name={}, type={}, id={}",
                            saved.name(),
                            saved.type(),
                            saved.id());
                    return saved;
                }
            }
            throw new FloeDataAccessException(
                    "insert", "catalog config", "Insert did not return result", null);
        } catch (SQLException e) {
            LOG.error("Error inserting catalog config", e);
            throw new FloeDataAccessException("insert", "catalog config", e);
        } catch (JsonProcessingException e) {
            LOG.error("Error inserting catalog config", e);
            throw new FloeConfigurationException(
                    "catalog config properties", config.properties(), e);
        }
    }

    private CatalogConfig update(UUID id, CatalogConfig config) {
        String sql =
                """
            UPDATE catalog_configs
            SET uri = ?, warehouse = ?, properties = ?::jsonb, updated_at = ?, active = true
            WHERE id = ?
            RETURNING *
            """;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, config.uri());
            stmt.setString(2, config.warehouse());
            stmt.setString(3, objectMapper.writeValueAsString(config.properties()));
            stmt.setTimestamp(4, Timestamp.from(Instant.now()));
            stmt.setObject(5, id);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    CatalogConfig saved = mapRow(rs);
                    LOG.info(
                            "Updated catalog config: name={}, type={}, id={}",
                            saved.name(),
                            saved.type(),
                            saved.id());
                    return saved;
                }
            }
            throw new FloeDataAccessException(
                    "update", "catalog config", "Update did not return result", null);
        } catch (SQLException e) {
            LOG.error("Error updating catalog config", e);
            throw new FloeDataAccessException("update", "catalog config", e);
        } catch (JsonProcessingException e) {
            LOG.error("Error updating catalog config", e);
            throw new FloeConfigurationException(
                    "catalog config properties", config.properties(), e);
        }
    }

    private CatalogConfig mapRow(ResultSet rs) throws SQLException {
        Map<String, String> properties = new HashMap<>();
        String propsJson = rs.getString("properties");
        if (propsJson != null && !propsJson.isEmpty()) {
            try {
                properties =
                        objectMapper.readValue(
                                propsJson, new TypeReference<Map<String, String>>() {});
            } catch (JsonProcessingException e) {
                LOG.warn("Failed to parse properties JSON: {}", propsJson, e);
            }
        }

        return new CatalogConfig(
                rs.getObject("id", UUID.class),
                rs.getString("name"),
                rs.getString("type"),
                rs.getString("uri"),
                rs.getString("warehouse"),
                properties,
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(),
                rs.getBoolean("active"));
    }
}
