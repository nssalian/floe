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

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Persisted catalog configuration.
 *
 * <p>Stores non-sensitive catalog properties for display in the UI and audit. Credentials (access
 * keys, tokens, secrets) are not persisted but only in environment variables.
 *
 * <p>No multi-catalog support yet; only one active catalog at a time.
 *
 * @param id Unique identifier
 * @param name Catalog name (e.g., "demo")
 * @param type Catalog type (REST, HIVE, NESSIE, POLARIS)
 * @param uri Catalog URI
 * @param warehouse Warehouse location (s3://bucket/path)
 * @param properties Non-sensitive properties (region, ref, path-style, etc.)
 * @param createdAt When this config was first persisted
 * @param updatedAt When this config was last updated
 * @param active Whether this is the currently active catalog
 */
public record CatalogConfig(
        UUID id,
        String name,
        String type,
        String uri,
        String warehouse,
        Map<String, String> properties,
        Instant createdAt,
        Instant updatedAt,
        boolean active) {

    /**
     * Creates a new CatalogConfig with generated ID and timestamps.
     *
     * @param name Catalog name
     * @param type Catalog type
     * @param uri Catalog URI
     * @param warehouse Warehouse location
     * @param properties Non-sensitive properties
     * @return New CatalogConfig instance
     */
    public static CatalogConfig create(
            String name,
            String type,
            String uri,
            String warehouse,
            Map<String, String> properties) {
        Instant now = Instant.now();
        return new CatalogConfig(
                UUID.randomUUID(), name, type, uri, warehouse, properties, now, now, true);
    }

    /**
     * Creates an updated copy with new timestamp.
     *
     * @param uri New URI
     * @param warehouse New warehouse
     * @param properties New properties
     * @return Updated CatalogConfig
     */
    public CatalogConfig withUpdates(String uri, String warehouse, Map<String, String> properties) {
        return new CatalogConfig(
                this.id,
                this.name,
                this.type,
                uri,
                warehouse,
                properties,
                this.createdAt,
                Instant.now(),
                this.active);
    }

    /** Creates a copy with active flag set. */
    public CatalogConfig withActive(boolean active) {
        return new CatalogConfig(
                this.id,
                this.name,
                this.type,
                this.uri,
                this.warehouse,
                this.properties,
                this.createdAt,
                this.updatedAt,
                active);
    }
}
