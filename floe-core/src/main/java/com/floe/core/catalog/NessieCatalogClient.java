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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.nessie.NessieCatalog;

/**
 * CatalogClient implementation for Project Nessie.
 *
 * <p>Authentication uses Bearer tokens (OIDC/JWT). The token from Floe's OIDC provider can be
 * passed directly to Nessie for end-to-end authentication.
 */
public class NessieCatalogClient extends AbstractIcebergCatalogClient {

    private final NessieCatalog catalog;
    private final String nessieUri;
    private final String warehouse;
    private final String ref;
    private final CatalogAuthConfig authConfig;

    private NessieCatalogClient(Builder builder) {
        super(builder.catalogName);
        this.nessieUri = builder.nessieUri;
        this.warehouse = builder.warehouse;
        this.ref = builder.ref;

        // Determine auth config
        if (builder.bearerToken != null) {
            this.authConfig = CatalogAuthConfig.BearerToken.of(builder.bearerToken);
        } else {
            this.authConfig = new CatalogAuthConfig.NoAuth();
        }

        this.catalog = new NessieCatalog();

        Map<String, String> properties = new HashMap<>();
        // Nessie API endpoint
        properties.put(CatalogProperties.URI, builder.nessieUri);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, builder.warehouse);

        // Nessie-specific: Git reference (branch/tag)
        properties.put("ref", builder.ref);

        // Bearer token authentication
        if (builder.bearerToken != null && !builder.bearerToken.isBlank()) {
            properties.put("authentication.type", "BEARER");
            properties.put("authentication.token", builder.bearerToken);
        }

        // Add any additional properties (e.g., S3 credentials)
        if (builder.additionalProperties != null) {
            properties.putAll(builder.additionalProperties);
        }

        catalog.initialize(builder.catalogName, properties);
        LOG.info(
                "Initialized Nessie catalog '{}' at {} on ref '{}'",
                builder.catalogName,
                builder.nessieUri,
                builder.ref);
    }

    /** Create a builder for NessieCatalogClient. */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected Catalog getCatalog() {
        return catalog;
    }

    @Override
    public void close() {
        try {
            catalog.close();
        } catch (Exception e) {
            LOG.warn("Error closing Nessie catalog '{}': {}", catalogName, e.getMessage());
        }
    }

    // Nessie-Specific Operations

    /** Get the current Git reference (branch/tag). */
    public String getRef() {
        return ref;
    }

    /** Get the Nessie URI. */
    public String getNessieUri() {
        return nessieUri;
    }

    /** Get the auth configuration. */
    public CatalogAuthConfig getAuthConfig() {
        return authConfig;
    }

    /**
     * Create a new client pointing to a different branch.
     *
     * <p>This is useful for multi-tenant scenarios where each tenant has their own branch.
     *
     * @param newRef the new branch or tag reference
     * @param bearerToken the bearer token (can be same or different)
     * @return new client instance pointing to the specified ref
     */
    public NessieCatalogClient withRef(String newRef, String bearerToken) {
        return NessieCatalogClient.builder()
                .catalogName(catalogName + "-" + newRef)
                .nessieUri(nessieUri)
                .warehouse(warehouse)
                .ref(newRef)
                .bearerToken(bearerToken)
                .build();
    }

    // Builder

    /** Builder for NessieCatalogClient. */
    public static class Builder {

        private String catalogName;
        private String nessieUri;
        private String warehouse;
        private String bearerToken;
        private String ref = "main";
        private Map<String, String> additionalProperties;

        /** Set the catalog name. */
        public Builder catalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        /** Set the Nessie server URI. */
        public Builder nessieUri(String nessieUri) {
            this.nessieUri = nessieUri;
            return this;
        }

        /** Set the warehouse location. */
        public Builder warehouse(String warehouse) {
            this.warehouse = warehouse;
            return this;
        }

        /**
         * Set the Bearer token for authentication.
         *
         * <p>This can be a JWT from Floe's OIDC provider, providing end-to-end authentication and
         * audit trail.
         */
        public Builder bearerToken(String bearerToken) {
            this.bearerToken = bearerToken;
            return this;
        }

        /** Set the Git reference (branch or tag). Defaults to "main". */
        public Builder ref(String ref) {
            this.ref = ref;
            return this;
        }

        /** Set additional catalog properties. */
        public Builder additionalProperties(Map<String, String> additionalProperties) {
            this.additionalProperties = additionalProperties;
            return this;
        }

        /** Build the NessieCatalogClient. */
        public NessieCatalogClient build() {
            if (catalogName == null || catalogName.isBlank()) {
                throw new IllegalArgumentException("catalogName is required");
            }
            if (nessieUri == null || nessieUri.isBlank()) {
                throw new IllegalArgumentException("nessieUri is required");
            }
            if (warehouse == null || warehouse.isBlank()) {
                throw new IllegalArgumentException("warehouse is required");
            }
            if (ref == null || ref.isBlank()) {
                ref = "main";
            }
            return new NessieCatalogClient(this);
        }
    }
}
