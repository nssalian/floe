package com.floe.core.catalog;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

/**
 * CatalogClient implementation for DataHub Iceberg Catalog.
 *
 * <p>Authentication uses DataHub Personal Access Tokens (PAT) via bearer token.
 */
public class DataHubCatalogClient extends AbstractIcebergCatalogClient {

    private final RESTCatalog catalog;
    private final String datahubUri;
    private final CatalogAuthConfig authConfig;

    private DataHubCatalogClient(Builder builder) {
        super(builder.catalogName);
        this.datahubUri = builder.datahubUri;

        this.catalog = new RESTCatalog();

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, RESTCatalog.class.getName());

        // DataHub Iceberg REST endpoint
        // Ensure URI ends with /iceberg/ for the catalog endpoint
        String uri = normalizeUri(builder.datahubUri);
        properties.put(CatalogProperties.URI, uri);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, builder.warehouse);

        // Bearer token authentication via DataHub PAT
        if (builder.token != null && !builder.token.isBlank()) {
            properties.put("header.Authorization", "Bearer " + builder.token);
            this.authConfig = CatalogAuthConfig.BearerToken.of(builder.token);
        } else {
            throw new IllegalStateException("DataHub requires a Personal Access Token");
        }

        // Add any additional properties (S3 config, etc.)
        if (builder.additionalProperties != null) {
            properties.putAll(builder.additionalProperties);
        }

        catalog.initialize(builder.catalogName, properties);
        LOG.info(
                "Initialized DataHub catalog '{}' at {} with bearer token authentication",
                builder.catalogName,
                uri);
    }

    /**
     * Normalize the DataHub URI to ensure it points to the Iceberg catalog endpoint.
     *
     * @param uri the base DataHub GMS URI
     * @return normalized URI ending with /iceberg/
     */
    private static String normalizeUri(String uri) {
        String normalized = uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
        if (!normalized.endsWith("/iceberg")) {
            normalized = normalized + "/iceberg";
        }
        return normalized;
    }

    /** Create a builder for DataHubCatalogClient. */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected Catalog getCatalog() {
        return catalog;
    }

    /** Get the DataHub GMS URI. */
    public String getDatahubUri() {
        return datahubUri;
    }

    /** Get the auth configuration. */
    public CatalogAuthConfig getAuthConfig() {
        return authConfig;
    }

    @Override
    public void close() {
        try {
            catalog.close();
        } catch (Exception e) {
            LOG.warn("Error closing DataHub catalog '{}': {}", catalogName, e.getMessage());
        }
    }

    /** Builder for DataHubCatalogClient. */
    public static class Builder {

        private String catalogName;
        private String datahubUri;
        private String warehouse;
        private String token;
        private Map<String, String> additionalProperties;

        /** Set the catalog name. */
        public Builder catalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        /**
         * Set the DataHub GMS URI.
         *
         * <p>This should be the base GMS URL (e.g., http://datahub-gms:8080). The /iceberg/
         * endpoint will be appended automatically if not present.
         */
        public Builder datahubUri(String datahubUri) {
            this.datahubUri = datahubUri;
            return this;
        }

        /** Set the warehouse location (e.g., s3://bucket/warehouse). */
        public Builder warehouse(String warehouse) {
            this.warehouse = warehouse;
            return this;
        }

        /**
         * Set the DataHub Personal Access Token for authentication.
         *
         * <p>PATs can be generated from the DataHub UI under Settings > Access Tokens.
         */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /** Set additional catalog properties (e.g., S3 configuration). */
        public Builder additionalProperties(Map<String, String> additionalProperties) {
            this.additionalProperties = additionalProperties;
            return this;
        }

        /** Build the DataHubCatalogClient. */
        public DataHubCatalogClient build() {
            if (catalogName == null || catalogName.isBlank()) {
                throw new IllegalArgumentException("catalogName is required");
            }
            if (datahubUri == null || datahubUri.isBlank()) {
                throw new IllegalArgumentException("datahubUri is required");
            }
            if (warehouse == null || warehouse.isBlank()) {
                throw new IllegalArgumentException("warehouse is required");
            }
            if (token == null || token.isBlank()) {
                throw new IllegalArgumentException(
                        "token is required - DataHub requires a Personal Access Token for"
                                + " authentication");
            }
            return new DataHubCatalogClient(this);
        }
    }
}
