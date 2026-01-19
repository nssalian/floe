package com.floe.core.catalog;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

/**
 * CatalogClient implementation for Iceberg REST Catalog.
 *
 * <p>Connects to an Iceberg REST catalog server. Supports multiple authentication methods:
 *
 * <ul>
 *   <li>No authentication (for development/testing)
 *   <li>Bearer token authentication
 *   <li>OAuth2 client credentials flow
 * </ul>
 */
public class IcebergRestCatalogClient extends AbstractIcebergCatalogClient {

    private final RESTCatalog catalog;
    private final String uri;
    private final CatalogAuthConfig authConfig;

    private IcebergRestCatalogClient(Builder builder) {
        super(builder.catalogName);
        this.uri = builder.uri;
        this.catalog = new RESTCatalog();

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, RESTCatalog.class.getName());
        properties.put(CatalogProperties.URI, builder.uri);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, builder.warehouse);

        // Configure authentication
        if (builder.token != null && !builder.token.isBlank()) {
            // Bearer token authentication
            properties.put("header.Authorization", "Bearer " + builder.token);
            this.authConfig = CatalogAuthConfig.BearerToken.of(builder.token);
        } else if (builder.clientId != null && builder.clientSecret != null) {
            // OAuth2 client credentials
            properties.put("rest.auth.type", "oauth2");
            properties.put(
                    "rest.auth.oauth2.credential", builder.clientId + ":" + builder.clientSecret);
            if (builder.tokenEndpoint != null) {
                properties.put("rest.auth.oauth2.server-uri", builder.tokenEndpoint);
            }
            if (builder.scope != null) {
                properties.put("rest.auth.oauth2.scope", builder.scope);
            }
            this.authConfig =
                    new CatalogAuthConfig.OAuth2ClientCredentials(
                            builder.clientId,
                            builder.clientSecret,
                            builder.tokenEndpoint != null
                                    ? java.util.Optional.of(builder.tokenEndpoint)
                                    : java.util.Optional.empty(),
                            builder.scope != null
                                    ? java.util.Optional.of(builder.scope)
                                    : java.util.Optional.empty());
        } else {
            // No authentication
            this.authConfig = new CatalogAuthConfig.NoAuth();
        }

        // Add any additional properties
        if (builder.additionalProperties != null) {
            properties.putAll(builder.additionalProperties);
        }

        catalog.initialize(builder.catalogName, properties);
        LOG.info(
                "Initialized REST catalog '{}' at {} with {} authentication",
                builder.catalogName,
                builder.uri,
                authConfig.authType());
    }

    /** Create a builder for IcebergRestCatalogClient. */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected Catalog getCatalog() {
        return catalog;
    }

    /** Get the catalog URI. */
    public String getUri() {
        return uri;
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
            LOG.warn("Error closing REST catalog '{}': {}", catalogName, e.getMessage());
        }
    }

    /** Builder for IcebergRestCatalogClient. */
    public static class Builder {

        private String catalogName;
        private String uri;
        private String warehouse;
        private String token;
        private String clientId;
        private String clientSecret;
        private String tokenEndpoint;
        private String scope;
        private Map<String, String> additionalProperties;

        /** Set the catalog name. */
        public Builder catalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        /** Set the REST catalog URI. */
        public Builder uri(String uri) {
            this.uri = uri;
            return this;
        }

        /** Set the warehouse location. */
        public Builder warehouse(String warehouse) {
            this.warehouse = warehouse;
            return this;
        }

        /** Set a bearer token for authentication. */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /**
         * Configure OAuth2 client credentials with custom token endpoint.
         *
         * @param clientId the OAuth2 client ID
         * @param clientSecret the OAuth2 client secret
         * @param tokenEndpoint the OAuth2 token endpoint URL
         * @return this builder
         */
        public Builder oauth2(String clientId, String clientSecret, String tokenEndpoint) {
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.tokenEndpoint = tokenEndpoint;
            return this;
        }

        /** Set the OAuth2 scope. */
        public Builder scope(String scope) {
            this.scope = scope;
            return this;
        }

        /** Set additional catalog properties. */
        public Builder additionalProperties(Map<String, String> additionalProperties) {
            this.additionalProperties = additionalProperties;
            return this;
        }

        /** Build the IcebergRestCatalogClient. */
        public IcebergRestCatalogClient build() {
            if (catalogName == null || catalogName.isBlank()) {
                throw new IllegalArgumentException("catalogName is required");
            }
            if (uri == null || uri.isBlank()) {
                throw new IllegalArgumentException("uri is required");
            }
            if (warehouse == null || warehouse.isBlank()) {
                throw new IllegalArgumentException("warehouse is required");
            }
            return new IcebergRestCatalogClient(this);
        }
    }
}
