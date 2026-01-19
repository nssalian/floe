package com.floe.core.catalog;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

/**
 * CatalogClient implementation for Apache Polaris (Incubating).
 *
 * <p>Authentication supports two modes:
 *
 * <ul>
 *   <li>OAuth2 client credentials flow (clientId + clientSecret)
 *   <li>Bearer token (pre-obtained access token)
 * </ul>
 */
public class PolarisCatalogClient extends AbstractIcebergCatalogClient {

    private final RESTCatalog catalog;
    private final String polarisUri;
    private final CatalogAuthConfig authConfig;

    private PolarisCatalogClient(Builder builder) {
        super(builder.catalogName);
        this.polarisUri = builder.polarisUri;

        this.catalog = new RESTCatalog();

        Map<String, String> properties = new HashMap<>();
        // polarisUri should be the full REST endpoint (e.g., http://polaris:8181/api/catalog)
        String uri =
                builder.polarisUri.endsWith("/api/catalog")
                        ? builder.polarisUri
                        : builder.polarisUri + "/api/catalog";
        properties.put(CatalogProperties.URI, uri);
        properties.put("warehouse", builder.catalogName);

        // Configure authentication based on what's provided
        if (builder.token != null && !builder.token.isBlank()) {
            // Bearer token authentication
            properties.put("header.Authorization", "Bearer " + builder.token);
            this.authConfig = CatalogAuthConfig.BearerToken.of(builder.token);
            LOG.info(
                    "Initialized Polaris catalog '{}' at {} with bearer token authentication",
                    builder.catalogName,
                    builder.polarisUri);
        } else {
            // OAuth2 client credentials flow
            this.authConfig =
                    CatalogAuthConfig.OAuth2ClientCredentials.forPolaris(
                            builder.clientId,
                            builder.clientSecret,
                            builder.polarisUri,
                            builder.principalRole);
            // Iceberg RESTCatalog OAuth2 properties
            properties.put("credential", builder.clientId + ":" + builder.clientSecret);
            // OAuth endpoint is at /v1/oauth/tokens relative to the catalog URI
            String oauthUri = uri + "/v1/oauth/tokens";
            properties.put("oauth2-server-uri", oauthUri);
            // Set scope - use specified role or default to ALL
            String scope =
                    (builder.principalRole != null && !builder.principalRole.isEmpty())
                            ? "PRINCIPAL_ROLE:" + builder.principalRole
                            : "PRINCIPAL_ROLE:ALL";
            properties.put("scope", scope);
            LOG.info(
                    "Initialized Polaris catalog '{}' at {} with OAuth2 authentication",
                    builder.catalogName,
                    builder.polarisUri);
        }

        // Add any additional properties
        if (builder.additionalProperties != null) {
            properties.putAll(builder.additionalProperties);
        }

        catalog.initialize(builder.catalogName, properties);
    }

    /** Create a builder for PolarisCatalogClient. */
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
            LOG.warn("Error closing Polaris catalog '{}': {}", catalogName, e.getMessage());
        }
    }

    /** Get the Polaris URI. */
    public String getPolarisUri() {
        return polarisUri;
    }

    /** Get the auth configuration. */
    public CatalogAuthConfig getAuthConfig() {
        return authConfig;
    }

    // Builder

    /** Builder for PolarisCatalogClient. */
    public static class Builder {

        private String catalogName;
        private String polarisUri;
        private String warehouse;
        private String clientId;
        private String clientSecret;
        private String principalRole;
        private String token;
        private Map<String, String> additionalProperties;

        /** Set the catalog name. */
        public Builder catalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        /** Set the Polaris server URI. */
        public Builder polarisUri(String polarisUri) {
            this.polarisUri = polarisUri;
            return this;
        }

        /** Set the warehouse location. */
        public Builder warehouse(String warehouse) {
            this.warehouse = warehouse;
            return this;
        }

        /** Set the OAuth2 client ID. */
        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        /** Set the OAuth2 client secret. */
        public Builder clientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
            return this;
        }

        /** Set the Polaris principal role. */
        public Builder principalRole(String principalRole) {
            this.principalRole = principalRole;
            return this;
        }

        /** Set a pre-obtained bearer token (alternative to clientId/clientSecret). */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /** Set additional catalog properties. */
        public Builder additionalProperties(Map<String, String> additionalProperties) {
            this.additionalProperties = additionalProperties;
            return this;
        }

        /** Build the PolarisCatalogClient. */
        public PolarisCatalogClient build() {
            if (catalogName == null || catalogName.isBlank()) {
                throw new IllegalArgumentException("catalogName is required");
            }
            if (polarisUri == null || polarisUri.isBlank()) {
                throw new IllegalArgumentException("polarisUri is required");
            }
            if (warehouse == null || warehouse.isBlank()) {
                throw new IllegalArgumentException("warehouse is required");
            }
            // Either token OR (clientId + clientSecret) must be provided
            boolean hasToken = token != null && !token.isBlank();
            boolean hasClientCredentials =
                    (clientId != null && !clientId.isBlank())
                            && (clientSecret != null && !clientSecret.isBlank());

            if (!hasToken && !hasClientCredentials) {
                throw new IllegalArgumentException(
                        "Either token or (clientId + clientSecret) is required for authentication");
            }
            return new PolarisCatalogClient(this);
        }
    }
}
