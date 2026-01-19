package com.floe.core.catalog;

import java.util.Map;
import java.util.Optional;

/**
 * Configuration for catalog authentication.
 *
 * <p>Supports multiple authentication methods for different catalog types:
 *
 * <ul>
 *   <li>OAuth2 Client Credentials - Polaris
 *   <li>Bearer Token - Nessie, generic REST
 *   <li>AWS IAM - S3
 *   <li>Kerberos - Hive Metastore
 * </ul>
 */
public sealed interface CatalogAuthConfig {
    /** Get the authentication type. */
    AuthType authType();

    /** Convert to Iceberg catalog properties. */
    Map<String, String> toIcebergProperties();

    /** Supported authentication types. */
    enum AuthType {
        NONE,
        OAUTH2,
        BEARER
    }

    /** No authentication configuration. Used for development and testing. */
    record NoAuth() implements CatalogAuthConfig {
        @Override
        public AuthType authType() {
            return AuthType.NONE;
        }

        @Override
        public Map<String, String> toIcebergProperties() {
            return Map.of();
        }
    }

    // OAuth2 Client Credentials

    /**
     * OAuth2 client credentials flow configuration.
     *
     * <p>Used by:
     *
     * <ul>
     *   <li>Apache Polaris
     *   <li>Custom REST catalogs with OAuth2
     * </ul>
     *
     * @param clientId OAuth2 client ID
     * @param clientSecret OAuth2 client secret
     * @param tokenEndpoint OAuth2 token endpoint URL (if different from catalog)
     * @param scope OAuth2 scope (optional, e.g., "PRINCIPAL_ROLE:admin")
     */
    record OAuth2ClientCredentials(
            String clientId,
            String clientSecret,
            Optional<String> tokenEndpoint,
            Optional<String> scope)
            implements CatalogAuthConfig {
        @Override
        public AuthType authType() {
            return AuthType.OAUTH2;
        }

        @Override
        public Map<String, String> toIcebergProperties() {
            var props = new java.util.HashMap<String, String>();
            props.put("rest.auth.type", "oauth2");
            props.put("rest.auth.oauth2.credential", clientId + ":" + clientSecret);
            tokenEndpoint.ifPresent(ep -> props.put("rest.auth.oauth2.server-uri", ep));
            scope.ifPresent(s -> props.put("rest.auth.oauth2.scope", s));
            return props;
        }

        /** Create OAuth2 config for Apache Polaris. */
        public static OAuth2ClientCredentials forPolaris(
                String clientId, String clientSecret, String polarisUri, String principalRole) {
            return new OAuth2ClientCredentials(
                    clientId,
                    clientSecret,
                    Optional.of(polarisUri + "/api/catalog/v1/oauth/tokens"),
                    Optional.of("PRINCIPAL_ROLE:" + principalRole));
        }

        /** Create OAuth2 config with custom token endpoint. */
        public static OAuth2ClientCredentials withTokenEndpoint(
                String clientId, String clientSecret, String tokenEndpoint) {
            return new OAuth2ClientCredentials(
                    clientId, clientSecret, Optional.of(tokenEndpoint), Optional.empty());
        }
    }

    // Bearer Token

    /**
     * Bearer token authentication configuration.
     *
     * <p>Used by:
     *
     * <ul>
     *   <li>Project Nessie
     *   <li>Custom REST catalogs
     * </ul>
     *
     * <p>The token can be:
     *
     * <ul>
     *   <li>A pre-obtained JWT from an OIDC provider
     *   <li>A service account token
     *   <li>A token obtained via token exchange
     * </ul>
     *
     * @param token the bearer token
     * @param tokenRefreshCallback optional callback to refresh expired tokens
     */
    record BearerToken(String token, Optional<TokenRefreshCallback> tokenRefreshCallback)
            implements CatalogAuthConfig {
        @Override
        public AuthType authType() {
            return AuthType.BEARER;
        }

        @Override
        public Map<String, String> toIcebergProperties() {
            return Map.of("token", token);
        }

        /** Create bearer token config without refresh. */
        public static BearerToken of(String token) {
            return new BearerToken(token, Optional.empty());
        }

        /** Create bearer token config with refresh callback. */
        public static BearerToken withRefresh(String token, TokenRefreshCallback callback) {
            return new BearerToken(token, Optional.of(callback));
        }

        /** Callback interface for refreshing expired tokens. */
        @FunctionalInterface
        public interface TokenRefreshCallback {
            /**
             * Refresh the token.
             *
             * @param expiredToken the expired token
             * @return new valid token
             */
            String refresh(String expiredToken);
        }
    }
}
