package com.floe.core.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CatalogAuthConfigTest {

    // NoAuth Tests

    @Nested
    class NoAuthTests {

        @Test
        @DisplayName("Should create NoAuth instance")
        void shouldCreateNoAuthInstance() {
            var auth = new CatalogAuthConfig.NoAuth();

            assertThat(auth.authType()).isEqualTo(CatalogAuthConfig.AuthType.NONE);
        }

        @Test
        void shouldReturnEmptyIcebergProperties() {
            var auth = new CatalogAuthConfig.NoAuth();

            assertThat(auth.toIcebergProperties()).isEmpty();
        }
    }

    // OAuth2ClientCredentials Tests

    @Nested
    class OAuth2ClientCredentialsTests {

        @Test
        @DisplayName("Should create OAuth2ClientCredentials instance")
        void shouldCreateOAuth2Config() {
            var auth =
                    new CatalogAuthConfig.OAuth2ClientCredentials(
                            "client-id",
                            "client-secret",
                            Optional.of("https://auth.example.com/token"),
                            Optional.of("read write"));

            assertThat(auth.authType()).isEqualTo(CatalogAuthConfig.AuthType.OAUTH2);
            assertThat(auth.clientId()).isEqualTo("client-id");
            assertThat(auth.clientSecret()).isEqualTo("client-secret");
            assertThat(auth.tokenEndpoint()).contains("https://auth.example.com/token");
            assertThat(auth.scope()).contains("read write");
        }

        @Test
        @DisplayName("Should generate Iceberg properties with all fields")
        void shouldGenerateIcebergPropertiesWithAllFields() {
            var auth =
                    new CatalogAuthConfig.OAuth2ClientCredentials(
                            "my-client",
                            "my-secret",
                            Optional.of("https://auth.example.com/token"),
                            Optional.of("PRINCIPAL_ROLE:admin"));

            Map<String, String> props = auth.toIcebergProperties();

            assertThat(props)
                    .containsEntry("rest.auth.type", "oauth2")
                    .containsEntry("rest.auth.oauth2.credential", "my-client:my-secret")
                    .containsEntry("rest.auth.oauth2.server-uri", "https://auth.example.com/token")
                    .containsEntry("rest.auth.oauth2.scope", "PRINCIPAL_ROLE:admin");
        }

        @Test
        @DisplayName("Should generate Iceberg properties without optional fields")
        void shouldGenerateIcebergPropertiesWithoutOptionalFields() {
            var auth =
                    new CatalogAuthConfig.OAuth2ClientCredentials(
                            "client", "secret", Optional.empty(), Optional.empty());

            Map<String, String> props = auth.toIcebergProperties();

            assertThat(props)
                    .containsEntry("rest.auth.type", "oauth2")
                    .containsEntry("rest.auth.oauth2.credential", "client:secret")
                    .doesNotContainKey("rest.auth.oauth2.server-uri")
                    .doesNotContainKey("rest.auth.oauth2.scope");
        }

        @Test
        @DisplayName("Should create Polaris OAuth2 config")
        void shouldCreatePolarisConfig() {
            var auth =
                    CatalogAuthConfig.OAuth2ClientCredentials.forPolaris(
                            "polaris-client",
                            "polaris-secret",
                            "https://polaris.example.com",
                            "catalog_admin");

            assertThat(auth.clientId()).isEqualTo("polaris-client");
            assertThat(auth.clientSecret()).isEqualTo("polaris-secret");
            assertThat(auth.tokenEndpoint())
                    .contains("https://polaris.example.com/api/catalog/v1/oauth/tokens");
            assertThat(auth.scope()).contains("PRINCIPAL_ROLE:catalog_admin");
        }

        @Test
        @DisplayName("Should create OAuth2 config with custom token endpoint")
        void shouldCreateConfigWithTokenEndpoint() {
            var auth =
                    CatalogAuthConfig.OAuth2ClientCredentials.withTokenEndpoint(
                            "client", "secret", "https://custom.auth.com/oauth/token");

            assertThat(auth.clientId()).isEqualTo("client");
            assertThat(auth.tokenEndpoint()).contains("https://custom.auth.com/oauth/token");
            assertThat(auth.scope()).isEmpty();
        }
    }

    // BearerToken Tests

    @Nested
    class BearerTokenTests {

        @Test
        @DisplayName("Should create BearerToken instance")
        void shouldCreateBearerTokenConfig() {
            var auth = CatalogAuthConfig.BearerToken.of("my-jwt-token");

            assertThat(auth.authType()).isEqualTo(CatalogAuthConfig.AuthType.BEARER);
            assertThat(auth.token()).isEqualTo("my-jwt-token");
            assertThat(auth.tokenRefreshCallback()).isEmpty();
        }

        @Test
        @DisplayName("Should create BearerToken with refresh callback")
        void shouldCreateBearerTokenWithRefreshCallback() {
            CatalogAuthConfig.BearerToken.TokenRefreshCallback callback =
                    expiredToken -> "refreshed-token";

            var auth = CatalogAuthConfig.BearerToken.withRefresh("initial-token", callback);

            assertThat(auth.token()).isEqualTo("initial-token");
            assertThat(auth.tokenRefreshCallback()).isPresent();
            assertThat(auth.tokenRefreshCallback().get().refresh("expired"))
                    .isEqualTo("refreshed-token");
        }

        @Test
        @DisplayName("Should generate Iceberg properties")
        void shouldGenerateIcebergProperties() {
            var auth = CatalogAuthConfig.BearerToken.of("bearer-token-value");

            Map<String, String> props = auth.toIcebergProperties();

            assertThat(props).containsEntry("token", "bearer-token-value").hasSize(1);
        }
    }

    // Sealed Type Tests

    @Nested
    class SealedTypeTests {

        @Test
        @DisplayName("Should match all auth types using pattern matching")
        void shouldMatchAllAuthTypes() {
            // Verify pattern matching works with sealed types
            CatalogAuthConfig[] configs = {
                new CatalogAuthConfig.NoAuth(),
                new CatalogAuthConfig.OAuth2ClientCredentials(
                        "c", "s", Optional.empty(), Optional.empty()),
                CatalogAuthConfig.BearerToken.of("token")
            };

            for (CatalogAuthConfig config : configs) {
                String result =
                        switch (config) {
                            case CatalogAuthConfig.NoAuth n -> "none";
                            case CatalogAuthConfig.OAuth2ClientCredentials o -> "oauth2";
                            case CatalogAuthConfig.BearerToken b -> "bearer";
                        };
                assertThat(result).isNotNull();
            }
        }

        @Test
        @DisplayName("Should have correct auth type for each implementation")
        void shouldHaveCorrectAuthTypeForEachImplementation() {
            assertThat(new CatalogAuthConfig.NoAuth().authType())
                    .isEqualTo(CatalogAuthConfig.AuthType.NONE);
            assertThat(
                            new CatalogAuthConfig.OAuth2ClientCredentials(
                                            "c", "s", Optional.empty(), Optional.empty())
                                    .authType())
                    .isEqualTo(CatalogAuthConfig.AuthType.OAUTH2);
            assertThat(CatalogAuthConfig.BearerToken.of("t").authType())
                    .isEqualTo(CatalogAuthConfig.AuthType.BEARER);
        }
    }
}
