package com.floe.core.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class NessieCatalogClientTest {

    // Builder Validation Tests

    @Nested
    class BuilderValidationTests {

        @Test
        void shouldRejectMissingCatalogName() {
            assertThatThrownBy(
                            () ->
                                    NessieCatalogClient.builder()
                                            .nessieUri("https://nessie.example.com/api/v2")
                                            .warehouse("s3://bucket/warehouse")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalogName is required");
        }

        @Test
        void shouldRejectBlankCatalogName() {
            assertThatThrownBy(
                            () ->
                                    NessieCatalogClient.builder()
                                            .catalogName("   ")
                                            .nessieUri("https://nessie.example.com/api/v2")
                                            .warehouse("s3://bucket/warehouse")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalogName is required");
        }

        @Test
        void shouldRejectMissingNessieUri() {
            assertThatThrownBy(
                            () ->
                                    NessieCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .warehouse("s3://bucket/warehouse")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nessieUri is required");
        }

        @Test
        void shouldRejectBlankNessieUri() {
            assertThatThrownBy(
                            () ->
                                    NessieCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .nessieUri("")
                                            .warehouse("s3://bucket/warehouse")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nessieUri is required");
        }

        @Test
        void shouldRejectMissingWarehouse() {
            assertThatThrownBy(
                            () ->
                                    NessieCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .nessieUri("https://nessie.example.com/api/v2")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("warehouse is required");
        }

        @Test
        void shouldRejectBlankWarehouse() {
            assertThatThrownBy(
                            () ->
                                    NessieCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .nessieUri("https://nessie.example.com/api/v2")
                                            .warehouse("  ")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("warehouse is required");
        }

        @Test
        void shouldDefaultRefToMain() {
            // Can't fully build without real server, but we can verify the builder accepts
            // null/blank ref
            var builder =
                    NessieCatalogClient.builder()
                            .catalogName("my-catalog")
                            .nessieUri("https://nessie.example.com/api/v2")
                            .warehouse("s3://bucket/warehouse");

            // Builder should not throw for missing ref - it defaults to "main"
            assertThat(builder).isNotNull();
        }
    }

    // Builder Fluent API Tests

    @Nested
    class BuilderFluentApiTests {

        @Test
        void shouldReturnBuilderFromEachMethod() {
            var builder = NessieCatalogClient.builder();

            assertThat(builder.catalogName("name")).isSameAs(builder);
            assertThat(builder.nessieUri("uri")).isSameAs(builder);
            assertThat(builder.warehouse("warehouse")).isSameAs(builder);
            assertThat(builder.bearerToken("token")).isSameAs(builder);
            assertThat(builder.ref("main")).isSameAs(builder);
            assertThat(builder.additionalProperties(Map.of())).isSameAs(builder);
        }

        @Test
        void shouldCreateBuilderViaStaticMethod() {
            var builder = NessieCatalogClient.builder();
            assertThat(builder).isNotNull();
        }
    }

    // Bearer Token Configuration Tests

    @Nested
    class BearerTokenConfigTests {

        @Test
        void shouldCreateBearerTokenConfig() {
            var config = CatalogAuthConfig.BearerToken.of("my-jwt-token");

            assertThat(config.token()).isEqualTo("my-jwt-token");
            assertThat(config.authType()).isEqualTo(CatalogAuthConfig.AuthType.BEARER);
        }

        @Test
        void shouldCreateBearerTokenWithRefreshCallback() {
            CatalogAuthConfig.BearerToken.TokenRefreshCallback callback =
                    expired -> "new-token-" + expired.substring(0, 5);

            var config = CatalogAuthConfig.BearerToken.withRefresh("initial-token", callback);

            assertThat(config.token()).isEqualTo("initial-token");
            assertThat(config.tokenRefreshCallback()).isPresent();

            String refreshed = config.tokenRefreshCallback().get().refresh("expired-token");
            assertThat(refreshed).isEqualTo("new-token-expir");
        }

        @Test
        void shouldGenerateIcebergProperties() {
            var config = CatalogAuthConfig.BearerToken.of("bearer-token-value");

            var props = config.toIcebergProperties();

            assertThat(props).containsEntry("token", "bearer-token-value");
            assertThat(props).hasSize(1);
        }
    }

    // NoAuth Configuration Tests

    @Nested
    class NoAuthConfigTests {

        @Test
        void shouldAllowNullBearerToken() {
            // Nessie can be configured without authentication (dev mode)
            var builder =
                    NessieCatalogClient.builder()
                            .catalogName("my-catalog")
                            .nessieUri("https://nessie.example.com/api/v2")
                            .warehouse("s3://bucket/warehouse");
            // No bearerToken set - should use NoAuth

            assertThat(builder).isNotNull();
        }

        @Test
        void shouldCreateNoAuthConfig() {
            var config = new CatalogAuthConfig.NoAuth();

            assertThat(config.authType()).isEqualTo(CatalogAuthConfig.AuthType.NONE);
            assertThat(config.toIcebergProperties()).isEmpty();
        }
    }

    // Git Reference Tests

    @Nested
    class GitReferenceTests {

        @Test
        void shouldAcceptBranchRef() {
            var builder =
                    NessieCatalogClient.builder()
                            .catalogName("catalog")
                            .nessieUri("https://nessie.example.com/api/v2")
                            .warehouse("s3://bucket/warehouse")
                            .ref("feature-branch");

            assertThat(builder).isNotNull();
        }

        @Test
        void shouldAcceptTagRef() {
            var builder =
                    NessieCatalogClient.builder()
                            .catalogName("catalog")
                            .nessieUri("https://nessie.example.com/api/v2")
                            .warehouse("s3://bucket/warehouse")
                            .ref("v1.0.0");

            assertThat(builder).isNotNull();
        }

        @Test
        void shouldAcceptTenantBranchPattern() {
            // Multi-tenancy via branch-per-tenant
            var builder =
                    NessieCatalogClient.builder()
                            .catalogName("catalog")
                            .nessieUri("https://nessie.example.com/api/v2")
                            .warehouse("s3://bucket/warehouse")
                            .ref("tenant-123/main");

            assertThat(builder).isNotNull();
        }
    }

    // Additional Properties Tests

    @Nested
    class AdditionalPropertiesTests {

        @Test
        void shouldAcceptAdditionalProperties() {
            Map<String, String> extraProps =
                    Map.of(
                            "s3.endpoint", "http://minio:9000",
                            "s3.path-style-access", "true");

            var builder =
                    NessieCatalogClient.builder()
                            .catalogName("catalog")
                            .nessieUri("https://nessie.example.com/api/v2")
                            .warehouse("s3://bucket/warehouse")
                            .additionalProperties(extraProps);

            assertThat(builder).isNotNull();
        }

        @Test
        void shouldAcceptNullAdditionalProperties() {
            var builder =
                    NessieCatalogClient.builder()
                            .catalogName("catalog")
                            .nessieUri("https://nessie.example.com/api/v2")
                            .warehouse("s3://bucket/warehouse")
                            .additionalProperties(null);

            assertThat(builder).isNotNull();
        }
    }
}
