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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PolarisCatalogClientTest {

    // Builder Validation Tests

    @Nested
    class BuilderValidationTests {

        @Test
        void shouldRejectMissingCatalogName() {
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .polarisUri("https://polaris.example.com")
                                            .warehouse("s3://bucket/warehouse")
                                            .clientId("client-id")
                                            .clientSecret("client-secret")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalogName is required");
        }

        @Test
        void shouldRejectBlankCatalogName() {
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .catalogName("   ")
                                            .polarisUri("https://polaris.example.com")
                                            .warehouse("s3://bucket/warehouse")
                                            .clientId("client-id")
                                            .clientSecret("client-secret")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalogName is required");
        }

        @Test
        void shouldRejectMissingPolarisUri() {
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .warehouse("s3://bucket/warehouse")
                                            .clientId("client-id")
                                            .clientSecret("client-secret")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("polarisUri is required");
        }

        @Test
        void shouldRejectBlankPolarisUri() {
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .polarisUri("")
                                            .warehouse("s3://bucket/warehouse")
                                            .clientId("client-id")
                                            .clientSecret("client-secret")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("polarisUri is required");
        }

        @Test
        void shouldRejectMissingWarehouse() {
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .polarisUri("https://polaris.example.com")
                                            .clientId("client-id")
                                            .clientSecret("client-secret")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("warehouse is required");
        }

        @Test
        void shouldRejectBlankWarehouse() {
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .polarisUri("https://polaris.example.com")
                                            .warehouse("  ")
                                            .clientId("client-id")
                                            .clientSecret("client-secret")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("warehouse is required");
        }

        @Test
        void shouldRejectMissingAuthentication() {
            // Neither token nor OAuth2 credentials provided
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .polarisUri("https://polaris.example.com")
                                            .warehouse("s3://bucket/warehouse")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Either token or (clientId + clientSecret) is required");
        }

        @Test
        void shouldRejectPartialOAuth2Credentials() {
            // Only clientId without clientSecret
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .polarisUri("https://polaris.example.com")
                                            .warehouse("s3://bucket/warehouse")
                                            .clientId("client-id")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Either token or (clientId + clientSecret) is required");
        }

        @Test
        void shouldRejectPartialOAuth2CredentialsSecretOnly() {
            // Only clientSecret without clientId
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .polarisUri("https://polaris.example.com")
                                            .warehouse("s3://bucket/warehouse")
                                            .clientSecret("client-secret")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Either token or (clientId + clientSecret) is required");
        }

        @Test
        void shouldAcceptBearerToken() {
            // Token-based auth should work without clientId/clientSecret
            // Note: This will fail at runtime without a real server, but builder validation passes
            assertThatThrownBy(
                            () ->
                                    PolarisCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .polarisUri("https://polaris.example.com")
                                            .warehouse("s3://bucket/warehouse")
                                            .token("bearer-token")
                                            .build())
                    .isInstanceOf(RuntimeException.class) // Connection error, not validation error
                    .isNotInstanceOf(IllegalArgumentException.class);
        }
    }

    // Builder Fluent API Tests

    @Nested
    class BuilderFluentApiTests {

        @Test
        void shouldReturnBuilderFromEachMethod() {
            var builder = PolarisCatalogClient.builder();

            assertThat(builder.catalogName("name")).isSameAs(builder);
            assertThat(builder.polarisUri("uri")).isSameAs(builder);
            assertThat(builder.warehouse("warehouse")).isSameAs(builder);
            assertThat(builder.clientId("id")).isSameAs(builder);
            assertThat(builder.clientSecret("secret")).isSameAs(builder);
            assertThat(builder.principalRole("role")).isSameAs(builder);
            assertThat(builder.additionalProperties(null)).isSameAs(builder);
        }

        @Test
        void shouldCreateBuilderViaStaticMethod() {
            var builder = PolarisCatalogClient.builder();
            assertThat(builder).isNotNull();
        }
    }

    // OAuth2 Configuration Tests

    @Nested
    class OAuth2ConfigTests {

        @Test
        void shouldCreatePolarisOAuth2Config() {
            var config =
                    CatalogAuthConfig.OAuth2ClientCredentials.forPolaris(
                            "my-client", "my-secret", "https://polaris.example.com", "admin");

            assertThat(config.clientId()).isEqualTo("my-client");
            assertThat(config.clientSecret()).isEqualTo("my-secret");
            assertThat(config.tokenEndpoint())
                    .contains("https://polaris.example.com/api/catalog/v1/oauth/tokens");
            assertThat(config.scope()).contains("PRINCIPAL_ROLE:admin");
        }

        @Test
        void shouldGenerateCorrectIcebergProperties() {
            var config =
                    CatalogAuthConfig.OAuth2ClientCredentials.forPolaris(
                            "client", "secret", "https://polaris.example.com", "role");

            var props = config.toIcebergProperties();

            assertThat(props).containsEntry("rest.auth.type", "oauth2");
            assertThat(props).containsEntry("rest.auth.oauth2.credential", "client:secret");
            assertThat(props)
                    .containsEntry(
                            "rest.auth.oauth2.server-uri",
                            "https://polaris.example.com/api/catalog/v1/oauth/tokens");
            assertThat(props).containsEntry("rest.auth.oauth2.scope", "PRINCIPAL_ROLE:role");
        }
    }
}
