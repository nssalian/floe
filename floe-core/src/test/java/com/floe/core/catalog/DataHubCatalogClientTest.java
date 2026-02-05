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

import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DataHubCatalogClientTest {

    // Builder Validation Tests

    @Nested
    class BuilderValidationTests {

        @Test
        void shouldRejectMissingCatalogName() {
            assertThatThrownBy(
                            () ->
                                    DataHubCatalogClient.builder()
                                            .datahubUri("http://datahub-gms:8080")
                                            .warehouse("s3://bucket/warehouse")
                                            .token("my-pat-token")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalogName is required");
        }

        @Test
        void shouldRejectBlankCatalogName() {
            assertThatThrownBy(
                            () ->
                                    DataHubCatalogClient.builder()
                                            .catalogName("   ")
                                            .datahubUri("http://datahub-gms:8080")
                                            .warehouse("s3://bucket/warehouse")
                                            .token("my-pat-token")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalogName is required");
        }

        @Test
        void shouldRejectMissingDatahubUri() {
            assertThatThrownBy(
                            () ->
                                    DataHubCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .warehouse("s3://bucket/warehouse")
                                            .token("my-pat-token")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("datahubUri is required");
        }

        @Test
        void shouldRejectBlankDatahubUri() {
            assertThatThrownBy(
                            () ->
                                    DataHubCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .datahubUri("")
                                            .warehouse("s3://bucket/warehouse")
                                            .token("my-pat-token")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("datahubUri is required");
        }

        @Test
        void shouldRejectMissingWarehouse() {
            assertThatThrownBy(
                            () ->
                                    DataHubCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .datahubUri("http://datahub-gms:8080")
                                            .token("my-pat-token")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("warehouse is required");
        }

        @Test
        void shouldRejectBlankWarehouse() {
            assertThatThrownBy(
                            () ->
                                    DataHubCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .datahubUri("http://datahub-gms:8080")
                                            .warehouse("  ")
                                            .token("my-pat-token")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("warehouse is required");
        }

        @Test
        void shouldRejectMissingToken() {
            assertThatThrownBy(
                            () ->
                                    DataHubCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .datahubUri("http://datahub-gms:8080")
                                            .warehouse("s3://bucket/warehouse")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("token is required");
        }

        @Test
        void shouldRejectBlankToken() {
            assertThatThrownBy(
                            () ->
                                    DataHubCatalogClient.builder()
                                            .catalogName("my-catalog")
                                            .datahubUri("http://datahub-gms:8080")
                                            .warehouse("s3://bucket/warehouse")
                                            .token("   ")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("token is required");
        }
    }

    // Builder Fluent API Tests

    @Nested
    class BuilderFluentApiTests {

        @Test
        void shouldReturnBuilderFromEachMethod() {
            var builder = DataHubCatalogClient.builder();

            assertThat(builder.catalogName("name")).isSameAs(builder);
            assertThat(builder.datahubUri("uri")).isSameAs(builder);
            assertThat(builder.warehouse("warehouse")).isSameAs(builder);
            assertThat(builder.token("token")).isSameAs(builder);
            assertThat(builder.additionalProperties(Map.of())).isSameAs(builder);
        }

        @Test
        void shouldCreateBuilderViaStaticMethod() {
            var builder = DataHubCatalogClient.builder();
            assertThat(builder).isNotNull();
        }
    }

    // Bearer Token Configuration Tests

    @Nested
    class BearerTokenConfigTests {

        @Test
        void shouldCreateBearerTokenConfig() {
            var config = CatalogAuthConfig.BearerToken.of("my-pat-token");

            assertThat(config.token()).isEqualTo("my-pat-token");
            assertThat(config.authType()).isEqualTo(CatalogAuthConfig.AuthType.BEARER);
        }

        @Test
        void shouldGenerateIcebergProperties() {
            var config = CatalogAuthConfig.BearerToken.of("datahub-pat-token");

            var props = config.toIcebergProperties();

            assertThat(props).containsEntry("token", "datahub-pat-token");
            assertThat(props).hasSize(1);
        }
    }

    // Additional Properties Tests

    @Nested
    class AdditionalPropertiesTests {

        @Test
        void shouldAcceptAdditionalProperties() {
            Map<String, String> extraProps =
                    Map.of("s3.endpoint", "http://minio:9000", "s3.path-style-access", "true");

            var builder =
                    DataHubCatalogClient.builder()
                            .catalogName("catalog")
                            .datahubUri("http://datahub-gms:8080")
                            .warehouse("s3://bucket/warehouse")
                            .token("my-pat-token")
                            .additionalProperties(extraProps);

            assertThat(builder).isNotNull();
        }

        @Test
        void shouldAcceptNullAdditionalProperties() {
            var builder =
                    DataHubCatalogClient.builder()
                            .catalogName("catalog")
                            .datahubUri("http://datahub-gms:8080")
                            .warehouse("s3://bucket/warehouse")
                            .token("my-pat-token")
                            .additionalProperties(null);

            assertThat(builder).isNotNull();
        }
    }

    // URI Normalization Tests

    @Nested
    class UriNormalizationTests {

        @Test
        void shouldAcceptUriWithIcebergPath() {
            var builder =
                    DataHubCatalogClient.builder()
                            .catalogName("catalog")
                            .datahubUri("http://datahub-gms:8080/iceberg/")
                            .warehouse("s3://bucket/warehouse")
                            .token("my-pat-token");

            assertThat(builder).isNotNull();
        }

        @Test
        void shouldAcceptUriWithoutIcebergPath() {
            var builder =
                    DataHubCatalogClient.builder()
                            .catalogName("catalog")
                            .datahubUri("http://datahub-gms:8080")
                            .warehouse("s3://bucket/warehouse")
                            .token("my-pat-token");

            assertThat(builder).isNotNull();
        }

        @Test
        void shouldAcceptUriWithTrailingSlash() {
            var builder =
                    DataHubCatalogClient.builder()
                            .catalogName("catalog")
                            .datahubUri("http://datahub-gms:8080/")
                            .warehouse("s3://bucket/warehouse")
                            .token("my-pat-token");

            assertThat(builder).isNotNull();
        }
    }
}
