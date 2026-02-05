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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Map;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for IcebergRestCatalogClient using Testcontainers.
 *
 * <p>Spins up MinIO and Iceberg REST catalog containers for testing.
 */
@Tag("integration")
@DisplayName("IcebergRestCatalogClient Integration Tests")
class IcebergRestCatalogClientIT {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRestCatalogClientIT.class);

    private static final String MINIO_ACCESS_KEY = "admin";
    private static final String MINIO_SECRET_KEY = "password";
    private static final String MINIO_BUCKET = "warehouse";
    private static final String CATALOG_NAME = "demo";

    private static Network network;
    private static GenericContainer<?> minio;
    private static GenericContainer<?> restCatalog;
    private static String catalogUri;
    private static String s3Endpoint;
    private static boolean containersStarted = false;

    @BeforeAll
    static void setUp() {
        // Check if Docker is available
        if (!isDockerAvailable()) {
            LOG.warn("Docker not available, skipping integration tests");
            return;
        }

        try {
            network = Network.newNetwork();

            minio =
                    new GenericContainer<>(DockerImageName.parse("minio/minio:latest"))
                            .withNetwork(network)
                            .withNetworkAliases("minio")
                            .withExposedPorts(9000)
                            .withEnv("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
                            .withEnv("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
                            .withCommand("server", "/data")
                            .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000))
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("minio"));

            minio.start();

            // Create bucket
            createMinioBucket();

            restCatalog =
                    new GenericContainer<>(DockerImageName.parse("apache/iceberg-rest-fixture"))
                            .withNetwork(network)
                            .withNetworkAliases("rest-catalog")
                            .withExposedPorts(8181)
                            .withEnv("CATALOG_WAREHOUSE", "s3://" + MINIO_BUCKET + "/")
                            .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
                            .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
                            .withEnv("CATALOG_S3_ACCESS__KEY__ID", MINIO_ACCESS_KEY)
                            .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", MINIO_SECRET_KEY)
                            .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
                            .withEnv("AWS_REGION", "us-east-1")
                            .waitingFor(Wait.forHttp("/v1/config").forPort(8181))
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("rest-catalog"));

            restCatalog.start();

            catalogUri =
                    String.format(
                            "http://%s:%d", restCatalog.getHost(), restCatalog.getMappedPort(8181));
            s3Endpoint = String.format("http://%s:%d", minio.getHost(), minio.getMappedPort(9000));

            LOG.info("REST Catalog URI: {}", catalogUri);
            LOG.info("S3 Endpoint: {}", s3Endpoint);

            containersStarted = true;
        } catch (Exception e) {
            LOG.error("Failed to start containers: {}", e.getMessage());
            containersStarted = false;
        }
    }

    private static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void createMinioBucket() throws Exception {
        try (var mcContainer =
                new GenericContainer<>(DockerImageName.parse("minio/mc:latest"))
                        .withNetwork(network)
                        .withCommand(
                                "sh",
                                "-c",
                                String.format(
                                        "mc alias set myminio http://minio:9000 %s %s && mc mb"
                                                + " --ignore-existing myminio/%s",
                                        MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET))) {
            mcContainer.start();
            Thread.sleep(2000);
        }
        LOG.info("MinIO bucket '{}' created", MINIO_BUCKET);
    }

    @AfterAll
    static void tearDown() {
        if (restCatalog != null) {
            restCatalog.stop();
        }
        if (minio != null) {
            minio.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @Test
    @DisplayName("should create client and connect to REST catalog")
    void shouldCreateClientAndConnect() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClient();
        assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
        client.close();
    }

    @Test
    @DisplayName("should list namespaces (empty initially)")
    void shouldListNamespaces() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClient();
        var namespaces = client.listNamespaces();
        assertThat(namespaces).isNotNull();
        client.close();
    }

    @Test
    @DisplayName("should handle non-existent namespace")
    void shouldHandleNonExistentNamespace() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClient();
        var tables = client.listTables("nonexistent_namespace");
        assertThat(tables).isEmpty();
        client.close();
    }

    @Test
    @DisplayName("should return empty for non-existent table")
    void shouldReturnEmptyForNonExistentTable() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClient();
        var tableId = new TableIdentifier(CATALOG_NAME, "test_ns", "nonexistent_table");
        var metadataOpt = client.getTableMetadata(tableId);
        assertThat(metadataOpt).isEmpty();
        client.close();
    }

    @Test
    @DisplayName("should get catalog name")
    void shouldGetCatalogName() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClient();
        assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
        client.close();
    }

    private IcebergRestCatalogClient createClient() {
        return IcebergRestCatalogClient.builder()
                .catalogName(CATALOG_NAME)
                .uri(catalogUri)
                .warehouse("s3://" + MINIO_BUCKET + "/")
                .additionalProperties(
                        Map.of(
                                "io-impl",
                                "org.apache.iceberg.aws.s3.S3FileIO",
                                "s3.endpoint",
                                s3Endpoint,
                                "s3.access-key-id",
                                MINIO_ACCESS_KEY,
                                "s3.secret-access-key",
                                MINIO_SECRET_KEY,
                                "s3.path-style-access",
                                "true"))
                .build();
    }

    private static final String TEST_BEARER_TOKEN = "test-token-for-rest";

    private IcebergRestCatalogClient createClientWithBearerToken() {
        return IcebergRestCatalogClient.builder()
                .catalogName(CATALOG_NAME)
                .uri(catalogUri)
                .warehouse("s3://" + MINIO_BUCKET + "/")
                .token(TEST_BEARER_TOKEN)
                .additionalProperties(
                        Map.of(
                                "io-impl",
                                "org.apache.iceberg.aws.s3.S3FileIO",
                                "s3.endpoint",
                                s3Endpoint,
                                "s3.access-key-id",
                                MINIO_ACCESS_KEY,
                                "s3.secret-access-key",
                                MINIO_SECRET_KEY,
                                "s3.path-style-access",
                                "true"))
                .build();
    }

    private IcebergRestCatalogClient createClientWithOAuth2() {
        return IcebergRestCatalogClient.builder()
                .catalogName(CATALOG_NAME)
                .uri(catalogUri)
                .warehouse("s3://" + MINIO_BUCKET + "/")
                .oauth2("test-client-id", "test-client-secret", catalogUri + "/v1/oauth/tokens")
                .scope("catalog")
                .additionalProperties(
                        Map.of(
                                "io-impl",
                                "org.apache.iceberg.aws.s3.S3FileIO",
                                "s3.endpoint",
                                s3Endpoint,
                                "s3.access-key-id",
                                MINIO_ACCESS_KEY,
                                "s3.secret-access-key",
                                MINIO_SECRET_KEY,
                                "s3.path-style-access",
                                "true"))
                .build();
    }

    @Test
    @DisplayName("should have NoAuth config when no auth provided")
    void shouldHaveNoAuthConfigWithoutAuth() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClient();
        assertThat(client.getAuthConfig()).isNotNull();
        assertThat(client.getAuthConfig().authType()).isEqualTo(CatalogAuthConfig.AuthType.NONE);
        assertThat(client.getUri()).isEqualTo(catalogUri);
        client.close();
    }

    @Test
    @DisplayName("should have Bearer auth config when token provided")
    void shouldHaveBearerAuthConfigWithToken() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClientWithBearerToken();
        assertThat(client.getAuthConfig()).isNotNull();
        assertThat(client.getAuthConfig().authType()).isEqualTo(CatalogAuthConfig.AuthType.BEARER);
        assertThat(client.getAuthConfig()).isInstanceOf(CatalogAuthConfig.BearerToken.class);
        CatalogAuthConfig.BearerToken bearerConfig =
                (CatalogAuthConfig.BearerToken) client.getAuthConfig();
        assertThat(bearerConfig.token()).isEqualTo(TEST_BEARER_TOKEN);
        client.close();
    }

    @Test
    @DisplayName("should have OAuth2 auth config when credentials provided")
    void shouldHaveOAuth2AuthConfigWithCredentials() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClientWithOAuth2();
        assertThat(client.getAuthConfig()).isNotNull();
        assertThat(client.getAuthConfig().authType()).isEqualTo(CatalogAuthConfig.AuthType.OAUTH2);
        assertThat(client.getAuthConfig())
                .isInstanceOf(CatalogAuthConfig.OAuth2ClientCredentials.class);
        CatalogAuthConfig.OAuth2ClientCredentials oauth2Config =
                (CatalogAuthConfig.OAuth2ClientCredentials) client.getAuthConfig();
        assertThat(oauth2Config.clientId()).isEqualTo("test-client-id");
        assertThat(oauth2Config.clientSecret()).isEqualTo("test-client-secret");
        assertThat(oauth2Config.tokenEndpoint()).isPresent();
        assertThat(oauth2Config.scope()).isPresent();
        assertThat(oauth2Config.scope().get()).isEqualTo("catalog");
        client.close();
    }
}
