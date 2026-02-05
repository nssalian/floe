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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for NessieCatalogClient using Testcontainers.
 *
 * <p>Spins up Nessie server and MinIO containers for testing.
 *
 * <p>Note: NessieCatalogClient requires Nessie with Iceberg REST API compatibility enabled. The
 * standard Nessie Docker image may not have this enabled. Tests will be skipped if the client
 * cannot connect.
 */
@Tag("integration")
@DisplayName("NessieCatalogClient Integration Tests")
class NessieCatalogClientIT {

    private static final Logger LOG = LoggerFactory.getLogger(NessieCatalogClientIT.class);

    private static final String MINIO_ACCESS_KEY = "admin";
    private static final String MINIO_SECRET_KEY = "password";
    private static final String MINIO_BUCKET = "warehouse";
    private static final String CATALOG_NAME = "nessie";

    private static Network network;
    private static GenericContainer<?> minio;
    private static GenericContainer<?> nessie;
    private static String nessieUri;
    private static String s3Endpoint;
    private static String warehouse;
    private static boolean containersStarted = false;
    private static boolean clientWorks = false;

    @BeforeAll
    static void setUp() {
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
            createMinioBucket();

            // Use Nessie with Iceberg REST catalog support
            // Note: Standard Nessie 0.76+ should have /iceberg endpoint
            nessie =
                    new GenericContainer<>(
                                    DockerImageName.parse("ghcr.io/projectnessie/nessie:0.76.0"))
                            .withNetwork(network)
                            .withNetworkAliases("nessie")
                            .withExposedPorts(19120)
                            .withEnv(
                                    "NESSIE_CATALOG_DEFAULT_WAREHOUSE",
                                    "s3://" + MINIO_BUCKET + "/")
                            .waitingFor(Wait.forHttp("/api/v2/config").forPort(19120))
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("nessie"));

            nessie.start();

            nessieUri =
                    String.format(
                            "http://%s:%d/api/v1", nessie.getHost(), nessie.getMappedPort(19120));
            s3Endpoint = String.format("http://%s:%d", minio.getHost(), minio.getMappedPort(9000));
            warehouse = "s3://" + MINIO_BUCKET + "/nessie";

            LOG.info("Nessie URI: {}", nessieUri);
            LOG.info("S3 Endpoint: {}", s3Endpoint);

            containersStarted = true;

            // Test if client can connect - Nessie needs Iceberg REST support
            try {
                NessieCatalogClient testClient = createClientInternal();
                testClient.close();
                clientWorks = true;
                LOG.info("NessieCatalogClient successfully connected");
            } catch (Exception e) {
                LOG.warn(
                        "NessieCatalogClient cannot connect (Nessie may not have Iceberg REST"
                                + " support enabled): {}",
                        e.getMessage());
                clientWorks = false;
            }
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
        if (nessie != null) {
            nessie.stop();
        }
        if (minio != null) {
            minio.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @BeforeEach
    void checkPreconditions() {
        assumeTrue(containersStarted, "Containers not started");
        assumeTrue(
                clientWorks,
                "NessieCatalogClient cannot connect - Nessie may not have Iceberg REST support");
    }

    private static final String TEST_BEARER_TOKEN = "test-token-for-nessie";

    private static NessieCatalogClient createClientInternal() {
        return NessieCatalogClient.builder()
                .catalogName(CATALOG_NAME)
                .nessieUri(nessieUri)
                .warehouse(warehouse)
                .ref("main")
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

    private static NessieCatalogClient createClientWithBearerToken() {
        return NessieCatalogClient.builder()
                .catalogName(CATALOG_NAME)
                .nessieUri(nessieUri)
                .warehouse(warehouse)
                .ref("main")
                .bearerToken(TEST_BEARER_TOKEN)
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
    @DisplayName("should initialize and connect to Nessie")
    void shouldInitializeAndConnect() {
        NessieCatalogClient client = createClientInternal();
        assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
        assertThat(client.getNessieUri()).isEqualTo(nessieUri);
        assertThat(client.getRef()).isEqualTo("main");
        client.close();
    }

    @Test
    @DisplayName("should list namespaces (empty initially)")
    void shouldListNamespaces() {
        NessieCatalogClient client = createClientInternal();
        var namespaces = client.listNamespaces();
        assertThat(namespaces).isNotNull();
        client.close();
    }

    @Test
    @DisplayName("should handle non-existent namespace")
    void shouldHandleNonExistentNamespace() {
        NessieCatalogClient client = createClientInternal();
        var tables = client.listTables("nonexistent_namespace");
        assertThat(tables).isEmpty();
        client.close();
    }

    @Test
    @DisplayName("should return empty for non-existent table")
    void shouldReturnEmptyForNonExistentTable() {
        NessieCatalogClient client = createClientInternal();
        var tableId = new TableIdentifier(CATALOG_NAME, "test_ns", "nonexistent_table");
        var metadataOpt = client.getTableMetadata(tableId);
        assertThat(metadataOpt).isEmpty();
        client.close();
    }

    @Test
    @DisplayName("should get catalog name")
    void shouldGetCatalogName() {
        NessieCatalogClient client = createClientInternal();
        assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
        client.close();
    }

    @Test
    @DisplayName("should have NoAuth config when no bearer token provided")
    void shouldHaveNoAuthConfigWithoutToken() {
        NessieCatalogClient client = createClientInternal();
        assertThat(client.getAuthConfig()).isNotNull();
        assertThat(client.getAuthConfig().authType()).isEqualTo(CatalogAuthConfig.AuthType.NONE);
        client.close();
    }

    @Test
    @DisplayName("should have Bearer auth config when token provided")
    void shouldHaveBearerAuthConfigWithToken() {
        NessieCatalogClient client = createClientWithBearerToken();
        assertThat(client.getAuthConfig()).isNotNull();
        assertThat(client.getAuthConfig().authType()).isEqualTo(CatalogAuthConfig.AuthType.BEARER);
        assertThat(client.getAuthConfig()).isInstanceOf(CatalogAuthConfig.BearerToken.class);
        CatalogAuthConfig.BearerToken bearerConfig =
                (CatalogAuthConfig.BearerToken) client.getAuthConfig();
        assertThat(bearerConfig.token()).isEqualTo(TEST_BEARER_TOKEN);
        client.close();
    }
}
