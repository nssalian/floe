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

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 * Integration test for PolarisCatalogClient using Testcontainers.
 *
 * <p>Spins up Polaris and MinIO containers for testing.
 *
 * <p>Note: Apache Polaris requires significant setup and may not be available in all environments.
 * Tests are skipped if containers cannot start.
 */
@Tag("integration")
@DisplayName("PolarisCatalogClient Integration Tests")
class PolarisCatalogClientIT {

    private static final Logger LOG = LoggerFactory.getLogger(PolarisCatalogClientIT.class);

    private static final String MINIO_ACCESS_KEY = "admin";
    private static final String MINIO_SECRET_KEY = "password";
    private static final String MINIO_BUCKET = "warehouse";
    private static final String CATALOG_NAME = "polaris";

    private static Network network;
    private static GenericContainer<?> minio;
    private static GenericContainer<?> polaris;
    private static String polarisUri;
    private static String s3Endpoint;
    private static String warehouse;
    private static String polarisClientId;
    private static String polarisClientSecret;
    private static String polarisToken;
    private static boolean containersStarted = false;

    // Pattern to extract credentials from Polaris log output
    // Format: "realm: POLARIS root principal credentials: <clientId>:<clientSecret>"
    private static final Pattern CREDENTIALS_PATTERN =
            Pattern.compile("root principal credentials: ([^:]+):(.+)");

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

            // Polaris has separate API (8181) and management (8182) ports
            // Health endpoint is /q/health on management port
            // Capture credentials from container logs
            AtomicReference<String> capturedClientId = new AtomicReference<>();
            AtomicReference<String> capturedClientSecret = new AtomicReference<>();

            polaris =
                    new GenericContainer<>(DockerImageName.parse("apache/polaris:latest"))
                            .withNetwork(network)
                            .withNetworkAliases("polaris")
                            .withExposedPorts(8181, 8182)
                            .waitingFor(
                                    Wait.forHttp("/q/health")
                                            .forPort(8182)
                                            .forStatusCode(200)
                                            .withStartupTimeout(java.time.Duration.ofMinutes(1)))
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("polaris"))
                            .withLogConsumer(
                                    outputFrame -> {
                                        String log = outputFrame.getUtf8String();
                                        Matcher matcher = CREDENTIALS_PATTERN.matcher(log);
                                        if (matcher.find()) {
                                            capturedClientId.set(matcher.group(1));
                                            capturedClientSecret.set(matcher.group(2).trim());
                                            LOG.info(
                                                    "Captured Polaris credentials: clientId={}",
                                                    capturedClientId.get());
                                        }
                                    });

            polaris.start();

            // Wait briefly for credentials to be captured from logs
            Thread.sleep(1000);
            polarisClientId = capturedClientId.get();
            polarisClientSecret = capturedClientSecret.get();

            if (polarisClientId == null || polarisClientSecret == null) {
                LOG.warn("Failed to capture Polaris credentials from logs");
                containersStarted = false;
                return;
            }
            LOG.info(
                    "Polaris credentials captured: clientId={}, clientSecret={}",
                    polarisClientId,
                    polarisClientSecret != null
                            ? polarisClientSecret.substring(
                                            0, Math.min(4, polarisClientSecret.length()))
                                    + "..."
                            : null);

            polarisUri =
                    String.format("http://%s:%d", polaris.getHost(), polaris.getMappedPort(8181));
            s3Endpoint = String.format("http://%s:%d", minio.getHost(), minio.getMappedPort(9000));
            warehouse = "s3://" + MINIO_BUCKET + "/polaris";

            LOG.info("Polaris URI: {}", polarisUri);
            LOG.info("S3 Endpoint: {}", s3Endpoint);

            // Create catalog in Polaris before tests can run
            createPolarisCatalog();

            containersStarted = true;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to start Polaris containers (this is expected in some environments): {}",
                    e.getMessage());
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

    private static void createPolarisCatalog() throws Exception {
        // First get an OAuth token
        String tokenUrl = polarisUri + "/api/catalog/v1/oauth/tokens";
        String tokenBody =
                String.format(
                        "grant_type=client_credentials&client_id=%s&client_secret=%s&scope=PRINCIPAL_ROLE:ALL",
                        polarisClientId, polarisClientSecret);

        HttpURLConnection tokenConn =
                (HttpURLConnection) URI.create(tokenUrl).toURL().openConnection();
        tokenConn.setRequestMethod("POST");
        tokenConn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        tokenConn.setDoOutput(true);
        try (OutputStream os = tokenConn.getOutputStream()) {
            os.write(tokenBody.getBytes(StandardCharsets.UTF_8));
        }

        String tokenResponse =
                new String(tokenConn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String accessToken = tokenResponse.split("\"access_token\":\"")[1].split("\"")[0];
        // Store token for use by tests
        polarisToken = accessToken;
        LOG.info("Obtained Polaris access token");

        // Create the catalog via management API
        String catalogUrl = polarisUri + "/api/management/v1/catalogs";
        String catalogBody =
                String.format(
                        """
            {
                "catalog": {
                    "name": "%s",
                    "type": "INTERNAL",
                    "properties": {
                        "default-base-location": "%s"
                    },
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": ["%s"],
                        "s3": {
                            "endpoint": "%s",
                            "region": "us-east-1",
                            "pathStyleAccess": true
                        }
                    }
                }
            }
            """,
                        CATALOG_NAME, warehouse, warehouse, s3Endpoint);

        HttpURLConnection catalogConn =
                (HttpURLConnection) URI.create(catalogUrl).toURL().openConnection();
        catalogConn.setRequestMethod("POST");
        catalogConn.setRequestProperty("Content-Type", "application/json");
        catalogConn.setRequestProperty("Authorization", "Bearer " + accessToken);
        catalogConn.setDoOutput(true);
        try (OutputStream os = catalogConn.getOutputStream()) {
            os.write(catalogBody.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = catalogConn.getResponseCode();
        if (responseCode == 201 || responseCode == 200) {
            LOG.info("Created Polaris catalog '{}'", CATALOG_NAME);
        } else {
            String error =
                    new String(catalogConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            LOG.warn("Failed to create Polaris catalog: {} - {}", responseCode, error);
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
        if (polaris != null) {
            polaris.stop();
        }
        if (minio != null) {
            minio.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    private PolarisCatalogClient createClient() {
        return PolarisCatalogClient.builder()
                .catalogName(CATALOG_NAME)
                .polarisUri(polarisUri)
                .warehouse(warehouse)
                .token(polarisToken)
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
    @DisplayName("should initialize and connect to Polaris")
    void shouldInitializeAndConnect() {
        assumeTrue(containersStarted, "Polaris containers not started");

        PolarisCatalogClient client = createClient();
        assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
        client.close();
    }

    @Test
    @DisplayName("should list namespaces (empty initially)")
    void shouldListNamespaces() {
        assumeTrue(containersStarted, "Polaris containers not started");

        PolarisCatalogClient client = createClient();
        var namespaces = client.listNamespaces();
        assertThat(namespaces).isNotNull();
        client.close();
    }

    @Test
    @DisplayName("should handle non-existent namespace")
    void shouldHandleNonExistentNamespace() {
        assumeTrue(containersStarted, "Polaris containers not started");

        PolarisCatalogClient client = createClient();
        var tables = client.listTables("nonexistent_namespace");
        assertThat(tables).isEmpty();
        client.close();
    }

    @Test
    @DisplayName("should get catalog name")
    void shouldGetCatalogName() {
        assumeTrue(containersStarted, "Polaris containers not started");

        PolarisCatalogClient client = createClient();
        assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
        client.close();
    }
}
