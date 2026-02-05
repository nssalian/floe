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

import java.util.List;
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
 * Integration test for HiveMetastoreCatalogClient using Testcontainers.
 *
 * <p>Note: This test requires iceberg-hive-metastore dependency and its transitive Hadoop
 * dependencies. The test will be skipped if these classes are not available.
 */
@Tag("integration")
@DisplayName("HiveMetastoreCatalogClient Integration Tests")
class HiveMetastoreCatalogClientIT {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreCatalogClientIT.class);

    private static final String MINIO_ACCESS_KEY = "admin";
    private static final String MINIO_SECRET_KEY = "password";
    private static final String MINIO_BUCKET = "warehouse";
    private static final String CATALOG_NAME = "hive";

    private static boolean setupComplete = false;
    private static String skipReason = null;

    private static Network network;
    private static GenericContainer<?> minio;
    private static GenericContainer<?> hiveMetastore;
    private static String metastoreUri;
    private static String s3Endpoint;
    private static String warehouse;

    // Use CatalogClient interface to avoid class loading of HiveMetastoreCatalogClient
    private static CatalogClient sharedClient;

    @BeforeAll
    static void setUp() {
        if (!isDockerAvailable()) {
            skipReason = "Docker not available";
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

            hiveMetastore =
                    new GenericContainer<>(DockerImageName.parse("apache/hive:4.0.0"))
                            .withNetwork(network)
                            .withNetworkAliases("hive-metastore")
                            .withExposedPorts(9083)
                            .withEnv("SERVICE_NAME", "metastore")
                            .withEnv("DB_DRIVER", "derby")
                            .withEnv("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
                            .withEnv("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
                            .waitingFor(
                                    Wait.forListeningPort()
                                            .withStartupTimeout(java.time.Duration.ofMinutes(3)))
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("hive"));

            hiveMetastore.start();

            metastoreUri =
                    String.format(
                            "thrift://%s:%d",
                            hiveMetastore.getHost(), hiveMetastore.getMappedPort(9083));
            s3Endpoint = String.format("http://%s:%d", minio.getHost(), minio.getMappedPort(9000));
            warehouse = "s3a://" + MINIO_BUCKET + "/hive";

            LOG.info("Hive Metastore URI: {}", metastoreUri);
            LOG.info("S3 Endpoint: {}", s3Endpoint);

            // Create client using reflection to avoid direct class reference
            // This catches both ClassNotFoundException and NoClassDefFoundError
            sharedClient = createClientViaReflection();
            setupComplete = true;
        } catch (ClassNotFoundException e) {
            LOG.warn("Hive classes not available: {}", e.getMessage());
            skipReason = "Hive classes not available: " + e.getMessage();
        } catch (NoClassDefFoundError e) {
            LOG.warn("Missing Hadoop dependency: {}", e.getMessage());
            skipReason = "Missing Hadoop dependency: " + e.getMessage();
        } catch (Exception e) {
            LOG.error("Failed to start containers or create client: {}", e.getMessage());
            skipReason = "Setup failed: " + e.getMessage();
        }
    }

    private static CatalogClient createClientViaReflection() throws Exception {
        Class<?> clazz = Class.forName("com.floe.core.catalog.HiveMetastoreCatalogClient");
        var constructor = clazz.getConstructor(String.class, String.class, String.class, Map.class);

        Map<String, String> props =
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
                        "true",
                        "hadoop.fs.s3a.endpoint",
                        s3Endpoint,
                        "hadoop.fs.s3a.access.key",
                        MINIO_ACCESS_KEY,
                        "hadoop.fs.s3a.secret.key",
                        MINIO_SECRET_KEY,
                        "hadoop.fs.s3a.path.style.access",
                        "true");

        return (CatalogClient)
                constructor.newInstance(CATALOG_NAME, metastoreUri, warehouse, props);
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
        if (sharedClient != null) {
            sharedClient.close();
        }
        if (hiveMetastore != null) {
            hiveMetastore.stop();
        }
        if (minio != null) {
            minio.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    private void checkPreconditions() {
        assumeTrue(setupComplete, skipReason != null ? skipReason : "Setup not complete");
    }

    @Test
    @DisplayName("should initialize and connect to Hive Metastore")
    void shouldInitializeAndConnect() {
        checkPreconditions();
        assertThat(sharedClient.getCatalogName()).isEqualTo(CATALOG_NAME);
    }

    @Test
    @DisplayName("should list namespaces")
    void shouldListNamespaces() {
        checkPreconditions();
        List<String> namespaces = sharedClient.listNamespaces();
        assertThat(namespaces).isNotNull();
    }

    @Test
    @DisplayName("should handle non-existent namespace")
    void shouldHandleNonExistentNamespace() {
        checkPreconditions();
        var tables = sharedClient.listTables("nonexistent_namespace");
        assertThat(tables).isEmpty();
    }

    @Test
    @DisplayName("should get catalog name")
    void shouldGetCatalogName() {
        checkPreconditions();
        assertThat(sharedClient.getCatalogName()).isEqualTo(CATALOG_NAME);
    }
}
