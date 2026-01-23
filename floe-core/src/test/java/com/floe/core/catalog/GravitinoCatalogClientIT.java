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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for Apache Gravitino Iceberg REST Server using Testcontainers.
 *
 * <p>Spins up MinIO, PostgreSQL, and Gravitino Iceberg REST containers for testing. Gravitino
 * Iceberg REST Server implements the Iceberg REST Catalog specification, so we use
 * IcebergRestCatalogClient to connect.
 */
@Tag("integration")
@DisplayName("Gravitino Iceberg REST Catalog Integration Tests")
class GravitinoCatalogClientIT {

    private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogClientIT.class);

    private static final String MINIO_ACCESS_KEY = "admin";
    private static final String MINIO_SECRET_KEY = "password";
    private static final String MINIO_BUCKET = "warehouse";
    private static final String CATALOG_NAME = "demo";

    private static Network network;
    private static GenericContainer<?> minio;
    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> gravitino;
    private static String catalogUri;
    private static String s3Endpoint;
    private static boolean containersStarted = false;

    @BeforeAll
    static void setUp() {
        if (!isDockerAvailable()) {
            LOG.warn("Docker not available, skipping integration tests");
            return;
        }

        try {
            network = Network.newNetwork();

            // Start MinIO
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

            // Start PostgreSQL for Gravitino JDBC backend
            postgres =
                    new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
                            .withNetwork(network)
                            .withNetworkAliases("postgres")
                            .withDatabaseName("gravitino")
                            .withUsername("gravitino")
                            .withPassword("gravitino")
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("postgres"));

            postgres.start();

            // Start Gravitino Iceberg REST Server
            gravitino =
                    new GenericContainer<>(
                                    DockerImageName.parse("apache/gravitino-iceberg-rest:1.0.1"))
                            .withNetwork(network)
                            .withNetworkAliases("gravitino")
                            .withExposedPorts(9001)
                            .withEnv("GRAVITINO_CATALOG_BACKEND", "jdbc")
                            .withEnv("GRAVITINO_JDBC_DRIVER", "org.postgresql.Driver")
                            .withEnv("GRAVITINO_URI", "jdbc:postgresql://postgres:5432/gravitino")
                            .withEnv("GRAVITINO_JDBC_USER", "gravitino")
                            .withEnv("GRAVITINO_JDBC_PASSWORD", "gravitino")
                            .withEnv("GRAVITINO_WAREHOUSE", "s3://warehouse/")
                            .withEnv("GRAVITINO_IO_IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
                            .withEnv("GRAVITINO_S3_ENDPOINT", "http://minio:9000")
                            .withEnv("GRAVITINO_S3_REGION", "us-east-1")
                            .withEnv("GRAVITINO_S3_ACCESS_KEY", MINIO_ACCESS_KEY)
                            .withEnv("GRAVITINO_S3_SECRET_KEY", MINIO_SECRET_KEY)
                            .withEnv("GRAVITINO_S3_PATH_STYLE_ACCESS", "true")
                            .waitingFor(Wait.forHttp("/iceberg/v1/config").forPort(9001))
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("gravitino"));

            gravitino.start();

            catalogUri =
                    String.format(
                            "http://%s:%d/iceberg/",
                            gravitino.getHost(), gravitino.getMappedPort(9001));
            s3Endpoint = String.format("http://%s:%d", minio.getHost(), minio.getMappedPort(9000));

            LOG.info("Gravitino Catalog URI: {}", catalogUri);
            LOG.info("S3 Endpoint: {}", s3Endpoint);

            containersStarted = true;
        } catch (Exception e) {
            LOG.error("Failed to start containers: {}", e.getMessage(), e);
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
        if (gravitino != null) {
            gravitino.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (minio != null) {
            minio.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @Test
    @DisplayName("should create client and connect to Gravitino catalog")
    void shouldCreateClientAndConnect() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClient();
        assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
        client.close();
    }

    @Test
    @DisplayName("should list namespaces")
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

    @Test
    @DisplayName("should have NoAuth config when no auth provided")
    void shouldHaveNoAuthConfigWithoutAuth() {
        assumeTrue(containersStarted, "Containers not started");

        IcebergRestCatalogClient client = createClient();
        assertThat(client.getAuthConfig()).isNotNull();
        assertThat(client.getAuthConfig().authType()).isEqualTo(CatalogAuthConfig.AuthType.NONE);
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
}
