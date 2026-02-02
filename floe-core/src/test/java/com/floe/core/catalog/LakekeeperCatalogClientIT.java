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
 * Integration test for Lakekeeper catalog using Testcontainers.
 *
 * <p>Spins up MinIO, PostgreSQL, and Lakekeeper containers for testing. Lakekeeper implements the
 * Iceberg REST Catalog specification, so we use IcebergRestCatalogClient to connect.
 */
@Tag("integration")
@DisplayName("Lakekeeper Catalog Integration Tests")
class LakekeeperCatalogClientIT {

    private static final Logger LOG = LoggerFactory.getLogger(LakekeeperCatalogClientIT.class);

    private static final String MINIO_ACCESS_KEY = "admin";
    private static final String MINIO_SECRET_KEY = "password";
    private static final String MINIO_BUCKET = "warehouse";
    private static final String CATALOG_NAME = "demo";
    private static final String WAREHOUSE_NAME = "demo";
    private static final String DEFAULT_LOCAL_URI = "http://localhost:8181/catalog/demo";
    private static final String DEFAULT_LOCAL_S3_ENDPOINT = "http://localhost:19000";

    private static Network network;
    private static GenericContainer<?> minio;
    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> lakekeeper;
    private static String catalogUri;
    private static String s3Endpoint;
    private static boolean containersStarted = false;
    private static boolean usingLocalStack = false;
    private static String s3AccessKey = MINIO_ACCESS_KEY;
    private static String s3SecretKey = MINIO_SECRET_KEY;

    @BeforeAll
    static void setUp() {
        if (useLocalStack()) {
            catalogUri = System.getenv().getOrDefault("FLOE_IT_LAKEKEEPER_URI", DEFAULT_LOCAL_URI);
            s3Endpoint =
                    System.getenv()
                            .getOrDefault(
                                    "FLOE_IT_LAKEKEEPER_S3_ENDPOINT", DEFAULT_LOCAL_S3_ENDPOINT);
            s3AccessKey =
                    System.getenv().getOrDefault("FLOE_IT_LAKEKEEPER_ACCESS_KEY", MINIO_ACCESS_KEY);
            s3SecretKey =
                    System.getenv().getOrDefault("FLOE_IT_LAKEKEEPER_SECRET_KEY", MINIO_SECRET_KEY);
            usingLocalStack = true;
            containersStarted = true;
            LOG.info("Using local Lakekeeper stack at {}", catalogUri);
            return;
        }

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

            // Start PostgreSQL for Lakekeeper
            postgres =
                    new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
                            .withNetwork(network)
                            .withNetworkAliases("postgres")
                            .withDatabaseName("lakekeeper")
                            .withUsername("lakekeeper")
                            .withPassword("lakekeeper")
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("postgres"));

            postgres.start();

            // Start Lakekeeper
            lakekeeper =
                    new GenericContainer<>(
                                    DockerImageName.parse("quay.io/lakekeeper/catalog:latest"))
                            .withNetwork(network)
                            .withNetworkAliases("lakekeeper")
                            .withExposedPorts(8181)
                            .withEnv(
                                    "LAKEKEEPER__PG_DATABASE_URL_READ",
                                    "postgres://lakekeeper:lakekeeper@postgres:5432/lakekeeper")
                            .withEnv(
                                    "LAKEKEEPER__PG_DATABASE_URL_WRITE",
                                    "postgres://lakekeeper:lakekeeper@postgres:5432/lakekeeper")
                            .withEnv("LAKEKEEPER__PG_ENCRYPTION_KEY", "thisisunsafe")
                            .withEnv("LAKEKEEPER__LISTEN_PORT", "8181")
                            .waitingFor(Wait.forHttp("/health").forPort(8181))
                            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("lakekeeper"));

            lakekeeper.start();

            catalogUri =
                    String.format(
                            "http://%s:%d/catalog/%s",
                            lakekeeper.getHost(), lakekeeper.getMappedPort(8181), WAREHOUSE_NAME);
            s3Endpoint = String.format("http://%s:%d", minio.getHost(), minio.getMappedPort(9000));

            // Create warehouse in Lakekeeper
            createLakekeeperWarehouse();

            LOG.info("Lakekeeper Catalog URI: {}", catalogUri);
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

    private static void createLakekeeperWarehouse() throws Exception {
        String lakekeeperHost = lakekeeper.getHost();
        int lakekeeperPort = lakekeeper.getMappedPort(8181);

        // Use curl container to create warehouse via Lakekeeper management API
        String createWarehouseCmd =
                String.format(
                        "curl -X POST http://lakekeeper:8181/management/v1/warehouse "
                                + "-H 'Content-Type: application/json' "
                                + "-d '{\"warehouse-name\": \"%s\", "
                                + "\"project-id\": \"00000000-0000-0000-0000-000000000000\", "
                                + "\"storage-profile\": {\"type\": \"s3\", \"bucket\": \"%s\", "
                                + "\"region\": \"us-east-1\", \"endpoint\": \"http://minio:9000\", "
                                + "\"path-style-access\": true, "
                                + "\"flavor\": \"minio\", "
                                + "\"sts-enabled\": false}, "
                                + "\"storage-credential\": {\"type\": \"s3\", "
                                + "\"credential-type\": \"access-key\", "
                                + "\"aws-access-key-id\": \"%s\", "
                                + "\"aws-secret-access-key\": \"%s\"}}'",
                        WAREHOUSE_NAME, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY);

        try (var curlContainer =
                new GenericContainer<>(DockerImageName.parse("curlimages/curl:latest"))
                        .withNetwork(network)
                        .withCommand("sh", "-c", createWarehouseCmd)) {
            curlContainer.start();
            Thread.sleep(3000);
        }
        LOG.info("Lakekeeper warehouse '{}' created", WAREHOUSE_NAME);
    }

    @AfterAll
    static void tearDown() {
        if (usingLocalStack) {
            return;
        }
        if (lakekeeper != null) {
            lakekeeper.stop();
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
    @DisplayName("should create client and connect to Lakekeeper catalog")
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
        client.close();
    }

    private IcebergRestCatalogClient createClient() {
        return IcebergRestCatalogClient.builder()
                .catalogName(CATALOG_NAME)
                .uri(catalogUri)
                .warehouse(WAREHOUSE_NAME)
                .additionalProperties(
                        Map.of(
                                "io-impl",
                                "org.apache.iceberg.aws.s3.S3FileIO",
                                "s3.endpoint",
                                s3Endpoint,
                                "s3.access-key-id",
                                s3AccessKey,
                                "s3.secret-access-key",
                                s3SecretKey,
                                "s3.path-style-access",
                                "true"))
                .build();
    }

    private IcebergRestCatalogClient createClientWithOAuth2() {
        return IcebergRestCatalogClient.builder()
                .catalogName(CATALOG_NAME)
                .uri(catalogUri)
                .warehouse(WAREHOUSE_NAME)
                .oauth2("test-client-id", "test-client-secret", catalogUri + "/v1/oauth/tokens")
                .scope("catalog")
                .additionalProperties(
                        Map.of(
                                "io-impl",
                                "org.apache.iceberg.aws.s3.S3FileIO",
                                "s3.endpoint",
                                s3Endpoint,
                                "s3.access-key-id",
                                s3AccessKey,
                                "s3.secret-access-key",
                                s3SecretKey,
                                "s3.path-style-access",
                                "true"))
                .build();
    }

    private static boolean useLocalStack() {
        String useLocal = System.getenv("FLOE_IT_USE_LOCAL_STACK");
        String uri = System.getenv("FLOE_IT_LAKEKEEPER_URI");
        return (Boolean.parseBoolean(useLocal != null ? useLocal : "false")
                || (uri != null && !uri.isBlank()));
    }
}
