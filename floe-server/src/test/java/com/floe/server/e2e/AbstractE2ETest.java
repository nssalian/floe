package com.floe.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Abstract base class for E2E tests providing common infrastructure.
 *
 * <p>Sets up:
 *
 * <ul>
 *   <li>MinIO for S3-compatible storage
 *   <li>Iceberg REST catalog
 *   <li>Floe server (configured by subclasses)
 * </ul>
 *
 * <p>Subclasses use {@link #createBaseFloeContainer()} to create a Floe server container and add
 * storage-specific configuration before starting it with {@link #startFloeServer}.
 */
@Tag("e2e")
@Testcontainers
public abstract class AbstractE2ETest {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractE2ETest.class);

    protected static final String MINIO_ACCESS_KEY = "admin";
    protected static final String MINIO_SECRET_KEY = "password";
    protected static final String MINIO_BUCKET = "warehouse";
    protected static final String FLOE_IMAGE =
            System.getenv().getOrDefault("FLOE_TEST_IMAGE", "floe:local");

    protected static final Network NETWORK = Network.newNetwork();

    @Container
    protected static final GenericContainer<?> minio =
            new GenericContainer<>(DockerImageName.parse("minio/minio:latest"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("minio")
                    .withExposedPorts(9000, 9001)
                    .withEnv("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
                    .withEnv("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
                    .withCommand("server", "/data", "--console-address", ":9001")
                    .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000))
                    .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("minio"));

    @Container
    protected static final GenericContainer<?> restCatalog =
            new GenericContainer<>(DockerImageName.parse("apache/iceberg-rest-fixture"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("rest-catalog", "rest")
                    .withExposedPorts(8181)
                    .withEnv("CATALOG_WAREHOUSE", "s3://" + MINIO_BUCKET + "/")
                    .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
                    .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
                    .withEnv("CATALOG_S3_ACCESS__KEY__ID", MINIO_ACCESS_KEY)
                    .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", MINIO_SECRET_KEY)
                    .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
                    .withEnv("AWS_REGION", "us-east-1")
                    .dependsOn(minio)
                    .waitingFor(Wait.forHttp("/v1/config").forPort(8181))
                    .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("rest-catalog"));

    protected static GenericContainer<?> floeServer;
    protected static String floeBaseUrl;
    protected static String catalogBaseUrl;

    @BeforeAll
    static void setupContainersBase() throws Exception {
        createMinioBucket();
    }

    protected static void createMinioBucket() throws Exception {
        try (var mcContainer =
                new GenericContainer<>(DockerImageName.parse("minio/mc:latest"))
                        .withNetwork(NETWORK)
                        .withCommand(
                                "sh",
                                "-c",
                                String.format(
                                        "mc alias set myminio http://minio:9000 %s %s && mc mb --ignore-existing myminio/%s",
                                        MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET))) {
            mcContainer.start();
            Thread.sleep(2000);
        }
        LOG.info("MinIO bucket '{}' created", MINIO_BUCKET);
    }

    /**
     * Creates the base Floe server container with common configuration. Subclasses should call this
     * then add storage-specific env vars.
     */
    protected static GenericContainer<?> createBaseFloeContainer() {
        return new GenericContainer<>(DockerImageName.parse(FLOE_IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases("floe-server")
                .withExposedPorts(8080)
                // Server config
                .withEnv("QUARKUS_HTTP_PORT", "8080")
                // Catalog config
                .withEnv("FLOE_CATALOG_TYPE", "REST")
                .withEnv("FLOE_CATALOG_URI", "http://rest-catalog:8181")
                .withEnv("FLOE_CATALOG_REST_URI", "http://rest-catalog:8181")
                .withEnv("FLOE_CATALOG_WAREHOUSE", "s3://" + MINIO_BUCKET + "/")
                .withEnv("FLOE_CATALOG_REST_WAREHOUSE", "s3://" + MINIO_BUCKET + "/")
                .withEnv("FLOE_CATALOG_NAME", "demo")
                // S3 config
                .withEnv("FLOE_CATALOG_S3_ENDPOINT", "http://minio:9000")
                .withEnv("FLOE_CATALOG_S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
                .withEnv("FLOE_CATALOG_S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
                .withEnv("FLOE_CATALOG_S3_ACCESS_KEY", MINIO_ACCESS_KEY)
                .withEnv("FLOE_CATALOG_S3_SECRET_KEY", MINIO_SECRET_KEY)
                .withEnv("FLOE_CATALOG_S3_PATH_STYLE_ACCESS", "true")
                .withEnv("FLOE_CATALOG_S3_REGION", "us-east-1")
                // Disable auth
                .withEnv("FLOE_AUTH_ENABLED", "false")
                .withEnv("QUARKUS_OIDC_TENANT_ENABLED", "false")
                // Disable scheduler
                .withEnv("FLOE_SCHEDULER_ENABLED", "false")
                .withEnv("QUARKUS_SCHEDULER_ENABLED", "false")
                // Disable engine
                .withEnv("FLOE_ENGINE_TYPE", "NONE")
                .dependsOn(restCatalog)
                .waitingFor(
                        Wait.forHttp("/health/ready")
                                .forPort(8080)
                                .forStatusCode(200)
                                .withStartupTimeout(Duration.ofMinutes(2)))
                .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("floe-server"));
    }

    protected static void startFloeServer(GenericContainer<?> server) {
        floeServer = server;
        floeServer.start();

        floeBaseUrl =
                String.format("http://%s:%d", floeServer.getHost(), floeServer.getMappedPort(8080));
        catalogBaseUrl =
                String.format(
                        "http://%s:%d", restCatalog.getHost(), restCatalog.getMappedPort(8181));

        LOG.info("Floe server available at: {}", floeBaseUrl);
        LOG.info("Iceberg REST catalog available at: {}", catalogBaseUrl);
    }

    @BeforeEach
    void setupRestAssured() {
        RestAssured.baseURI = floeBaseUrl;
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    protected RequestSpecification givenJson() {
        return RestAssured.given().contentType(ContentType.JSON).accept(ContentType.JSON);
    }

    protected void assertServerHealthy() {
        var response = RestAssured.given().get("/health/ready");
        assertThat(response.getStatusCode()).isEqualTo(200);
    }
}
