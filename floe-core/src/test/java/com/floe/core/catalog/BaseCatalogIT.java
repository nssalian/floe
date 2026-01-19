package com.floe.core.catalog;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for catalog integration tests using Testcontainers.
 *
 * <p>Provides common infrastructure for MinIO and network setup, with proper resource cleanup using
 * try-with-resources patterns.
 */
@Tag("integration")
public abstract class BaseCatalogIT implements AutoCloseable {

    protected static final Logger LOG = LoggerFactory.getLogger(BaseCatalogIT.class);

    protected static final String MINIO_ACCESS_KEY = "admin";
    protected static final String MINIO_SECRET_KEY = "password";
    protected static final String MINIO_BUCKET = "warehouse";

    protected static Network network;
    protected static GenericContainer<?> minio;
    protected static String s3Endpoint;
    protected static boolean containersStarted = false;

    /** Check if Docker is available. */
    protected static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** Initialize the shared network. */
    protected static void initNetwork() {
        if (network == null) {
            network = Network.newNetwork();
        }
    }

    /** Start MinIO container. */
    protected static void startMinio() {
        if (minio != null && minio.isRunning()) {
            return;
        }

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
        s3Endpoint = String.format("http://%s:%d", minio.getHost(), minio.getMappedPort(9000));
        LOG.info("MinIO started at: {}", s3Endpoint);
    }

    /** Create the MinIO bucket using mc client container. */
    protected static void createMinioBucket() throws Exception {
        try (GenericContainer<?> mcContainer =
                new GenericContainer<>(DockerImageName.parse("minio/mc:latest"))
                        .withNetwork(network)
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

    /** Close a container safely. */
    protected static void closeContainer(GenericContainer<?> container, String name) {
        if (container != null) {
            try {
                container.close();
                LOG.debug("Closed {} container", name);
            } catch (Exception e) {
                LOG.warn("Failed to close {} container", name, e);
            }
        }
    }

    /** Close the network safely. */
    protected static void closeNetwork() {
        if (network != null) {
            try {
                network.close();
                LOG.debug("Closed network");
            } catch (Exception e) {
                LOG.warn("Failed to close network", e);
            }
            network = null;
        }
    }

    /** Clean up all base containers (MinIO and network). */
    protected static void cleanupBaseContainers() {
        closeContainer(minio, "MinIO");
        minio = null;
        closeNetwork();
        containersStarted = false;
    }

    @Override
    public void close() {
        // Subclasses should override to clean up their specific containers
        cleanupBaseContainers();
    }

    @AfterAll
    static void baseTearDown() {
        cleanupBaseContainers();
    }
}
