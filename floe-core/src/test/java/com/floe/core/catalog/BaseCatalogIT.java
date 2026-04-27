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
 * <p>Provides common infrastructure for SeaweedFS (S3-compatible storage) and network setup, with
 * proper resource cleanup using try-with-resources patterns.
 */
@Tag("integration")
public abstract class BaseCatalogIT implements AutoCloseable {

    protected static final Logger LOG = LoggerFactory.getLogger(
        BaseCatalogIT.class
    );

    protected static final String S3_ACCESS_KEY = "admin";
    protected static final String S3_SECRET_KEY = "password";
    protected static final String S3_BUCKET = "warehouse";

    protected static Network network;
    protected static GenericContainer<?> seaweedfs;
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

    /** Start SeaweedFS container. */
    protected static void startSeaweedFS() {
        if (seaweedfs != null && seaweedfs.isRunning()) {
            return;
        }

        seaweedfs = new GenericContainer<>(
            DockerImageName.parse("chrislusf/seaweedfs:latest")
        )
            .withNetwork(network)
            .withNetworkAliases("seaweedfs")
            .withExposedPorts(8333, 9333)
            .withEnv("AWS_ACCESS_KEY_ID", S3_ACCESS_KEY)
            .withEnv("AWS_SECRET_ACCESS_KEY", S3_SECRET_KEY)
            .withCommand(
                "server",
                "-dir=/data",
                "-s3",
                "-s3.port=8333",
                "-s3.allowEmptyFolder"
            )
            .waitingFor(Wait.forHttp("/cluster/status").forPort(9333))
            .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("seaweedfs"));

        seaweedfs.start();
        s3Endpoint = String.format(
            "http://%s:%d",
            seaweedfs.getHost(),
            seaweedfs.getMappedPort(8333)
        );
        LOG.info("SeaweedFS started at: {}", s3Endpoint);
    }

    /** Create the S3 bucket using an S3 client container. */
    protected static void createS3Bucket() throws Exception {
        try (
            GenericContainer<?> s3ClientContainer = new GenericContainer<>(
                DockerImageName.parse("amazon/aws-cli:latest")
            )
                .withNetwork(network)
                .withCommand(
                    "sh",
                    "-c",
                    String.format(
                        "export AWS_ACCESS_KEY_ID=%s; " +
                            "export AWS_SECRET_ACCESS_KEY=%s; " +
                            "export AWS_DEFAULT_REGION=us-east-1; " +
                            "until aws --endpoint-url http://seaweedfs:8333 s3 ls >/dev/null 2>&1; do sleep 1; done; " +
                            "aws --endpoint-url http://seaweedfs:8333 s3 mb s3://%s || true",
                        S3_ACCESS_KEY,
                        S3_SECRET_KEY,
                        S3_BUCKET
                    )
                )
        ) {
            s3ClientContainer.start();
            Thread.sleep(2000);
        }
        LOG.info("S3 bucket '{}' created", S3_BUCKET);
    }

    /** Close a container safely. */
    protected static void closeContainer(
        GenericContainer<?> container,
        String name
    ) {
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

    /** Clean up all base containers (SeaweedFS and network). */
    protected static void cleanupBaseContainers() {
        closeContainer(seaweedfs, "SeaweedFS");
        seaweedfs = null;
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
