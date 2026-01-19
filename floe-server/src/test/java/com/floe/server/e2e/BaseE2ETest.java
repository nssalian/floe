package com.floe.server.e2e;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for E2E tests using in-memory storage.
 *
 * <p>Extends {@link AbstractE2ETest} with MEMORY store configuration. This is the default for most
 * E2E tests that don't need Postgres persistence.
 */
@Tag("e2e")
@Testcontainers
@DisplayName("E2E Tests (Memory Store)")
public abstract class BaseE2ETest extends AbstractE2ETest {

    @BeforeAll
    static void setupContainers() throws Exception {
        // Create and configure Floe server with MEMORY store
        var server =
                createBaseFloeContainer()
                        .withEnv("FLOE_STORE_TYPE", "MEMORY")
                        .withEnv("QUARKUS_FLYWAY_MIGRATE_AT_START", "false");

        startFloeServer(server);
    }
}
