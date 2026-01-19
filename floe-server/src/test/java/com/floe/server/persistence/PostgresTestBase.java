package com.floe.server.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base class for Postgres integration tests using Testcontainers. Provides a shared Postgres
 * container and DataSource for tests.
 */
@Tag("integration")
public abstract class PostgresTestBase {

    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;

    @BeforeAll
    static void startContainer() {
        if (postgres == null || !postgres.isRunning()) {
            postgres =
                    new PostgreSQLContainer<>("postgres:15-alpine")
                            .withDatabaseName("floe_test")
                            .withUsername("floe")
                            .withPassword("floe_test_password");
            postgres.start();
        }

        if (dataSource == null || dataSource.isClosed()) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(postgres.getJdbcUrl());
            config.setUsername(postgres.getUsername());
            config.setPassword(postgres.getPassword());
            config.setMaximumPoolSize(5);
            config.setMinimumIdle(1);
            config.setConnectionTimeout(10000);
            config.setMaxLifetime(60000);
            config.setIdleTimeout(30000);
            config.setValidationTimeout(3000);
            dataSource = new HikariDataSource(config);
        }
    }

    /** Returns a shared DataSource connected to the test Postgres container. */
    protected static DataSource getDataSource() {
        if (postgres == null || !postgres.isRunning()) {
            startContainer();
        }
        return dataSource;
    }

    /** Returns the JDBC URL for the test container. */
    protected static String getJdbcUrl() {
        return postgres.getJdbcUrl();
    }
}
