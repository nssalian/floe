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
