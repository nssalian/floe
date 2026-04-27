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

package com.floe.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * E2E tests with Postgres persistence.
 *
 * <p>Extends {@link AbstractE2ETest} with PostgreSQL for Floe persistence. Verifies that policies
 * and operations are correctly persisted and retrieved.
 */
@Tag("e2e")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Postgres Persistence E2E Tests")
class PostgresE2ETest extends AbstractE2ETest {

    private static String policyId;

    @Container
    private static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("postgres")
                    .withDatabaseName("floe")
                    .withUsername("floe")
                    .withPassword("floe")
                    .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("postgres"));

    @BeforeAll
    static void setupContainers() throws Exception {
        // Create and configure Floe server with POSTGRES store
        var server =
                createBaseFloeContainer()
                        .withEnv("FLOE_STORE_TYPE", "POSTGRES")
                        .withEnv(
                                "QUARKUS_DATASOURCE_JDBC_URL",
                                "jdbc:postgresql://postgres:5432/floe")
                        .withEnv("QUARKUS_DATASOURCE_USERNAME", "floe")
                        .withEnv("QUARKUS_DATASOURCE_PASSWORD", "floe")
                        .withEnv("QUARKUS_FLYWAY_MIGRATE_AT_START", "true")
                        .dependsOn(postgres);

        startFloeServer(server);
        LOG.info("PostgreSQL available at: {}", postgres.getJdbcUrl());
    }

    @Test
    @Order(1)
    @DisplayName("should verify server is healthy")
    void shouldVerifyServerHealthy() {
        RestAssured.given().get("/health/ready").then().statusCode(200);
        LOG.info("Floe server is healthy");
    }

    @Test
    @Order(2)
    @DisplayName("should create policy and persist to Postgres")
    void shouldCreatePolicyAndPersist() {
        String policyJson =
                """
            {
                "name": "postgres-e2e-test-policy",
                "tablePattern": "demo.test_db.*",
                "enabled": true,
                "priority": 100,
                "description": "E2E test policy with Postgres persistence",
                "rewriteDataFiles": {
                    "targetFileSizeBytes": 134217728
                },
                "expireSnapshots": {
                    "retainLast": 5,
                    "maxSnapshotAge": "P1D"
                }
            }
            """;

        Response response = givenJson().body(policyJson).post("/api/v1/policies");

        response.then()
                .statusCode(201)
                .body("id", notNullValue())
                .body("name", equalTo("postgres-e2e-test-policy"));

        policyId = response.jsonPath().getString("id");
        LOG.info("Created policy with ID: {}", policyId);
    }

    @Test
    @Order(3)
    @DisplayName("should retrieve policy from Postgres")
    void shouldRetrievePolicyFromPostgres() {
        assertThat(policyId).isNotNull();

        givenJson()
                .get("/api/v1/policies/" + policyId)
                .then()
                .statusCode(200)
                .body("id", equalTo(policyId))
                .body("name", equalTo("postgres-e2e-test-policy"))
                .body("enabled", equalTo(true))
                .body("priority", equalTo(100));
    }

    @Test
    @Order(4)
    @DisplayName("should list policies including created policy")
    void shouldListPoliciesIncludingCreated() {
        givenJson()
                .get("/api/v1/policies")
                .then()
                .statusCode(200)
                .body("policies", notNullValue())
                .body("policies.size()", greaterThanOrEqualTo(1))
                .body("policies.name", hasItem("postgres-e2e-test-policy"));
    }

    @Test
    @Order(5)
    @DisplayName("should update policy in Postgres")
    void shouldUpdatePolicyInPostgres() {
        assertThat(policyId).isNotNull();

        String updateJson =
                """
            {
                "name": "postgres-e2e-test-policy",
                "tablePattern": "demo.test_db.*",
                "enabled": false,
                "priority": 200,
                "description": "Updated E2E test policy"
            }
            """;

        givenJson()
                .body(updateJson)
                .put("/api/v1/policies/" + policyId)
                .then()
                .statusCode(200)
                .body("enabled", equalTo(false))
                .body("priority", equalTo(200));

        // Verify update persisted
        givenJson()
                .get("/api/v1/policies/" + policyId)
                .then()
                .statusCode(200)
                .body("enabled", equalTo(false))
                .body("priority", equalTo(200))
                .body("description", equalTo("Updated E2E test policy"));
    }

    @Test
    @Order(6)
    @DisplayName("should create second policy")
    void shouldCreateSecondPolicy() {
        String policyJson =
                """
            {
                "name": "postgres-e2e-second-policy",
                "tablePattern": "demo.other_db.*",
                "enabled": true,
                "priority": 50,
                "rewriteDataFiles": {
                    "targetFileSizeBytes": 268435456
                }
            }
            """;

        givenJson()
                .body(policyJson)
                .post("/api/v1/policies")
                .then()
                .statusCode(201)
                .body("name", equalTo("postgres-e2e-second-policy"));
    }

    @Test
    @Order(7)
    @DisplayName("should list multiple policies")
    void shouldListMultiplePolicies() {
        givenJson()
                .get("/api/v1/policies")
                .then()
                .statusCode(200)
                .body("policies.size()", greaterThanOrEqualTo(2));
    }

    @Test
    @Order(8)
    @DisplayName("should get operations list (empty)")
    void shouldGetOperationsListEmpty() {
        givenJson()
                .get("/api/v1/operations")
                .then()
                .statusCode(200)
                .body("operations", notNullValue());
    }

    @Test
    @Order(9)
    @DisplayName("should get operation stats")
    void shouldGetOperationStats() {
        givenJson().get("/api/v1/operations/stats").then().statusCode(200);
    }

    @Test
    @Order(10)
    @DisplayName("should delete policy from Postgres")
    void shouldDeletePolicyFromPostgres() {
        assertThat(policyId).isNotNull();

        givenJson()
                .delete("/api/v1/policies/" + policyId)
                .then()
                .statusCode(anyOf(equalTo(200), equalTo(204)));

        // Verify deletion
        givenJson().get("/api/v1/policies/" + policyId).then().statusCode(404);

        LOG.info("Deleted policy: {}", policyId);
    }

    @Test
    @Order(11)
    @DisplayName("should cleanup - delete second policy")
    void shouldCleanupSecondPolicy() {
        // Get all policies and delete the second one
        Response response = givenJson().get("/api/v1/policies");
        var policies = response.jsonPath().getList("policies");

        for (Object policy : policies) {
            @SuppressWarnings("unchecked")
            var policyMap = (java.util.Map<String, Object>) policy;
            if ("postgres-e2e-second-policy".equals(policyMap.get("name"))) {
                String id = (String) policyMap.get("id");
                givenJson().delete("/api/v1/policies/" + id);
                LOG.info("Cleaned up policy: {}", id);
            }
        }
    }

    @Test
    @Order(12)
    @DisplayName("should verify all test policies cleaned up")
    void shouldVerifyCleanup() {
        Response response = givenJson().get("/api/v1/policies");
        var names = response.jsonPath().getList("policies.name");

        assertThat(names).doesNotContain("postgres-e2e-test-policy", "postgres-e2e-second-policy");
    }
}
