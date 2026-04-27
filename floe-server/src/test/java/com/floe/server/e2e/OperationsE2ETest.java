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

import static org.hamcrest.Matchers.*;

import org.junit.jupiter.api.*;

/**
 * E2E tests for Operations API.
 *
 * <p>Tests operation listing, filtering, stats, and table-specific queries. Note: Since no actual
 * maintenance runs in E2E tests (scheduler disabled), these tests verify API structure and
 * empty/default responses.
 */
@Tag("e2e")
@DisplayName("Operations API E2E Tests")
class OperationsE2ETest extends BaseE2ETest {

    private static final String OPERATIONS_PATH = "/api/v1/operations";

    @BeforeEach
    void verifyServerReady() {
        assertServerHealthy();
    }

    @Nested
    @DisplayName("List Operations")
    class ListOperations {

        @Test
        @DisplayName("should list operations with default pagination")
        void shouldListOperationsWithDefaultPagination() {
            givenJson()
                    .get(OPERATIONS_PATH)
                    .then()
                    .statusCode(200)
                    .body("operations", notNullValue())
                    .body("total", greaterThanOrEqualTo(0))
                    .body("limit", equalTo(20))
                    .body("offset", equalTo(0))
                    .body("hasMore", notNullValue());
        }

        @Test
        @DisplayName("should list operations with custom pagination")
        void shouldListOperationsWithCustomPagination() {
            givenJson()
                    .queryParam("limit", 10)
                    .queryParam("offset", 0)
                    .get(OPERATIONS_PATH)
                    .then()
                    .statusCode(200)
                    .body("limit", equalTo(10))
                    .body("offset", equalTo(0));
        }

        @Test
        @DisplayName("should reject invalid limit")
        void shouldRejectInvalidLimit() {
            givenJson()
                    .queryParam("limit", 0)
                    .get(OPERATIONS_PATH)
                    .then()
                    .statusCode(400)
                    .body("error", containsString("Limit must be between 1 and 100"));
        }

        @Test
        @DisplayName("should reject limit over 100")
        void shouldRejectLimitOver100() {
            givenJson()
                    .queryParam("limit", 101)
                    .get(OPERATIONS_PATH)
                    .then()
                    .statusCode(400)
                    .body("error", containsString("Limit must be between 1 and 100"));
        }

        @Test
        @DisplayName("should reject negative offset")
        void shouldRejectNegativeOffset() {
            givenJson()
                    .queryParam("offset", -1)
                    .get(OPERATIONS_PATH)
                    .then()
                    .statusCode(400)
                    .body("error", containsString("Offset must be non-negative"));
        }

        @Test
        @DisplayName("should filter by status SUCCESS")
        void shouldFilterByStatusSuccess() {
            givenJson()
                    .queryParam("status", "SUCCESS")
                    .get(OPERATIONS_PATH)
                    .then()
                    .statusCode(200)
                    .body("operations", notNullValue());
        }

        @Test
        @DisplayName("should filter by status FAILED")
        void shouldFilterByStatusFailed() {
            givenJson()
                    .queryParam("status", "FAILED")
                    .get(OPERATIONS_PATH)
                    .then()
                    .statusCode(200)
                    .body("operations", notNullValue());
        }

        @Test
        @DisplayName("should filter by status RUNNING")
        void shouldFilterByStatusRunning() {
            givenJson()
                    .queryParam("status", "RUNNING")
                    .get(OPERATIONS_PATH)
                    .then()
                    .statusCode(200)
                    .body("operations", notNullValue());
        }

        @Test
        @DisplayName("should reject invalid status")
        void shouldRejectInvalidStatus() {
            givenJson()
                    .queryParam("status", "INVALID_STATUS")
                    .get(OPERATIONS_PATH)
                    .then()
                    .statusCode(400)
                    .body("error", containsString("Invalid status"));
        }
    }

    @Nested
    @DisplayName("Get Operation by ID")
    class GetOperationById {

        @Test
        @DisplayName("should return 404 for non-existent operation")
        void shouldReturn404ForNonExistentOperation() {
            givenJson()
                    .get(OPERATIONS_PATH + "/00000000-0000-0000-0000-000000000000")
                    .then()
                    .statusCode(404)
                    .body("error", containsString("Operation not found"));
        }

        @Test
        @DisplayName("should return 400 for invalid UUID")
        void shouldReturn400ForInvalidUuid() {
            givenJson()
                    .get(OPERATIONS_PATH + "/not-a-valid-uuid")
                    .then()
                    .statusCode(400)
                    .body("error", containsString("Invalid UUID"));
        }
    }

    @Nested
    @DisplayName("Operation Stats")
    class OperationStats {

        @Test
        @DisplayName("should get stats with default window")
        void shouldGetStatsWithDefaultWindow() {
            givenJson()
                    .get(OPERATIONS_PATH + "/stats")
                    .then()
                    .statusCode(200)
                    .body("totalOperations", greaterThanOrEqualTo(0))
                    .body("successCount", greaterThanOrEqualTo(0))
                    .body("failedCount", greaterThanOrEqualTo(0))
                    .body("runningCount", greaterThanOrEqualTo(0))
                    .body("successRate", greaterThanOrEqualTo(0.0f))
                    .body("windowStart", notNullValue())
                    .body("windowEnd", notNullValue())
                    .body("windowDuration", notNullValue());
        }

        @Test
        @DisplayName("should get stats with 1h window")
        void shouldGetStatsWith1hWindow() {
            givenJson()
                    .queryParam("window", "1h")
                    .get(OPERATIONS_PATH + "/stats")
                    .then()
                    .statusCode(200)
                    .body("windowDuration", equalTo("1h"));
        }

        @Test
        @DisplayName("should get stats with 24h window")
        void shouldGetStatsWith24hWindow() {
            givenJson()
                    .queryParam("window", "24h")
                    .get(OPERATIONS_PATH + "/stats")
                    .then()
                    .statusCode(200)
                    .body("windowDuration", equalTo("1d")); // 24h is formatted as 1d
        }

        @Test
        @DisplayName("should get stats with 7d window")
        void shouldGetStatsWith7dWindow() {
            givenJson()
                    .queryParam("window", "7d")
                    .get(OPERATIONS_PATH + "/stats")
                    .then()
                    .statusCode(200)
                    .body("windowDuration", equalTo("7d"));
        }

        @Test
        @DisplayName("should reject invalid window format")
        void shouldRejectInvalidWindowFormat() {
            givenJson()
                    .queryParam("window", "invalid")
                    .get(OPERATIONS_PATH + "/stats")
                    .then()
                    .statusCode(400)
                    .body("error", containsString("Invalid window format"));
        }
    }

    @Nested
    @DisplayName("Running Operations")
    class RunningOperations {

        @Test
        @DisplayName("should get running operations")
        void shouldGetRunningOperations() {
            givenJson()
                    .get(OPERATIONS_PATH + "/running")
                    .then()
                    .statusCode(200)
                    .body("operations", notNullValue())
                    .body("total", greaterThanOrEqualTo(0));
        }

        @Test
        @DisplayName("should get running operations with pagination")
        void shouldGetRunningOperationsWithPagination() {
            givenJson()
                    .queryParam("limit", 10)
                    .queryParam("offset", 0)
                    .get(OPERATIONS_PATH + "/running")
                    .then()
                    .statusCode(200)
                    .body("limit", equalTo(10))
                    .body("offset", equalTo(0));
        }
    }

    @Nested
    @DisplayName("Operations by Table")
    class OperationsByTable {

        @Test
        @DisplayName("should get operations for specific table")
        void shouldGetOperationsForSpecificTable() {
            givenJson()
                    .get(OPERATIONS_PATH + "/table/demo/test_db/events")
                    .then()
                    .statusCode(200)
                    .body("operations", notNullValue())
                    .body("table.catalog", equalTo("demo"))
                    .body("table.namespace", equalTo("test_db"))
                    .body("table.name", equalTo("events"))
                    .body("table.qualified", equalTo("demo.test_db.events"));
        }

        @Test
        @DisplayName("should get operations with custom limit")
        void shouldGetOperationsWithCustomLimit() {
            givenJson()
                    .queryParam("limit", 5)
                    .get(OPERATIONS_PATH + "/table/demo/analytics/metrics")
                    .then()
                    .statusCode(200)
                    .body("operations", notNullValue());
        }
    }
}
