package com.floe.server.e2e;

import static org.hamcrest.Matchers.*;

import org.junit.jupiter.api.*;

/**
 * E2E tests for Tables API.
 *
 * <p>Tests listing namespaces, tables, and getting table metadata/health.
 */
@Tag("e2e")
@DisplayName("Tables E2E Tests")
class TablesE2ETest extends BaseE2ETest {

    private static final String TABLES_PATH = "/api/v1/tables";

    @BeforeEach
    void verifyServerReady() {
        assertServerHealthy();
    }

    @Nested
    @DisplayName("List Namespaces")
    class ListNamespaces {

        @Test
        @DisplayName("should list namespaces from catalog")
        void shouldListNamespaces() {
            givenJson()
                    .get(TABLES_PATH + "/namespaces")
                    .then()
                    .statusCode(200)
                    .body("catalog", notNullValue())
                    .body("namespaces", notNullValue())
                    .body("total", greaterThanOrEqualTo(0));
        }
    }

    @Nested
    @DisplayName("List Tables")
    class ListTables {

        @Test
        @DisplayName("should list all tables")
        void shouldListAllTables() {
            givenJson()
                    .get(TABLES_PATH)
                    .then()
                    .statusCode(200)
                    .body("catalog", notNullValue())
                    .body("tables", notNullValue())
                    .body("total", greaterThanOrEqualTo(0));
        }

        @Test
        @DisplayName("should support pagination")
        void shouldSupportPagination() {
            givenJson()
                    .queryParam("limit", 5)
                    .queryParam("offset", 0)
                    .get(TABLES_PATH)
                    .then()
                    .statusCode(200)
                    .body("limit", equalTo(5))
                    .body("offset", equalTo(0));
        }

        @Test
        @DisplayName("should reject invalid limit")
        void shouldRejectInvalidLimit() {
            givenJson()
                    .queryParam("limit", 200)
                    .get(TABLES_PATH)
                    .then()
                    .statusCode(400)
                    .body("error", containsString("Limit"));
        }

        @Test
        @DisplayName("should reject negative offset")
        void shouldRejectNegativeOffset() {
            givenJson()
                    .queryParam("offset", -1)
                    .get(TABLES_PATH)
                    .then()
                    .statusCode(400)
                    .body("error", containsString("Offset"));
        }
    }

    @Nested
    @DisplayName("List Tables in Namespace")
    class ListTablesInNamespace {

        @Test
        @DisplayName("should list tables in namespace")
        void shouldListTablesInNamespace() {
            // Use a namespace that may or may not exist - API should return 200 either way
            givenJson()
                    .get(TABLES_PATH + "/namespaces/test")
                    .then()
                    .statusCode(200)
                    .body("namespace", equalTo("test"))
                    .body("tables", notNullValue());
        }

        @Test
        @DisplayName("should support pagination for namespace")
        void shouldSupportPaginationForNamespace() {
            givenJson()
                    .queryParam("limit", 10)
                    .queryParam("offset", 0)
                    .get(TABLES_PATH + "/namespaces/test")
                    .then()
                    .statusCode(200)
                    .body("limit", equalTo(10))
                    .body("offset", equalTo(0));
        }
    }

    @Nested
    @DisplayName("Get Table")
    class GetTable {

        @Test
        @DisplayName("should return 404 for non-existent table")
        void shouldReturn404ForNonExistentTable() {
            givenJson()
                    .get(TABLES_PATH + "/nonexistent_ns/nonexistent_table")
                    .then()
                    .statusCode(404)
                    .body("error", containsString("not found"));
        }

        @Test
        @DisplayName("HEAD should return 404 for non-existent table")
        void headShouldReturn404ForNonExistentTable() {
            givenJson()
                    .head(TABLES_PATH + "/nonexistent_ns/nonexistent_table")
                    .then()
                    .statusCode(404);
        }
    }

    @Nested
    @DisplayName("Table Health")
    class TableHealth {

        @Test
        @DisplayName("should return 404 for health of non-existent table")
        void shouldReturn404ForHealthOfNonExistentTable() {
            givenJson()
                    .get(TABLES_PATH + "/nonexistent_ns/nonexistent_table/health")
                    .then()
                    .statusCode(404)
                    .body("error", containsString("not found"));
        }
    }
}
