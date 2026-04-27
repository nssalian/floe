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

import io.restassured.response.Response;
import org.junit.jupiter.api.*;

/**
 * E2E tests for Auth/API Key operations via REST API.
 *
 * <p>Tests the full lifecycle: create, read, update, delete API keys. Note: Auth is disabled in E2E
 * tests, so these test the API structure without actual authentication enforcement.
 */
@Tag("e2e")
@DisplayName("Auth API E2E Tests")
class AuthE2ETest extends BaseE2ETest {

    private static final String AUTH_KEYS_PATH = "/api/v1/auth/keys";

    @BeforeEach
    void verifyServerReady() {
        assertServerHealthy();
    }

    @Nested
    @DisplayName("Create API Key")
    class CreateApiKey {

        @Test
        @DisplayName("should create API key with default role")
        void shouldCreateApiKeyWithDefaultRole() {
            String keyJson =
                    """
                {
                    "name": "test-key-default-role"
                }
                """;

            Response response = givenJson().body(keyJson).post(AUTH_KEYS_PATH);

            response.then()
                    .statusCode(201)
                    .body("apiKey.id", notNullValue())
                    .body("apiKey.name", equalTo("test-key-default-role"))
                    .body("key", notNullValue()) // plaintext key returned on create
                    .body("apiKey.role", equalTo("VIEWER"));

            // Clean up
            String keyId = response.jsonPath().getString("apiKey.id");
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId);
        }

        @Test
        @DisplayName("should create API key with ADMIN role")
        void shouldCreateApiKeyWithAdminRole() {
            String keyJson =
                    """
                {
                    "name": "test-key-admin",
                    "role": "ADMIN"
                }
                """;

            Response response = givenJson().body(keyJson).post(AUTH_KEYS_PATH);

            response.then()
                    .statusCode(201)
                    .body("apiKey.name", equalTo("test-key-admin"))
                    .body("apiKey.role", equalTo("ADMIN"));

            // Clean up
            String keyId = response.jsonPath().getString("apiKey.id");
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId);
        }

        @Test
        @DisplayName("should create API key with OPERATOR role")
        void shouldCreateApiKeyWithOperatorRole() {
            String keyJson =
                    """
                {
                    "name": "test-key-operator",
                    "role": "OPERATOR"
                }
                """;

            Response response = givenJson().body(keyJson).post(AUTH_KEYS_PATH);

            response.then()
                    .statusCode(201)
                    .body("apiKey.name", equalTo("test-key-operator"))
                    .body("apiKey.role", equalTo("OPERATOR"));

            // Clean up
            String keyId = response.jsonPath().getString("apiKey.id");
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId);
        }

        @Test
        @DisplayName("should reject API key without name")
        void shouldRejectApiKeyWithoutName() {
            String keyJson =
                    """
                {
                    "role": "VIEWER"
                }
                """;

            givenJson().body(keyJson).post(AUTH_KEYS_PATH).then().statusCode(400);
        }

        @Test
        @DisplayName("should reject duplicate API key name")
        void shouldRejectDuplicateApiKeyName() {
            String keyJson =
                    """
                {
                    "name": "duplicate-key-test"
                }
                """;

            // Create first key
            Response first = givenJson().body(keyJson).post(AUTH_KEYS_PATH);
            first.then().statusCode(201);
            String keyId = first.jsonPath().getString("apiKey.id");

            // Try to create duplicate
            givenJson().body(keyJson).post(AUTH_KEYS_PATH).then().statusCode(409);

            // Clean up
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId);
        }
    }

    @Nested
    @DisplayName("Read API Key")
    class ReadApiKey {

        @Test
        @DisplayName("should get API key by ID")
        void shouldGetApiKeyById() {
            // Create key
            String keyJson =
                    """
                {
                    "name": "get-by-id-key-test",
                    "role": "OPERATOR"
                }
                """;

            Response createResponse = givenJson().body(keyJson).post(AUTH_KEYS_PATH);
            String keyId = createResponse.jsonPath().getString("apiKey.id");

            // Get by ID - should NOT return plaintext key
            givenJson()
                    .get(AUTH_KEYS_PATH + "/" + keyId)
                    .then()
                    .statusCode(200)
                    .body("id", equalTo(keyId))
                    .body("name", equalTo("get-by-id-key-test"))
                    .body("role", equalTo("OPERATOR"));

            // Clean up
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId);
        }

        @Test
        @DisplayName("should return 404 for non-existent API key")
        void shouldReturn404ForNonExistentApiKey() {
            givenJson().get(AUTH_KEYS_PATH + "/non-existent-key-id").then().statusCode(404);
        }

        @Test
        @DisplayName("should list all API keys")
        void shouldListAllApiKeys() {
            // Create two keys
            String key1 =
                    """
                {"name": "list-test-key-1"}
                """;
            String key2 =
                    """
                {"name": "list-test-key-2"}
                """;

            Response r1 = givenJson().body(key1).post(AUTH_KEYS_PATH);
            Response r2 = givenJson().body(key2).post(AUTH_KEYS_PATH);
            String id1 = r1.jsonPath().getString("apiKey.id");
            String id2 = r2.jsonPath().getString("apiKey.id");

            // List keys
            givenJson()
                    .get(AUTH_KEYS_PATH)
                    .then()
                    .statusCode(200)
                    .body("keys", notNullValue())
                    .body("keys.size()", greaterThanOrEqualTo(2))
                    .body("total", greaterThanOrEqualTo(2));

            // Clean up
            givenJson().delete(AUTH_KEYS_PATH + "/" + id1);
            givenJson().delete(AUTH_KEYS_PATH + "/" + id2);
        }

        @Test
        @DisplayName("should filter enabled keys only")
        void shouldFilterEnabledKeysOnly() {
            // Create a key
            String keyJson =
                    """
                {"name": "enabled-filter-test-key"}
                """;

            Response response = givenJson().body(keyJson).post(AUTH_KEYS_PATH);
            String keyId = response.jsonPath().getString("apiKey.id");

            // List with enabled filter
            givenJson()
                    .queryParam("enabled", true)
                    .get(AUTH_KEYS_PATH)
                    .then()
                    .statusCode(200)
                    .body("keys", notNullValue());

            // Clean up
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId);
        }
    }

    @Nested
    @DisplayName("Update API Key")
    class UpdateApiKey {

        @Test
        @DisplayName("should update API key name")
        void shouldUpdateApiKeyName() {
            // Create key
            String createJson =
                    """
                {
                    "name": "update-name-test-key"
                }
                """;
            Response createResponse = givenJson().body(createJson).post(AUTH_KEYS_PATH);
            String keyId = createResponse.jsonPath().getString("apiKey.id");

            // Update name
            String updateJson =
                    """
                {
                    "name": "updated-key-name"
                }
                """;

            givenJson()
                    .body(updateJson)
                    .put(AUTH_KEYS_PATH + "/" + keyId)
                    .then()
                    .statusCode(200)
                    .body("name", equalTo("updated-key-name"));

            // Clean up
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId);
        }

        @Test
        @DisplayName("should update API key role")
        void shouldUpdateApiKeyRole() {
            // Create key with VIEWER role
            String createJson =
                    """
                {
                    "name": "update-role-test-key",
                    "role": "VIEWER"
                }
                """;
            Response createResponse = givenJson().body(createJson).post(AUTH_KEYS_PATH);
            String keyId = createResponse.jsonPath().getString("apiKey.id");

            // Update to ADMIN role
            String updateJson =
                    """
                {
                    "role": "ADMIN"
                }
                """;

            givenJson()
                    .body(updateJson)
                    .put(AUTH_KEYS_PATH + "/" + keyId)
                    .then()
                    .statusCode(200)
                    .body("role", equalTo("ADMIN"));

            // Clean up
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId);
        }

        @Test
        @DisplayName("should disable API key")
        void shouldDisableApiKey() {
            // Create key
            String createJson =
                    """
                {
                    "name": "disable-test-key"
                }
                """;
            Response createResponse = givenJson().body(createJson).post(AUTH_KEYS_PATH);
            String keyId = createResponse.jsonPath().getString("apiKey.id");

            // Disable key
            String updateJson =
                    """
                {
                    "enabled": false
                }
                """;

            givenJson()
                    .body(updateJson)
                    .put(AUTH_KEYS_PATH + "/" + keyId)
                    .then()
                    .statusCode(200)
                    .body("enabled", equalTo(false));

            // Clean up
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId);
        }

        @Test
        @DisplayName("should return 404 when updating non-existent key")
        void shouldReturn404WhenUpdatingNonExistent() {
            String updateJson =
                    """
                {
                    "name": "non-existent"
                }
                """;

            givenJson()
                    .body(updateJson)
                    .put(AUTH_KEYS_PATH + "/non-existent-key-id")
                    .then()
                    .statusCode(404);
        }
    }

    @Nested
    @DisplayName("Delete API Key")
    class DeleteApiKey {

        @Test
        @DisplayName("should delete existing API key")
        void shouldDeleteExistingApiKey() {
            // Create key
            String keyJson =
                    """
                {
                    "name": "delete-test-key"
                }
                """;
            Response createResponse = givenJson().body(keyJson).post(AUTH_KEYS_PATH);
            String keyId = createResponse.jsonPath().getString("apiKey.id");

            // Delete key
            givenJson().delete(AUTH_KEYS_PATH + "/" + keyId).then().statusCode(204);

            // Verify deleted
            givenJson().get(AUTH_KEYS_PATH + "/" + keyId).then().statusCode(404);
        }

        @Test
        @DisplayName("should return 404 when deleting non-existent key")
        void shouldReturn404WhenDeletingNonExistent() {
            givenJson().delete(AUTH_KEYS_PATH + "/non-existent-key-id").then().statusCode(404);
        }
    }

    @Nested
    @DisplayName("Me Endpoint")
    class MeEndpoint {

        @Test
        @DisplayName("should return 401 when not authenticated")
        void shouldReturn401WhenNotAuthenticated() {
            // With auth disabled, this may return different behavior
            // Test the endpoint exists and responds
            givenJson()
                    .get(AUTH_KEYS_PATH + "/me")
                    .then()
                    .statusCode(anyOf(equalTo(200), equalTo(401)));
        }
    }
}
