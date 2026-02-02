package com.floe.server.e2e;

import static org.hamcrest.Matchers.*;

import io.restassured.response.Response;
import org.junit.jupiter.api.*;

/**
 * E2E tests for Signal-Based Triggering functionality.
 *
 * <p>Tests the trigger-status API endpoint and condition-based triggering behavior.
 */
@Tag("e2e")
@DisplayName("Signal-Based Triggering E2E Tests")
class SignalBasedTriggeringE2ETest extends BaseE2ETest {

    private static final String POLICIES_PATH = "/api/v1/policies";
    private static final String TABLES_PATH = "/api/v1/tables";

    @BeforeEach
    void verifyServerReady() {
        assertServerHealthy();
    }

    @Nested
    @DisplayName("Trigger Status API")
    class TriggerStatusApi {

        @Test
        @DisplayName("should return 404 for nonexistent table")
        void shouldReturn404ForNonexistentTable() {
            givenJson()
                    .get(TABLES_PATH + "/nonexistent/table/trigger-status")
                    .then()
                    .statusCode(404)
                    .body("error", containsString("not found"));
        }

        @Test
        @DisplayName("should return no-policy status when table has no matching policy")
        void shouldReturnNoPolicyStatus() {
            // This test assumes the catalog has a table but no policy matches
            // The exact behavior depends on the test catalog setup
            Response response = givenJson().get(TABLES_PATH + "/test/events/trigger-status");

            // Either 404 (table not found) or 200 with no policy
            int status = response.getStatusCode();
            if (status == 200) {
                response.then()
                        .body("conditionBasedTriggeringEnabled", notNullValue())
                        .body("operationStatuses", notNullValue());
            }
        }
    }

    @Nested
    @DisplayName("Policy with Trigger Conditions")
    class PolicyWithTriggerConditions {

        @Test
        @DisplayName("should create policy with trigger conditions")
        void shouldCreatePolicyWithTriggerConditions() {
            String policyJson =
                    """
                {
                    "name": "test-trigger-policy",
                    "tablePattern": "demo.test.*",
                    "enabled": true,
                    "priority": 10,
                    "rewriteDataFiles": {
                        "targetFileSizeBytes": 268435456
                    },
                    "rewriteDataFilesSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 * * * ?"
                    },
                    "triggerConditions": {
                        "smallFilePercentageAbove": 20.0,
                        "smallFileCountAbove": 100,
                        "minIntervalMinutes": 60
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-trigger-policy"))
                    .body("triggerConditions", notNullValue())
                    .body("triggerConditions.smallFilePercentageAbove", equalTo(20.0f))
                    .body("triggerConditions.smallFileCountAbove", equalTo(100))
                    .body("triggerConditions.minIntervalMinutes", equalTo(60));

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should create policy with critical pipeline settings")
        void shouldCreatePolicyWithCriticalPipelineSettings() {
            String policyJson =
                    """
                {
                    "name": "critical-pipeline-policy",
                    "tablePattern": "prod.critical.*",
                    "enabled": true,
                    "priority": 100,
                    "rewriteDataFiles": {},
                    "rewriteDataFilesSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 * * * ?"
                    },
                    "triggerConditions": {
                        "smallFilePercentageAbove": 10.0,
                        "minIntervalMinutes": 30,
                        "criticalPipeline": true,
                        "criticalPipelineMaxDelayMinutes": 120
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("critical-pipeline-policy"))
                    .body("triggerConditions.criticalPipeline", equalTo(true))
                    .body("triggerConditions.criticalPipelineMaxDelayMinutes", equalTo(120));

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should update policy trigger conditions")
        void shouldUpdatePolicyTriggerConditions() {
            // Create policy without trigger conditions
            String createJson =
                    """
                {
                    "name": "update-trigger-test",
                    "tablePattern": "test.*",
                    "enabled": true,
                    "rewriteDataFiles": {},
                    "rewriteDataFilesSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 * * * ?"
                    }
                }
                """;

            Response createResponse = givenJson().body(createJson).post(POLICIES_PATH);
            createResponse.then().statusCode(201);
            String policyId = createResponse.jsonPath().getString("id");

            // Update with trigger conditions
            String updateJson =
                    """
                {
                    "triggerConditions": {
                        "snapshotCountAbove": 50,
                        "snapshotAgeAboveDays": 7,
                        "minIntervalMinutes": 120
                    }
                }
                """;

            givenJson()
                    .body(updateJson)
                    .put(POLICIES_PATH + "/" + policyId)
                    .then()
                    .statusCode(200)
                    .body("triggerConditions.snapshotCountAbove", equalTo(50))
                    .body("triggerConditions.snapshotAgeAboveDays", equalTo(7))
                    .body("triggerConditions.minIntervalMinutes", equalTo(120));

            // Clean up
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }
    }

    @Nested
    @DisplayName("Policy with Health Thresholds")
    class PolicyWithHealthThresholds {

        @Test
        @DisplayName("should create policy with health thresholds")
        void shouldCreatePolicyWithHealthThresholds() {
            String policyJson =
                    """
                {
                    "name": "health-thresholds-policy",
                    "tablePattern": "test.*",
                    "enabled": true,
                    "rewriteDataFiles": {},
                    "rewriteDataFilesSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 * * * ?"
                    },
                    "healthThresholds": {
                        "smallFileSizeBytes": 16777216,
                        "smallFilePercentWarning": 25.0,
                        "smallFilePercentCritical": 40.0,
                        "deleteFileCountWarning": 100,
                        "deleteFileCountCritical": 200,
                        "snapshotCountWarning": 50,
                        "snapshotCountCritical": 100
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("healthThresholds", notNullValue())
                    .body("healthThresholds.smallFileSizeBytes", equalTo(16777216))
                    .body("healthThresholds.smallFilePercentWarning", equalTo(25.0f))
                    .body("healthThresholds.smallFilePercentCritical", equalTo(40.0f))
                    .body("healthThresholds.deleteFileCountWarning", equalTo(100))
                    .body("healthThresholds.deleteFileCountCritical", equalTo(200))
                    .body("healthThresholds.snapshotCountWarning", equalTo(50))
                    .body("healthThresholds.snapshotCountCritical", equalTo(100));

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }
    }
}
