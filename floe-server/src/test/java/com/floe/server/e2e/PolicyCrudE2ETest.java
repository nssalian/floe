package com.floe.server.e2e;

import static org.hamcrest.Matchers.*;

import io.restassured.response.Response;
import org.junit.jupiter.api.*;

/**
 * E2E tests for Policy CRUD operations via REST API.
 *
 * <p>Tests the full lifecycle: create, read, update, delete policies.
 */
@Tag("e2e")
@DisplayName("Policy CRUD E2E Tests")
class PolicyCrudE2ETest extends BaseE2ETest {

    private static final String POLICIES_PATH = "/api/v1/policies";

    @BeforeEach
    void verifyServerReady() {
        assertServerHealthy();
    }

    @Nested
    @DisplayName("Create Policy")
    class CreatePolicy {

        @Test
        @DisplayName("should create a simple policy")
        void shouldCreateSimplePolicy() {
            String policyJson =
                    """
                {
                    "name": "test-policy-simple",
                    "description": "A simple test policy",
                    "tablePattern": "test_catalog.test_db.*",
                    "enabled": true,
                    "priority": 10
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-policy-simple"))
                    .body("description", equalTo("A simple test policy"))
                    .body("tablePattern", equalTo("test_catalog.test_db.*"))
                    .body("enabled", equalTo(true))
                    .body("priority", equalTo(10))
                    .body("id", notNullValue());

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should create policy with compaction config")
        void shouldCreatePolicyWithCompactionConfig() {
            // Note: Quartz cron requires 6 fields (seconds minutes hours day month weekday)
            String policyJson =
                    """
                {
                    "name": "test-policy-compaction",
                    "tablePattern": "prod.analytics.*",
                    "enabled": true,
                    "priority": 50,
                    "rewriteDataFiles": {
                        "targetFileSizeBytes": 268435456,
                        "partialProgressEnabled": true
                    },
                    "rewriteDataFilesSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 2 * * ?"
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            // API returns summarized operation config (enabled + schedule)
            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-policy-compaction"))
                    .body("rewriteDataFiles.enabled", equalTo(true))
                    .body("rewriteDataFiles.schedule", equalTo("0 0 2 * * ?"));

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should create policy with all operations")
        void shouldCreatePolicyWithAllOperations() {
            // Note: Quartz cron requires 6 fields (seconds minutes hours day month weekday)
            String policyJson =
                    """
                {
                    "name": "test-policy-full",
                    "tablePattern": "warehouse.events.*",
                    "enabled": true,
                    "priority": 100,
                    "rewriteDataFiles": {
                        "targetFileSizeBytes": 536870912
                    },
                    "rewriteDataFilesSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 */6 * * ?"
                    },
                    "expireSnapshots": {
                        "retainLast": 5,
                        "maxSnapshotAge": "P7D"
                    },
                    "expireSnapshotsSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 3 * * ?"
                    },
                    "orphanCleanup": {
                        "retentionPeriodInDays": 3
                    },
                    "orphanCleanupSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 4 ? * SUN"
                    },
                    "rewriteManifests": {},
                    "rewriteManifestsSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 5 * * ?"
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-policy-full"))
                    .body("rewriteDataFiles", notNullValue())
                    .body("expireSnapshots", notNullValue())
                    .body("orphanCleanup", notNullValue())
                    .body("rewriteManifests", notNullValue());

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should create policy with health thresholds")
        void shouldCreatePolicyWithHealthThresholds() {
            String policyJson =
                    """
                {
                    "name": "test-policy-thresholds",
                    "tablePattern": "prod.analytics.*",
                    "enabled": true,
                    "priority": 20,
                    "healthThresholds": {
                        "deleteFileRatioWarning": 0.12,
                        "deleteFileRatioCritical": 0.30,
                        "partitionSkewWarning": 4.0,
                        "partitionSkewCritical": 12.0
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-policy-thresholds"))
                    .body("healthThresholds.deleteFileRatioWarning", equalTo(0.12f))
                    .body("healthThresholds.deleteFileRatioCritical", equalTo(0.30f))
                    .body("healthThresholds.partitionSkewWarning", equalTo(4.0f))
                    .body("healthThresholds.partitionSkewCritical", equalTo(12.0f));

            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should reject policy without name")
        void shouldRejectPolicyWithoutName() {
            String policyJson =
                    """
                {
                    "tablePattern": "test.*.*",
                    "enabled": true
                }
                """;

            givenJson().body(policyJson).post(POLICIES_PATH).then().statusCode(400);
        }

        @Test
        @DisplayName("should reject duplicate policy name")
        void shouldRejectDuplicatePolicyName() {
            String policyJson =
                    """
                {
                    "name": "duplicate-test-policy",
                    "tablePattern": "test.*.*",
                    "enabled": true
                }
                """;

            // Create first policy
            Response first = givenJson().body(policyJson).post(POLICIES_PATH);
            first.then().statusCode(201);
            String policyId = first.jsonPath().getString("id");

            // Try to create duplicate
            givenJson().body(policyJson).post(POLICIES_PATH).then().statusCode(409);

            // Clean up
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should create policy with expireSnapshotId")
        void shouldCreatePolicyWithExpireSnapshotId() {
            String policyJson =
                    """
                {
                    "name": "test-policy-expire-snapshot-id",
                    "tablePattern": "prod.warehouse.*",
                    "enabled": true,
                    "priority": 50,
                    "expireSnapshots": {
                        "retainLast": 5,
                        "maxSnapshotAge": "P7D",
                        "cleanExpiredMetadata": true,
                        "expireSnapshotId": 9876543210
                    },
                    "expireSnapshotsSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 3 * * ?"
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-policy-expire-snapshot-id"))
                    .body("expireSnapshots", notNullValue());

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should create policy with all RewriteDataFiles fields")
        void shouldCreatePolicyWithAllRewriteDataFilesFields() {
            String policyJson =
                    """
                {
                    "name": "test-policy-all-rewrite-fields",
                    "tablePattern": "prod.analytics.*",
                    "enabled": true,
                    "priority": 100,
                    "rewriteDataFiles": {
                        "strategy": "SORT",
                        "sortOrder": ["date", "region"],
                        "zOrderColumns": ["customer_id"],
                        "targetFileSizeBytes": 536870912,
                        "maxFileGroupSizeBytes": 10737418240,
                        "maxConcurrentFileGroupRewrites": 4,
                        "partialProgressEnabled": true,
                        "partialProgressMaxCommits": 20,
                        "partialProgressMaxFailedCommits": 5,
                        "filter": "date > '2024-01-01'",
                        "rewriteJobOrder": "FILES_DESC",
                        "useStartingSequenceNumber": true,
                        "removeDanglingDeletes": true,
                        "outputSpecId": 1
                    },
                    "rewriteDataFilesSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 2 * * ?"
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-policy-all-rewrite-fields"))
                    .body("rewriteDataFiles", notNullValue());

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should create policy with all OrphanCleanup fields")
        void shouldCreatePolicyWithAllOrphanCleanupFields() {
            String policyJson =
                    """
                {
                    "name": "test-policy-all-orphan-fields",
                    "tablePattern": "prod.data.*",
                    "enabled": true,
                    "priority": 50,
                    "orphanCleanup": {
                        "retentionPeriodInDays": 7,
                        "location": "/data/warehouse",
                        "prefixMismatchMode": "IGNORE",
                        "equalSchemes": {"s3a": "s3", "s3n": "s3"},
                        "equalAuthorities": {"bucket.s3.amazonaws.com": "bucket"}
                    },
                    "orphanCleanupSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 4 ? * SUN"
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-policy-all-orphan-fields"))
                    .body("orphanCleanup", notNullValue());

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should create policy with RewriteManifests rewriteIf filter")
        void shouldCreatePolicyWithRewriteManifestsRewriteIf() {
            String policyJson =
                    """
                {
                    "name": "test-policy-rewrite-manifests-filter",
                    "tablePattern": "prod.events.*",
                    "enabled": true,
                    "priority": 75,
                    "rewriteManifests": {
                        "specId": 0,
                        "stagingLocation": "/data/staging",
                        "sortBy": ["date", "hour"],
                        "rewriteIf": {
                            "content": "DATA",
                            "addedFilesCount": 100,
                            "snapshotId": 123456789
                        }
                    },
                    "rewriteManifestsSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 5 * * ?"
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-policy-rewrite-manifests-filter"))
                    .body("rewriteManifests", notNullValue());

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should create policy with comprehensive RewriteManifests rewriteIf")
        void shouldCreatePolicyWithComprehensiveRewriteIf() {
            String policyJson =
                    """
                {
                    "name": "test-policy-comprehensive-rewrite-if",
                    "tablePattern": "prod.metrics.*",
                    "enabled": true,
                    "priority": 80,
                    "rewriteManifests": {
                        "specId": 1,
                        "stagingLocation": "/staging/manifests",
                        "sortBy": ["partition_date"],
                        "rewriteIf": {
                            "path": "/data/manifest.avro",
                            "length": 4096,
                            "specId": 0,
                            "content": "DATA",
                            "sequenceNumber": 100,
                            "minSequenceNumber": 50,
                            "snapshotId": 999999,
                            "addedFilesCount": 10,
                            "existingFilesCount": 5,
                            "deletedFilesCount": 2,
                            "addedRowsCount": 10000,
                            "existingRowsCount": 5000,
                            "deletedRowsCount": 200,
                            "firstRowId": 1,
                            "keyMetadata": "deadbeef",
                            "partitionSummaries": [
                                {
                                    "containsNull": true,
                                    "containsNan": false,
                                    "lowerBound": "00000000",
                                    "upperBound": "ffffffff"
                                }
                            ]
                        }
                    },
                    "rewriteManifestsSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 6 * * ?"
                    }
                }
                """;

            Response response = givenJson().body(policyJson).post(POLICIES_PATH);

            response.then()
                    .statusCode(201)
                    .body("name", equalTo("test-policy-comprehensive-rewrite-if"))
                    .body("rewriteManifests", notNullValue());

            // Clean up
            String policyId = response.jsonPath().getString("id");
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }
    }

    @Nested
    @DisplayName("Read Policy")
    class ReadPolicy {

        @Test
        @DisplayName("should get policy by ID")
        void shouldGetPolicyById() {
            // Create policy
            String policyJson =
                    """
                {
                    "name": "get-by-id-test",
                    "tablePattern": "test.*.*",
                    "enabled": true,
                    "priority": 25
                }
                """;

            Response createResponse = givenJson().body(policyJson).post(POLICIES_PATH);
            String policyId = createResponse.jsonPath().getString("id");

            // Get by ID
            givenJson()
                    .get(POLICIES_PATH + "/" + policyId)
                    .then()
                    .statusCode(200)
                    .body("id", equalTo(policyId))
                    .body("name", equalTo("get-by-id-test"))
                    .body("priority", equalTo(25));

            // Clean up
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);
        }

        @Test
        @DisplayName("should return 404 for non-existent policy")
        void shouldReturn404ForNonExistentPolicy() {
            givenJson().get(POLICIES_PATH + "/non-existent-id").then().statusCode(404);
        }

        @Test
        @DisplayName("should list all policies")
        void shouldListAllPolicies() {
            // Create two policies
            String policy1 =
                    """
                {"name": "list-test-1", "tablePattern": "a.*.*", "enabled": true}
                """;
            String policy2 =
                    """
                {"name": "list-test-2", "tablePattern": "b.*.*", "enabled": true}
                """;

            Response r1 = givenJson().body(policy1).post(POLICIES_PATH);
            Response r2 = givenJson().body(policy2).post(POLICIES_PATH);
            String id1 = r1.jsonPath().getString("id");
            String id2 = r2.jsonPath().getString("id");

            // List policies
            givenJson()
                    .get(POLICIES_PATH)
                    .then()
                    .statusCode(200)
                    .body("policies", notNullValue())
                    .body("policies.size()", greaterThanOrEqualTo(2));

            // Clean up
            givenJson().delete(POLICIES_PATH + "/" + id1);
            givenJson().delete(POLICIES_PATH + "/" + id2);
        }

        @Test
        @DisplayName("should support pagination")
        void shouldSupportPagination() {
            // Create 3 policies
            String[] ids = new String[3];
            for (int i = 0; i < 3; i++) {
                String json =
                        String.format(
                                """
                    {"name": "pagination-test-%d", "tablePattern": "t%d.*.*", "enabled": true}
                    """,
                                i, i);
                Response r = givenJson().body(json).post(POLICIES_PATH);
                ids[i] = r.jsonPath().getString("id");
            }

            // Get first page
            givenJson()
                    .queryParam("limit", 2)
                    .queryParam("offset", 0)
                    .get(POLICIES_PATH)
                    .then()
                    .statusCode(200)
                    .body("policies.size()", lessThanOrEqualTo(2));

            // Clean up
            for (String id : ids) {
                givenJson().delete(POLICIES_PATH + "/" + id);
            }
        }
    }

    @Nested
    @DisplayName("Update Policy")
    class UpdatePolicy {

        @Test
        @DisplayName("should update policy fields")
        void shouldUpdatePolicyFields() {
            // Create policy
            String createJson =
                    """
                {
                    "name": "update-test",
                    "tablePattern": "test.*.*",
                    "enabled": true,
                    "priority": 10
                }
                """;
            Response createResponse = givenJson().body(createJson).post(POLICIES_PATH);
            String policyId = createResponse.jsonPath().getString("id");

            // Update policy
            String updateJson =
                    """
                {
                    "description": "Updated description",
                    "enabled": false,
                    "priority": 99
                }
                """;

            givenJson()
                    .body(updateJson)
                    .put(POLICIES_PATH + "/" + policyId)
                    .then()
                    .statusCode(200)
                    .body("description", equalTo("Updated description"))
                    .body("enabled", equalTo(false))
                    .body("priority", equalTo(99));

            // Verify update persisted
            givenJson()
                    .get(POLICIES_PATH + "/" + policyId)
                    .then()
                    .statusCode(200)
                    .body("priority", equalTo(99));

            // Clean up
            givenJson().delete(POLICIES_PATH + "/" + policyId);
        }

        @Test
        @DisplayName("should add operation config to existing policy")
        void shouldAddOperationConfig() {
            // Create policy without operations
            String createJson =
                    """
                {
                    "name": "add-operation-test",
                    "tablePattern": "test.*.*",
                    "enabled": true
                }
                """;
            Response createResponse = givenJson().body(createJson).post(POLICIES_PATH);
            String policyId = createResponse.jsonPath().getString("id");

            // Add compaction config (Quartz cron: 6 fields)
            String updateJson =
                    """
                {
                    "rewriteDataFiles": {
                        "targetFileSizeBytes": 134217728
                    },
                    "rewriteDataFilesSchedule": {
                        "enabled": true,
                        "cronExpression": "0 0 1 * * ?"
                    }
                }
                """;

            // API returns summarized format (enabled + schedule)
            givenJson()
                    .body(updateJson)
                    .put(POLICIES_PATH + "/" + policyId)
                    .then()
                    .statusCode(200)
                    .body("rewriteDataFiles.enabled", equalTo(true))
                    .body("rewriteDataFiles.schedule", equalTo("0 0 1 * * ?"));

            // Clean up
            givenJson().delete(POLICIES_PATH + "/" + policyId);
        }
    }

    @Nested
    @DisplayName("Delete Policy")
    class DeletePolicy {

        @Test
        @DisplayName("should delete existing policy")
        void shouldDeleteExistingPolicy() {
            // Create policy
            String policyJson =
                    """
                {
                    "name": "delete-test",
                    "tablePattern": "test.*.*",
                    "enabled": true
                }
                """;
            Response createResponse = givenJson().body(policyJson).post(POLICIES_PATH);
            String policyId = createResponse.jsonPath().getString("id");

            // Delete policy
            givenJson().delete(POLICIES_PATH + "/" + policyId).then().statusCode(204);

            // Verify deleted
            givenJson().get(POLICIES_PATH + "/" + policyId).then().statusCode(404);
        }

        @Test
        @DisplayName("should return 404 when deleting non-existent policy")
        void shouldReturn404WhenDeletingNonExistent() {
            givenJson().delete(POLICIES_PATH + "/non-existent-policy-id").then().statusCode(404);
        }
    }
}
