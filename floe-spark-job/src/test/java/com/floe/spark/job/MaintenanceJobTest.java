package com.floe.spark.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("MaintenanceJob")
class MaintenanceJobTest {

    @Nested
    @DisplayName("parseArgs")
    class ParseArgsTests {

        @Test
        @DisplayName("should parse valid arguments")
        void shouldParseValidArguments() {
            String[] args = {
                "REWRITE_DATA_FILES", "demo_catalog", "test_ns", "events", "{}",
            };

            MaintenanceJob.JobArgs result = MaintenanceJob.parseArgs(args);

            assertThat(result).isNotNull();
            assertThat(result.operationType()).isEqualTo("REWRITE_DATA_FILES");
            assertThat(result.catalogName()).isEqualTo("demo_catalog");
            assertThat(result.namespace()).isEqualTo("test_ns");
            assertThat(result.tableName()).isEqualTo("events");
            assertThat(result.configJson()).isEqualTo("{}");
        }

        @Test
        @DisplayName("should return null for null args")
        void shouldReturnNullForNullArgs() {
            MaintenanceJob.JobArgs result = MaintenanceJob.parseArgs(null);

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("should return null for empty args")
        void shouldReturnNullForEmptyArgs() {
            MaintenanceJob.JobArgs result = MaintenanceJob.parseArgs(new String[0]);

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("should return null for insufficient args")
        void shouldReturnNullForInsufficientArgs() {
            String[] args = {
                "REWRITE_DATA_FILES", "catalog", "namespace", "table",
            };

            MaintenanceJob.JobArgs result = MaintenanceJob.parseArgs(args);

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("should accept extra arguments beyond required 5")
        void shouldAcceptExtraArguments() {
            String[] args = {
                "EXPIRE_SNAPSHOTS", "catalog", "ns", "table", "{}", "extra1", "extra2",
            };

            MaintenanceJob.JobArgs result = MaintenanceJob.parseArgs(args);

            assertThat(result).isNotNull();
            assertThat(result.operationType()).isEqualTo("EXPIRE_SNAPSHOTS");
        }

        @Test
        @DisplayName("should preserve complex JSON config")
        void shouldPreserveComplexJsonConfig() {
            String complexJson =
                    "{\"targetFileSizeBytes\":268435456,\"maxConcurrentFileGroupRewrites\":5,\"nested\":{\"key\":\"value\"}}";
            String[] args = {
                "REWRITE_DATA_FILES", "catalog", "ns", "table", complexJson,
            };

            MaintenanceJob.JobArgs result = MaintenanceJob.parseArgs(args);

            assertThat(result.configJson()).isEqualTo(complexJson);
        }
    }

    @Nested
    @DisplayName("JobArgs")
    class JobArgsTests {

        @Test
        @DisplayName("should compute full table name")
        void shouldComputeFullTableName() {
            MaintenanceJob.JobArgs args =
                    new MaintenanceJob.JobArgs(
                            "REWRITE_DATA_FILES", "catalog", "my_namespace", "my_table", "{}");

            assertThat(args.fullTableName()).isEqualTo("my_namespace.my_table");
        }

        @Test
        @DisplayName("should handle multi-level namespace")
        void shouldHandleMultiLevelNamespace() {
            MaintenanceJob.JobArgs args =
                    new MaintenanceJob.JobArgs(
                            "EXPIRE_SNAPSHOTS", "catalog", "db.schema", "events", "{}");

            assertThat(args.fullTableName()).isEqualTo("db.schema.events");
        }

        @Test
        @DisplayName("should be a record with correct components")
        void shouldBeRecordWithCorrectComponents() {
            MaintenanceJob.JobArgs args =
                    new MaintenanceJob.JobArgs("OP", "cat", "ns", "tbl", "{}");

            assertThat(args.operationType()).isEqualTo("OP");
            assertThat(args.catalogName()).isEqualTo("cat");
            assertThat(args.namespace()).isEqualTo("ns");
            assertThat(args.tableName()).isEqualTo("tbl");
            assertThat(args.configJson()).isEqualTo("{}");
        }
    }

    @Nested
    @DisplayName("isValidOperationType")
    class IsValidOperationTypeTests {

        @ParameterizedTest
        @ValueSource(
                strings = {
                    "REWRITE_DATA_FILES",
                    "EXPIRE_SNAPSHOTS",
                    "REWRITE_MANIFESTS",
                    "ORPHAN_CLEANUP",
                })
        @DisplayName("should accept valid operation types")
        void shouldAcceptValidOperationTypes(String operationType) {
            assertThat(MaintenanceJob.isValidOperationType(operationType)).isTrue();
        }

        @ParameterizedTest
        @ValueSource(
                strings = {
                    "rewrite_data_files",
                    "UNKNOWN",
                    "COMPACT",
                    "DELETE",
                    "Rewrite_Data_Files",
                    "REWRITE-DATA-FILES",
                })
        @DisplayName("should reject invalid operation types")
        void shouldRejectInvalidOperationTypes(String operationType) {
            assertThat(MaintenanceJob.isValidOperationType(operationType)).isFalse();
        }

        @ParameterizedTest
        @NullAndEmptySource
        @DisplayName("should reject null and empty operation types")
        void shouldRejectNullAndEmpty(String operationType) {
            assertThat(MaintenanceJob.isValidOperationType(operationType)).isFalse();
        }
    }

    @Nested
    @DisplayName("parseConfig")
    class ParseConfigTests {

        @Test
        @DisplayName("should parse empty config")
        void shouldParseEmptyConfig() {
            JsonNode config = MaintenanceJob.parseConfig("{}");

            assertThat(config).isNotNull();
            assertThat(config.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("should parse RewriteDataFiles config")
        void shouldParseRewriteDataFilesConfig() {
            String configJson =
                    """
                {
                    "strategy": "BINPACK",
                    "targetFileSizeBytes": 268435456,
                    "maxFileGroupSizeBytes": 107374182400,
                    "maxConcurrentFileGroupRewrites": 2,
                    "partialProgressEnabled": true,
                    "partialProgressMaxCommits": 10
                }
                """;

            JsonNode config = MaintenanceJob.parseConfig(configJson);

            assertThat(config.get("strategy").asText()).isEqualTo("BINPACK");
            assertThat(config.get("targetFileSizeBytes").asLong()).isEqualTo(268435456L);
            assertThat(config.get("maxFileGroupSizeBytes").asLong()).isEqualTo(107374182400L);
            assertThat(config.get("maxConcurrentFileGroupRewrites").asInt()).isEqualTo(2);
            assertThat(config.get("partialProgressEnabled").asBoolean()).isTrue();
            assertThat(config.get("partialProgressMaxCommits").asInt()).isEqualTo(10);
        }

        @Test
        @DisplayName("should parse ExpireSnapshots config")
        void shouldParseExpireSnapshotsConfig() {
            // Duration is serialized as seconds (decimal) by Jackson JavaTimeModule
            String configJson =
                    """
                {
                    "retainLast": 10,
                    "maxSnapshotAge": 604800.000000000
                }
                """;

            JsonNode config = MaintenanceJob.parseConfig(configJson);

            assertThat(config.get("retainLast").asInt()).isEqualTo(10);
            assertThat(config.get("maxSnapshotAge").asDouble()).isEqualTo(604800.0);
        }

        @Test
        @DisplayName("should parse OrphanCleanup config")
        void shouldParseOrphanCleanupConfig() {
            // Duration is serialized as seconds (decimal) by Jackson JavaTimeModule
            String configJson =
                    """
                {
                    "olderThan": 259200.000000000
                }
                """;

            JsonNode config = MaintenanceJob.parseConfig(configJson);

            assertThat(config.get("olderThan").asDouble()).isEqualTo(259200.0);
        }

        @Test
        @DisplayName("should handle null values in config")
        void shouldHandleNullValues() {
            String configJson =
                    """
                {
                    "targetFileSizeBytes": null,
                    "maxConcurrentFileGroupRewrites": 5
                }
                """;

            JsonNode config = MaintenanceJob.parseConfig(configJson);

            assertThat(config.has("targetFileSizeBytes")).isTrue();
            assertThat(config.get("targetFileSizeBytes").isNull()).isTrue();
            assertThat(config.get("maxConcurrentFileGroupRewrites").asInt()).isEqualTo(5);
        }

        @Test
        @DisplayName("should handle missing fields gracefully")
        void shouldHandleMissingFields() {
            String configJson = "{\"retainLast\": 5}";

            JsonNode config = MaintenanceJob.parseConfig(configJson);

            assertThat(config.has("retainLast")).isTrue();
            assertThat(config.has("maxSnapshotAge")).isFalse();
            assertThat(config.get("maxSnapshotAge")).isNull();
        }

        @Test
        @DisplayName("should throw MaintenanceJobException on invalid JSON")
        void shouldThrowOnInvalidJson() {
            String invalidJson = "{ invalid json }";

            assertThatThrownBy(() -> MaintenanceJob.parseConfig(invalidJson))
                    .isInstanceOf(MaintenanceJob.MaintenanceJobException.class)
                    .hasMessageContaining("Failed to parse config JSON")
                    .hasCauseInstanceOf(JsonProcessingException.class);
        }

        @Test
        @DisplayName("should throw MaintenanceJobException on malformed JSON")
        void shouldThrowOnMalformedJson() {
            String malformedJson = "{\"key\": }";

            assertThatThrownBy(() -> MaintenanceJob.parseConfig(malformedJson))
                    .isInstanceOf(MaintenanceJob.MaintenanceJobException.class)
                    .hasCauseInstanceOf(Exception.class);
        }

        @Test
        @DisplayName("should parse nested objects")
        void shouldParseNestedObjects() {
            String configJson =
                    """
                {
                    "options": {
                        "spark.sql.shuffle.partitions": "200"
                    }
                }
                """;

            JsonNode config = MaintenanceJob.parseConfig(configJson);

            assertThat(config.has("options")).isTrue();
            assertThat(config.get("options").get("spark.sql.shuffle.partitions").asText())
                    .isEqualTo("200");
        }

        @Test
        @DisplayName("should parse arrays")
        void shouldParseArrays() {
            String configJson =
                    """
                {
                    "partitionFilter": ["date=2026-01-01", "date=2026-01-02"]
                }
                """;

            JsonNode config = MaintenanceJob.parseConfig(configJson);

            assertThat(config.get("partitionFilter").isArray()).isTrue();
            assertThat(config.get("partitionFilter").size()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("MaintenanceJobException")
    class MaintenanceJobExceptionTests {

        @Test
        @DisplayName("should create exception with message and cause")
        void shouldCreateExceptionWithMessageAndCause() {
            Throwable cause = new RuntimeException("Original error");

            MaintenanceJob.MaintenanceJobException exception =
                    new MaintenanceJob.MaintenanceJobException("Failed to load table", cause);

            assertThat(exception.getMessage()).isEqualTo("Failed to load table");
            assertThat(exception.getCause()).isEqualTo(cause);
        }

        @Test
        @DisplayName("should extend RuntimeException")
        void shouldExtendRuntimeException() {
            MaintenanceJob.MaintenanceJobException exception =
                    new MaintenanceJob.MaintenanceJobException("Error", new Exception());

            assertThat(exception).isInstanceOf(RuntimeException.class);
        }

        @Test
        @DisplayName("should preserve cause chain")
        void shouldPreserveCauseChain() {
            Exception rootCause = new IllegalStateException("Root cause");
            Exception intermediateCause = new RuntimeException("Intermediate", rootCause);

            MaintenanceJob.MaintenanceJobException exception =
                    new MaintenanceJob.MaintenanceJobException("Top level", intermediateCause);

            assertThat(exception.getCause()).isEqualTo(intermediateCause);
            assertThat(exception.getCause().getCause()).isEqualTo(rootCause);
        }
    }

    @Nested
    @DisplayName("Constants")
    class ConstantsTests {

        @Test
        @DisplayName("should have expected arg count of 5")
        void shouldHaveExpectedArgCount() {
            assertThat(MaintenanceJob.EXPECTED_ARG_COUNT).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("Integration scenarios")
    class IntegrationScenarios {

        @Test
        @DisplayName("should parse args and config for compaction job")
        void shouldParseArgsAndConfigForCompactionJob() {
            String configJson =
                    """
                {"targetFileSizeBytes":268435456,"maxConcurrentFileGroupRewrites":5}
                """;
            String[] args = {
                "REWRITE_DATA_FILES", "iceberg_catalog", "analytics", "events", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(jobArgs).isNotNull();
            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            assertThat(jobArgs.fullTableName()).isEqualTo("analytics.events");
            assertThat(config.get("targetFileSizeBytes").asLong()).isEqualTo(268435456L);
        }

        @Test
        @DisplayName("should parse args and config for expire snapshots job")
        void shouldParseArgsAndConfigForExpireSnapshotsJob() {
            // Duration serialized as seconds by Jackson JavaTimeModule
            String configJson = "{\"retainLast\":5,\"maxSnapshotAge\":86400.0}";
            String[] args = {
                "EXPIRE_SNAPSHOTS", "prod_catalog", "warehouse.bronze", "orders", configJson,
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            assertThat(jobArgs.fullTableName()).isEqualTo("warehouse.bronze.orders");
            assertThat(config.get("retainLast").asInt()).isEqualTo(5);
            assertThat(config.get("maxSnapshotAge").asDouble()).isEqualTo(86400.0);
        }

        @Test
        @DisplayName("should parse args and config for orphan cleanup job")
        void shouldParseArgsAndConfigForOrphanCleanupJob() {
            // Duration serialized as seconds by Jackson JavaTimeModule
            String configJson = "{\"olderThan\":172800.0}";
            String[] args = {
                "ORPHAN_CLEANUP", "data_catalog", "raw", "clickstream", configJson,
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            assertThat(jobArgs.catalogName()).isEqualTo("data_catalog");
            assertThat(config.get("olderThan").asDouble()).isEqualTo(172800.0);
        }

        @Test
        @DisplayName("should parse args and config for orphan cleanup with equalSchemes")
        void shouldParseArgsAndConfigForOrphanCleanupWithEqualSchemes() {
            // Duration serialized as seconds by Jackson JavaTimeModule
            String configJson =
                    """
                {
                    "olderThan": 259200.0,
                    "prefixMismatchMode": "IGNORE",
                    "equalSchemes": {"s3a": "s3", "s3n": "s3"},
                    "equalAuthorities": {"bucket.s3.amazonaws.com": "bucket"}
                }
                """;
            String[] args = {
                "ORPHAN_CLEANUP", "data_catalog", "raw", "events", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            assertThat(config.get("olderThan").asDouble()).isEqualTo(259200.0);
            assertThat(config.get("prefixMismatchMode").asText()).isEqualTo("IGNORE");
            assertThat(config.get("equalSchemes").isObject()).isTrue();
            assertThat(config.get("equalSchemes").get("s3a").asText()).isEqualTo("s3");
            assertThat(config.get("equalSchemes").get("s3n").asText()).isEqualTo("s3");
            assertThat(config.get("equalAuthorities").isObject()).isTrue();
            assertThat(config.get("equalAuthorities").get("bucket.s3.amazonaws.com").asText())
                    .isEqualTo("bucket");
        }

        @Test
        @DisplayName("should parse args for rewrite manifests job with empty config")
        void shouldParseArgsForRewriteManifestsJob() {
            String[] args = {
                "REWRITE_MANIFESTS", "catalog", "ns", "table", "{}",
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            assertThat(config.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("should parse args and config for compaction with filter")
        void shouldParseArgsAndConfigForCompactionWithFilter() {
            String configJson =
                    """
                {
                    "strategy": "BINPACK",
                    "targetFileSizeBytes": 268435456,
                    "filter": "{\\"type\\":\\"eq\\",\\"term\\":\\"category\\",\\"value\\":\\"electronics\\"}"
                }
                """;
            String[] args = {
                "REWRITE_DATA_FILES", "iceberg_catalog", "analytics", "products", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(jobArgs).isNotNull();
            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            assertThat(config.has("filter")).isTrue();
            assertThat(config.get("filter").asText()).contains("eq");
            assertThat(config.get("filter").asText()).contains("category");
        }

        @Test
        @DisplayName("should parse expire snapshots config with expireSnapshotId")
        void shouldParseExpireSnapshotsWithExpireSnapshotId() {
            String configJson =
                    """
                {
                    "retainLast": 5,
                    "maxSnapshotAge": 604800.0,
                    "cleanExpiredMetadata": true,
                    "expireSnapshotId": 9876543210
                }
                """;
            String[] args = {
                "EXPIRE_SNAPSHOTS", "prod_catalog", "warehouse", "orders", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            assertThat(config.get("retainLast").asInt()).isEqualTo(5);
            assertThat(config.get("maxSnapshotAge").asDouble()).isEqualTo(604800.0);
            assertThat(config.get("cleanExpiredMetadata").asBoolean()).isTrue();
            assertThat(config.get("expireSnapshotId").asLong()).isEqualTo(9876543210L);
        }

        @Test
        @DisplayName("should parse rewrite manifests config with rewriteIf filter")
        void shouldParseRewriteManifestsWithRewriteIf() {
            String configJson =
                    """
                {
                    "specId": 0,
                    "stagingLocation": "/data/staging",
                    "sortBy": ["date", "hour"],
                    "rewriteIf": {
                        "content": "DATA",
                        "addedFilesCount": 100,
                        "snapshotId": 123456789
                    }
                }
                """;
            String[] args = {
                "REWRITE_MANIFESTS", "catalog", "db", "events", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            assertThat(config.get("specId").asInt()).isEqualTo(0);
            assertThat(config.get("stagingLocation").asText()).isEqualTo("/data/staging");
            assertThat(config.get("sortBy").isArray()).isTrue();
            assertThat(config.get("sortBy").size()).isEqualTo(2);
            assertThat(config.get("rewriteIf").isObject()).isTrue();
            assertThat(config.get("rewriteIf").get("content").asText()).isEqualTo("DATA");
            assertThat(config.get("rewriteIf").get("addedFilesCount").asInt()).isEqualTo(100);
            assertThat(config.get("rewriteIf").get("snapshotId").asLong()).isEqualTo(123456789L);
        }

        @Test
        @DisplayName("should parse comprehensive rewriteIf with all 16 ManifestFile fields")
        void shouldParseComprehensiveRewriteIf() {
            String configJson =
                    """
                {
                    "specId": 1,
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
                }
                """;
            String[] args = {
                "REWRITE_MANIFESTS", "catalog", "db", "metrics", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            JsonNode rewriteIf = config.get("rewriteIf");
            assertThat(rewriteIf.isObject()).isTrue();
            assertThat(rewriteIf.get("path").asText()).isEqualTo("/data/manifest.avro");
            assertThat(rewriteIf.get("length").asLong()).isEqualTo(4096L);
            assertThat(rewriteIf.get("specId").asInt()).isEqualTo(0);
            assertThat(rewriteIf.get("content").asText()).isEqualTo("DATA");
            assertThat(rewriteIf.get("sequenceNumber").asLong()).isEqualTo(100L);
            assertThat(rewriteIf.get("minSequenceNumber").asLong()).isEqualTo(50L);
            assertThat(rewriteIf.get("snapshotId").asLong()).isEqualTo(999999L);
            assertThat(rewriteIf.get("addedFilesCount").asInt()).isEqualTo(10);
            assertThat(rewriteIf.get("existingFilesCount").asInt()).isEqualTo(5);
            assertThat(rewriteIf.get("deletedFilesCount").asInt()).isEqualTo(2);
            assertThat(rewriteIf.get("addedRowsCount").asLong()).isEqualTo(10000L);
            assertThat(rewriteIf.get("existingRowsCount").asLong()).isEqualTo(5000L);
            assertThat(rewriteIf.get("deletedRowsCount").asLong()).isEqualTo(200L);
            assertThat(rewriteIf.get("firstRowId").asLong()).isEqualTo(1L);
            assertThat(rewriteIf.get("keyMetadata").asText()).isEqualTo("deadbeef");
            assertThat(rewriteIf.get("partitionSummaries").isArray()).isTrue();
            assertThat(rewriteIf.get("partitionSummaries").size()).isEqualTo(1);
            JsonNode summary = rewriteIf.get("partitionSummaries").get(0);
            assertThat(summary.get("containsNull").asBoolean()).isTrue();
            assertThat(summary.get("containsNan").asBoolean()).isFalse();
            assertThat(summary.get("lowerBound").asText()).isEqualTo("00000000");
            assertThat(summary.get("upperBound").asText()).isEqualTo("ffffffff");
        }

        @Test
        @DisplayName("should parse all 14 RewriteDataFiles options")
        void shouldParseAllRewriteDataFilesOptions() {
            String configJson =
                    """
                {
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
                }
                """;
            String[] args = {
                "REWRITE_DATA_FILES", "catalog", "db", "events", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(MaintenanceJob.isValidOperationType(jobArgs.operationType())).isTrue();
            assertThat(config.get("strategy").asText()).isEqualTo("SORT");
            assertThat(config.get("sortOrder").isArray()).isTrue();
            assertThat(config.get("sortOrder").size()).isEqualTo(2);
            assertThat(config.get("zOrderColumns").isArray()).isTrue();
            assertThat(config.get("targetFileSizeBytes").asLong()).isEqualTo(536870912L);
            assertThat(config.get("maxFileGroupSizeBytes").asLong()).isEqualTo(10737418240L);
            assertThat(config.get("maxConcurrentFileGroupRewrites").asInt()).isEqualTo(4);
            assertThat(config.get("partialProgressEnabled").asBoolean()).isTrue();
            assertThat(config.get("partialProgressMaxCommits").asInt()).isEqualTo(20);
            assertThat(config.get("partialProgressMaxFailedCommits").asInt()).isEqualTo(5);
            assertThat(config.get("filter").asText()).isEqualTo("date > '2024-01-01'");
            assertThat(config.get("rewriteJobOrder").asText()).isEqualTo("FILES_DESC");
            assertThat(config.get("useStartingSequenceNumber").asBoolean()).isTrue();
            assertThat(config.get("removeDanglingDeletes").asBoolean()).isTrue();
            assertThat(config.get("outputSpecId").asInt()).isEqualTo(1);
        }

        @Test
        @DisplayName("should parse all ExpireSnapshots options including expireSnapshotId")
        void shouldParseAllExpireSnapshotsOptions() {
            String configJson =
                    """
                {
                    "retainLast": 10,
                    "maxSnapshotAge": 1209600.0,
                    "cleanExpiredMetadata": true,
                    "expireSnapshotId": 111222333444
                }
                """;
            String[] args = {
                "EXPIRE_SNAPSHOTS", "catalog", "db", "table", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(config.get("retainLast").asInt()).isEqualTo(10);
            assertThat(config.get("maxSnapshotAge").asDouble()).isEqualTo(1209600.0);
            assertThat(config.get("cleanExpiredMetadata").asBoolean()).isTrue();
            assertThat(config.get("expireSnapshotId").asLong()).isEqualTo(111222333444L);
        }

        @Test
        @DisplayName("should parse all OrphanCleanup options")
        void shouldParseAllOrphanCleanupOptions() {
            String configJson =
                    """
                {
                    "olderThan": 604800.0,
                    "location": "/data/warehouse/orphans",
                    "prefixMismatchMode": "DELETE",
                    "equalSchemes": {"s3a": "s3", "s3n": "s3", "hdfs": "file"},
                    "equalAuthorities": {"bucket1.s3.amazonaws.com": "bucket1", "bucket2.s3.amazonaws.com": "bucket2"}
                }
                """;
            String[] args = {
                "ORPHAN_CLEANUP", "catalog", "db", "table", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(config.get("olderThan").asDouble()).isEqualTo(604800.0);
            assertThat(config.get("location").asText()).isEqualTo("/data/warehouse/orphans");
            assertThat(config.get("prefixMismatchMode").asText()).isEqualTo("DELETE");
            assertThat(config.get("equalSchemes").size()).isEqualTo(3);
            assertThat(config.get("equalAuthorities").size()).isEqualTo(2);
        }

        @Test
        @DisplayName("should parse all RewriteManifests options")
        void shouldParseAllRewriteManifestsOptions() {
            String configJson =
                    """
                {
                    "specId": 2,
                    "stagingLocation": "/staging/manifests",
                    "sortBy": ["partition_date", "partition_hour", "region"],
                    "rewriteIf": {
                        "content": "DELETES",
                        "deletedFilesCount": 10
                    }
                }
                """;
            String[] args = {
                "REWRITE_MANIFESTS", "catalog", "db", "table", configJson.trim(),
            };

            MaintenanceJob.JobArgs jobArgs = MaintenanceJob.parseArgs(args);
            JsonNode config = MaintenanceJob.parseConfig(jobArgs.configJson());

            assertThat(config.get("specId").asInt()).isEqualTo(2);
            assertThat(config.get("stagingLocation").asText()).isEqualTo("/staging/manifests");
            assertThat(config.get("sortBy").size()).isEqualTo(3);
            assertThat(config.get("rewriteIf").get("content").asText()).isEqualTo("DELETES");
            assertThat(config.get("rewriteIf").get("deletedFilesCount").asInt()).isEqualTo(10);
        }
    }
}
