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

package com.floe.engine.spark;

import static org.assertj.core.api.Assertions.assertThat;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.engine.EngineCapabilities;
import com.floe.core.engine.EngineType;
import com.floe.core.engine.ExecutionContext;
import com.floe.core.engine.ExecutionResult;
import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.ExpireSnapshotsOperation;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.maintenance.OrphanCleanupOperation;
import com.floe.core.maintenance.RewriteDataFilesOperation;
import com.floe.core.maintenance.RewriteManifestsOperation;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("SparkExecutionEngine")
class SparkExecutionEngineTest {

    private static final String LIVY_URL = "http://livy:8998";
    private static final String JOB_JAR = "/opt/floe/maintenance-job.jar";
    private static final String JOB_CLASS = "com.floe.spark.job.MaintenanceJob";

    private SparkEngineConfig config;
    private SparkExecutionEngine engine;
    private TableIdentifier tableId;
    private ExecutionContext context;

    @BeforeEach
    void setUp() {
        config =
                SparkEngineConfig.builder()
                        .livyUrl(LIVY_URL)
                        .maintenanceJobJar(JOB_JAR)
                        .maintenanceJobClass(JOB_CLASS)
                        .catalogProperties(Map.of("uri", "http://catalog:8181"))
                        .sparkConf(Map.of("spark.executor.instances", "2"))
                        .driverMemory("2g")
                        .executorMemory("4g")
                        .pollIntervalMs(100) // Short for testing
                        .defaultTimeoutSeconds(60)
                        .build();

        engine = new SparkExecutionEngine(config);
        tableId = new TableIdentifier("demo", "test_namespace", "test_table");
        context =
                ExecutionContext.builder(UUID.randomUUID().toString())
                        .timeoutSeconds(60)
                        .catalogProperties(Map.of("catalog.type", "rest"))
                        .build();
    }

    @Nested
    @DisplayName("Engine Properties")
    class EngineProperties {

        @Test
        @DisplayName("should return correct engine name")
        void shouldReturnEngineName() {
            assertThat(engine.getEngineName()).isEqualTo("Spark Execution Engine");
        }

        @Test
        @DisplayName("should return SPARK engine type")
        void shouldReturnEngineType() {
            assertThat(engine.getEngineType()).isEqualTo(EngineType.SPARK);
        }

        @Test
        @DisplayName("should return livy capabilities")
        void shouldReturnCapabilities() {
            EngineCapabilities capabilities = engine.getCapabilities();

            assertThat(capabilities).isNotNull();
        }
    }

    @Nested
    @DisplayName("isOperational()")
    class IsOperational {

        @Test
        @DisplayName("should return false after shutdown")
        void shouldReturnFalseAfterShutdown() {
            engine.shutdown();

            assertThat(engine.isOperational()).isFalse();
        }

        @Test
        @DisplayName("should return false when Livy is not reachable")
        void shouldReturnFalseWhenLivyNotReachable() {
            assertThat(engine.isOperational()).isFalse();
        }
    }

    @Nested
    @DisplayName("execute()")
    class Execute {

        @Test
        @DisplayName("should return failure when engine is shutdown")
        void shouldReturnFailureWhenShutdown() {
            engine.shutdown();

            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            assertThat(result.status()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(result.errorMessage()).isPresent();
            assertThat(result.errorMessage().get()).contains("shutdown");
        }

        @Test
        @DisplayName("should accept REWRITE_DATA_FILES operation")
        void shouldAcceptRewriteDataFiles() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .targetFileSizeBytes(256 * 1024 * 1024L)
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES);
        }

        @Test
        @DisplayName("should accept EXPIRE_SNAPSHOTS operation")
        void shouldAcceptExpireSnapshots() {
            ExpireSnapshotsOperation operation =
                    ExpireSnapshotsOperation.builder()
                            .maxSnapshotAge(Duration.ofDays(7))
                            .retainLast(5)
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS);
        }

        @Test
        @DisplayName("should accept ORPHAN_CLEANUP operation")
        void shouldAcceptOrphanCleanup() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder().olderThan(Duration.ofDays(3)).build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.ORPHAN_CLEANUP);
        }

        @Test
        @DisplayName("should accept REWRITE_MANIFESTS operation")
        void shouldAcceptRewriteManifests() {
            RewriteManifestsOperation operation = RewriteManifestsOperation.builder().build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_MANIFESTS);
        }

        @Test
        @DisplayName("should track execution in activeExecutions")
        void shouldTrackExecution() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);

            assertThat(future).isNotNull();
        }

        @Test
        @DisplayName("should return failure when Livy submission fails")
        void shouldReturnFailureWhenSubmissionFails() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            assertThat(result.status()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(result.table()).isEqualTo(tableId);
        }
    }

    @Nested
    @DisplayName("getStatus()")
    class GetStatus {

        @Test
        @DisplayName("should return empty for unknown execution")
        void shouldReturnEmptyForUnknownExecution() {
            Optional<ExecutionStatus> status = engine.getStatus("unknown-id");

            assertThat(status).isEmpty();
        }
    }

    @Nested
    @DisplayName("cancelExecution()")
    class CancelExecution {

        @Test
        @DisplayName("should return false for unknown execution")
        void shouldReturnFalseForUnknownExecution() {
            boolean cancelled = engine.cancelExecution("unknown-id");

            assertThat(cancelled).isFalse();
        }
    }

    @Nested
    @DisplayName("shutdown()")
    class Shutdown {

        @Test
        @DisplayName("should be idempotent")
        void shouldBeIdempotent() {
            engine.shutdown();
            engine.shutdown();

            assertThat(engine.isOperational()).isFalse();
        }

        @Test
        @DisplayName("should mark engine as not operational")
        void shouldMarkAsNotOperational() {
            assertThat(engine.isOperational()).isFalse();

            engine.shutdown();

            assertThat(engine.isOperational()).isFalse();
        }
    }

    @Nested
    @DisplayName("Configuration Properties")
    class ConfigurationProperties {

        @Test
        @DisplayName("should use configured Livy URL")
        void shouldUseConfiguredLivyUrl() {
            assertThat(config.livyUrl()).isEqualTo(LIVY_URL);
        }

        @Test
        @DisplayName("should use configured job JAR")
        void shouldUseConfiguredJobJar() {
            assertThat(config.maintenanceJobJar()).isEqualTo(JOB_JAR);
        }

        @Test
        @DisplayName("should use configured job class")
        void shouldUseConfiguredJobClass() {
            assertThat(config.maintenanceJobClass()).isEqualTo(JOB_CLASS);
        }

        @Test
        @DisplayName("should use configured catalog properties")
        void shouldUseConfiguredCatalogProperties() {
            assertThat(config.catalogProperties()).containsEntry("uri", "http://catalog:8181");
        }

        @Test
        @DisplayName("should use configured spark conf")
        void shouldUseConfiguredSparkConf() {
            assertThat(config.sparkConf()).containsEntry("spark.executor.instances", "2");
        }

        @Test
        @DisplayName("should use configured memory settings")
        void shouldUseConfiguredMemory() {
            assertThat(config.driverMemory()).isEqualTo("2g");
            assertThat(config.executorMemory()).isEqualTo("4g");
        }

        @Test
        @DisplayName("should use configured poll interval")
        void shouldUseConfiguredPollInterval() {
            assertThat(config.pollIntervalMs()).isEqualTo(100);
        }

        @Test
        @DisplayName("should use configured timeout")
        void shouldUseConfiguredTimeout() {
            assertThat(config.defaultTimeoutSeconds()).isEqualTo(60);
        }
    }

    @Nested
    @DisplayName("Batch Lifecycle")
    class BatchLifecycle {

        @Test
        @DisplayName("should handle batch submission error gracefully")
        void shouldHandleBatchSubmissionError() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            // Should fail gracefully when Livy is not available
            assertThat(result.status()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(result.errorMessage()).isPresent();
        }

        @Test
        @DisplayName("should include stack trace on error")
        void shouldIncludeStackTraceOnError() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            assertThat(result.status()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(result.errorStackTrace()).isPresent();
        }
    }

    @Nested
    @DisplayName("Operation Serialization")
    class OperationSerialization {

        @Test
        @DisplayName("should serialize RewriteDataFiles operation config")
        void shouldSerializeRewriteDataFilesConfig() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .targetFileSizeBytes(512 * 1024 * 1024L)
                            .maxConcurrentFileGroupRewrites(5)
                            .build();

            assertThat(operation.targetFileSizeBytes()).isPresent();
            assertThat(operation.targetFileSizeBytes().get()).isEqualTo(512 * 1024 * 1024L);
            assertThat(operation.maxConcurrentFileGroupRewrites()).isEqualTo(5);
        }

        @Test
        @DisplayName("should serialize RewriteDataFiles with filter")
        void shouldSerializeRewriteDataFilesWithFilter() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .targetFileSizeBytes(256 * 1024 * 1024L)
                            .filter("date >= '2024-01-01'")
                            .build();

            assertThat(operation.filter()).isPresent();
            assertThat(operation.filter().get()).isEqualTo("date >= '2024-01-01'");
        }

        @Test
        @DisplayName("should serialize RewriteDataFiles with strategy")
        void shouldSerializeRewriteDataFilesWithStrategy() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .strategy(RewriteDataFilesOperation.RewriteStrategy.SORT)
                            .build();

            assertThat(operation.strategy())
                    .isEqualTo(RewriteDataFilesOperation.RewriteStrategy.SORT);
        }

        @Test
        @DisplayName("should serialize RewriteDataFiles with partialProgress")
        void shouldSerializeRewriteDataFilesWithPartialProgress() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .partialProgressEnabled(true)
                            .partialProgressMaxCommits(5)
                            .build();

            assertThat(operation.partialProgressEnabled()).isTrue();
            assertThat(operation.partialProgressMaxCommits()).isEqualTo(5);
        }

        @Test
        @DisplayName("should serialize RewriteDataFiles with rewriteJobOrder")
        void shouldSerializeRewriteDataFilesWithRewriteJobOrder() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .rewriteJobOrder(RewriteDataFilesOperation.RewriteJobOrder.BYTES_DESC)
                            .build();

            assertThat(operation.rewriteJobOrder())
                    .isEqualTo(RewriteDataFilesOperation.RewriteJobOrder.BYTES_DESC);
        }

        @Test
        @DisplayName("should serialize RewriteDataFiles with removeDanglingDeletes")
        void shouldSerializeRewriteDataFilesWithRemoveDanglingDeletes() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder().removeDanglingDeletes(true).build();

            assertThat(operation.removeDanglingDeletes()).isTrue();
        }

        @Test
        @DisplayName("should serialize RewriteDataFiles with outputSpecId")
        void shouldSerializeRewriteDataFilesWithOutputSpecId() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder().outputSpecId(2).build();

            assertThat(operation.outputSpecId()).isPresent();
            assertThat(operation.outputSpecId().get()).isEqualTo(2);
        }

        @Test
        @DisplayName("should serialize ExpireSnapshots operation config")
        void shouldSerializeExpireSnapshotsConfig() {
            ExpireSnapshotsOperation operation =
                    ExpireSnapshotsOperation.builder()
                            .retainLast(10)
                            .maxSnapshotAge(Duration.ofDays(30))
                            .build();

            assertThat(operation.retainLast()).isEqualTo(10);
            assertThat(operation.maxSnapshotAge()).isPresent();
            assertThat(operation.maxSnapshotAge().get().toDays()).isEqualTo(30);
        }

        @Test
        @DisplayName("should serialize ExpireSnapshots with cleanExpiredMetadata")
        void shouldSerializeExpireSnapshotsWithCleanExpiredMetadata() {
            ExpireSnapshotsOperation operation =
                    ExpireSnapshotsOperation.builder()
                            .retainLast(5)
                            .maxSnapshotAge(Duration.ofDays(14))
                            .cleanExpiredMetadata(true)
                            .build();

            assertThat(operation.retainLast()).isEqualTo(5);
            assertThat(operation.maxSnapshotAge()).isPresent();
            assertThat(operation.maxSnapshotAge().get().toDays()).isEqualTo(14);
            assertThat(operation.cleanExpiredMetadata()).isTrue();
        }

        @Test
        @DisplayName("should serialize OrphanCleanup operation config")
        void shouldSerializeOrphanCleanupConfig() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder()
                            .olderThan(Duration.ofDays(7))
                            .location("s3://bucket/warehouse")
                            .build();

            assertThat(operation.olderThan()).isNotNull();
            assertThat(operation.olderThan().toDays()).isEqualTo(7);
            assertThat(operation.location()).isPresent();
            assertThat(operation.location().get()).isEqualTo("s3://bucket/warehouse");
        }

        @Test
        @DisplayName("should serialize OrphanCleanup with prefixMismatchMode")
        void shouldSerializeOrphanCleanupWithPrefixMismatchMode() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder()
                            .olderThan(Duration.ofDays(3))
                            .prefixMismatchMode(OrphanCleanupOperation.PrefixMismatchMode.DELETE)
                            .build();

            assertThat(operation.prefixMismatchMode())
                    .isEqualTo(OrphanCleanupOperation.PrefixMismatchMode.DELETE);
        }

        @Test
        @DisplayName("should serialize OrphanCleanup with equalSchemes")
        void shouldSerializeOrphanCleanupWithEqualSchemes() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder()
                            .olderThan(Duration.ofDays(3))
                            .equalScheme("s3a", "s3")
                            .build();

            assertThat(operation.equalSchemes()).containsEntry("s3a", "s3");
        }

        @Test
        @DisplayName("should serialize RewriteManifests operation config")
        void shouldSerializeRewriteManifestsConfig() {
            RewriteManifestsOperation operation = RewriteManifestsOperation.builder().build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_MANIFESTS);
        }

        @Test
        @DisplayName("should serialize RewriteManifests with specId")
        void shouldSerializeRewriteManifestsWithSpecId() {
            RewriteManifestsOperation operation =
                    RewriteManifestsOperation.builder().specId(1).build();

            assertThat(operation.specId()).isPresent();
            assertThat(operation.specId().get()).isEqualTo(1);
        }

        @Test
        @DisplayName("should serialize RewriteManifests with stagingLocation")
        void shouldSerializeRewriteManifestsWithStagingLocation() {
            RewriteManifestsOperation operation =
                    RewriteManifestsOperation.builder()
                            .stagingLocation("s3://bucket/staging")
                            .build();

            assertThat(operation.stagingLocation()).isPresent();
            assertThat(operation.stagingLocation().get()).isEqualTo("s3://bucket/staging");
        }
    }

    @Nested
    @DisplayName("Concurrent Executions")
    class ConcurrentExecutions {

        @Test
        @DisplayName("should handle multiple concurrent executions")
        void shouldHandleMultipleConcurrentExecutions() {
            RewriteDataFilesOperation operation1 = RewriteDataFilesOperation.builder().build();
            ExpireSnapshotsOperation operation2 = ExpireSnapshotsOperation.builder().build();

            ExecutionContext context1 =
                    ExecutionContext.builder(UUID.randomUUID().toString())
                            .timeoutSeconds(60)
                            .build();
            ExecutionContext context2 =
                    ExecutionContext.builder(UUID.randomUUID().toString())
                            .timeoutSeconds(60)
                            .build();

            CompletableFuture<ExecutionResult> future1 =
                    engine.execute(tableId, operation1, context1);
            CompletableFuture<ExecutionResult> future2 =
                    engine.execute(tableId, operation2, context2);

            ExecutionResult result1 = future1.join();
            ExecutionResult result2 = future2.join();

            assertThat(result1).isNotNull();
            assertThat(result2).isNotNull();
        }

        @Test
        @DisplayName("should track each execution independently")
        void shouldTrackExecutionsIndependently() {
            String execId1 = UUID.randomUUID().toString();
            String execId2 = UUID.randomUUID().toString();

            ExecutionContext context1 =
                    ExecutionContext.builder(execId1).timeoutSeconds(60).build();
            ExecutionContext context2 =
                    ExecutionContext.builder(execId2).timeoutSeconds(60).build();

            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            engine.execute(tableId, operation, context1);
            engine.execute(tableId, operation, context2);

            assertThat(execId1).isNotEqualTo(execId2);
        }
    }
}
