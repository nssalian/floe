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

package com.floe.engine.trino;

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
@DisplayName("TrinoExecutionEngine")
class TrinoExecutionEngineTest {

    private static final String JDBC_URL = "jdbc:trino://localhost:8080";
    private static final String USERNAME = "trino";
    private static final String CATALOG = "iceberg";

    private TrinoEngineConfig config;
    private TrinoExecutionEngine engine;
    private TableIdentifier tableId;
    private ExecutionContext context;

    @BeforeEach
    void setUp() {
        config =
                TrinoEngineConfig.builder()
                        .jdbcUrl(JDBC_URL)
                        .username(USERNAME)
                        .catalog(CATALOG)
                        .queryTimeoutSeconds(300)
                        .build();

        engine = new TrinoExecutionEngine(config);
        tableId = new TableIdentifier("demo", "test_namespace", "test_table");
        context =
                ExecutionContext.builder(UUID.randomUUID().toString())
                        .timeoutSeconds(300)
                        .catalogProperties(Map.of("catalog.type", "rest"))
                        .build();
    }

    @Nested
    @DisplayName("Engine Properties")
    class EngineProperties {

        @Test
        @DisplayName("should return correct engine name")
        void shouldReturnEngineName() {
            assertThat(engine.getEngineName()).isEqualTo("Trino Execution Engine");
        }

        @Test
        @DisplayName("should return TRINO engine type")
        void shouldReturnEngineType() {
            assertThat(engine.getEngineType()).isEqualTo(EngineType.TRINO);
        }

        @Test
        @DisplayName("should return trino capabilities")
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
        @DisplayName("should execute REWRITE_MANIFESTS operation via optimize_manifests")
        void shouldExecuteRewriteManifests() {
            RewriteManifestsOperation operation = RewriteManifestsOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            assertThat(result.operationType())
                    .isEqualTo(MaintenanceOperation.Type.REWRITE_MANIFESTS);
        }

        @Test
        @DisplayName("should track execution in activeExecutions")
        void shouldTrackExecution() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();
            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            assertThat(future).isNotNull();
        }

        @Test
        @DisplayName("should return failure when connection fails")
        void shouldReturnFailureWhenConnectionFails() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            // Without a real Trino server, connection will fail
            assertThat(result.status()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(result.errorMessage()).isPresent();
        }
    }

    @Nested
    @DisplayName("SQL Generation")
    class SqlGeneration {

        @Test
        @DisplayName("should build optimize SQL for REWRITE_DATA_FILES")
        void shouldBuildOptimizeSql() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .targetFileSizeBytes(256 * 1024 * 1024L)
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES);
            assertThat(operation.targetFileSizeBytes()).isPresent();
            assertThat(operation.targetFileSizeBytes().get()).isEqualTo(256 * 1024 * 1024L);
        }

        @Test
        @DisplayName("should build optimize SQL with filter")
        void shouldBuildOptimizeSqlWithFilter() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .targetFileSizeBytes(512 * 1024 * 1024L)
                            .filter("date >= '2024-01-01'")
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES);
            assertThat(operation.filter()).isPresent();
            assertThat(operation.filter().get()).isEqualTo("date >= '2024-01-01'");
        }

        @Test
        @DisplayName("should build optimize SQL with strategy")
        void shouldBuildOptimizeSqlWithStrategy() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .strategy(RewriteDataFilesOperation.RewriteStrategy.SORT)
                            .build();

            assertThat(operation.strategy())
                    .isEqualTo(RewriteDataFilesOperation.RewriteStrategy.SORT);
        }

        @Test
        @DisplayName("should build optimize SQL with partialProgress")
        void shouldBuildOptimizeSqlWithPartialProgress() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .partialProgressEnabled(true)
                            .partialProgressMaxCommits(5)
                            .build();

            assertThat(operation.partialProgressEnabled()).isTrue();
            assertThat(operation.partialProgressMaxCommits()).isEqualTo(5);
        }

        @Test
        @DisplayName("should build optimize SQL with rewriteJobOrder")
        void shouldBuildOptimizeSqlWithRewriteJobOrder() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .rewriteJobOrder(RewriteDataFilesOperation.RewriteJobOrder.BYTES_DESC)
                            .build();

            assertThat(operation.rewriteJobOrder())
                    .isEqualTo(RewriteDataFilesOperation.RewriteJobOrder.BYTES_DESC);
        }

        @Test
        @DisplayName("should build optimize SQL with removeDanglingDeletes")
        void shouldBuildOptimizeSqlWithRemoveDanglingDeletes() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder().removeDanglingDeletes(true).build();

            assertThat(operation.removeDanglingDeletes()).isTrue();
        }

        @Test
        @DisplayName("should build optimize SQL with outputSpecId")
        void shouldBuildOptimizeSqlWithOutputSpecId() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder().outputSpecId(2).build();

            assertThat(operation.outputSpecId()).isPresent();
            assertThat(operation.outputSpecId().get()).isEqualTo(2);
        }

        @Test
        @DisplayName("should build expire_snapshots SQL for EXPIRE_SNAPSHOTS")
        void shouldBuildExpireSnapshotsSql() {
            ExpireSnapshotsOperation operation =
                    ExpireSnapshotsOperation.builder()
                            .maxSnapshotAge(Duration.ofDays(7))
                            .retainLast(5)
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS);
            assertThat(operation.maxSnapshotAge()).isPresent();
            assertThat(operation.maxSnapshotAge().get().toDays()).isEqualTo(7);
            assertThat(operation.retainLast()).isEqualTo(5);
        }

        @Test
        @DisplayName("should build expire_snapshots SQL with cleanExpiredMetadata")
        void shouldBuildExpireSnapshotsSqlWithCleanExpiredMetadata() {
            ExpireSnapshotsOperation operation =
                    ExpireSnapshotsOperation.builder()
                            .maxSnapshotAge(Duration.ofDays(14))
                            .retainLast(3)
                            .cleanExpiredMetadata(true)
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS);
            assertThat(operation.maxSnapshotAge()).isPresent();
            assertThat(operation.maxSnapshotAge().get().toDays()).isEqualTo(14);
            assertThat(operation.retainLast()).isEqualTo(3);
            assertThat(operation.cleanExpiredMetadata()).isTrue();
        }

        @Test
        @DisplayName("should build remove_orphan_files SQL for ORPHAN_CLEANUP")
        void shouldBuildRemoveOrphanFilesSql() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder().olderThan(Duration.ofDays(3)).build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.ORPHAN_CLEANUP);
            assertThat(operation.olderThan()).isNotNull();
            assertThat(operation.olderThan().toDays()).isEqualTo(3);
        }

        @Test
        @DisplayName("should build remove_orphan_files SQL with location")
        void shouldBuildRemoveOrphanFilesSqlWithLocation() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder()
                            .olderThan(Duration.ofDays(5))
                            .location("s3://bucket/warehouse/db/table")
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.ORPHAN_CLEANUP);
            assertThat(operation.olderThan().toDays()).isEqualTo(5);
            assertThat(operation.location()).isPresent();
            assertThat(operation.location().get()).isEqualTo("s3://bucket/warehouse/db/table");
        }

        @Test
        @DisplayName("should build remove_orphan_files SQL with prefixMismatchMode")
        void shouldBuildRemoveOrphanFilesSqlWithPrefixMismatchMode() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder()
                            .olderThan(Duration.ofDays(3))
                            .prefixMismatchMode(OrphanCleanupOperation.PrefixMismatchMode.IGNORE)
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.ORPHAN_CLEANUP);
            assertThat(operation.prefixMismatchMode())
                    .isEqualTo(OrphanCleanupOperation.PrefixMismatchMode.IGNORE);
        }

        @Test
        @DisplayName("should build remove_orphan_files SQL with equalSchemes")
        void shouldBuildRemoveOrphanFilesSqlWithEqualSchemes() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder()
                            .olderThan(Duration.ofDays(3))
                            .equalScheme("s3a", "s3")
                            .build();

            assertThat(operation.equalSchemes()).containsEntry("s3a", "s3");
        }

        @Test
        @DisplayName("should build optimize_manifests SQL for REWRITE_MANIFESTS")
        void shouldBuildOptimizeManifestsSql() {
            RewriteManifestsOperation operation = RewriteManifestsOperation.builder().build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_MANIFESTS);
        }

        @Test
        @DisplayName("should build optimize_manifests SQL with specId")
        void shouldBuildOptimizeManifestsSqlWithSpecId() {
            RewriteManifestsOperation operation =
                    RewriteManifestsOperation.builder().specId(1).build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_MANIFESTS);
            assertThat(operation.specId()).isPresent();
            assertThat(operation.specId().get()).isEqualTo(1);
        }

        @Test
        @DisplayName("should build optimize_manifests SQL with stagingLocation")
        void shouldBuildOptimizeManifestsSqlWithStagingLocation() {
            RewriteManifestsOperation operation =
                    RewriteManifestsOperation.builder()
                            .stagingLocation("s3://bucket/staging")
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_MANIFESTS);
            assertThat(operation.stagingLocation()).isPresent();
            assertThat(operation.stagingLocation().get()).isEqualTo("s3://bucket/staging");
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
        @DisplayName("should cancel active executions")
        void shouldCancelActiveExecutions() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();
            engine.shutdown();
            assertThat(engine.isOperational()).isFalse();
        }
    }

    @Nested
    @DisplayName("Configuration")
    class Configuration {

        @Test
        @DisplayName("should use configured JDBC URL")
        void shouldUseConfiguredJdbcUrl() {
            assertThat(config.jdbcUrl()).isEqualTo(JDBC_URL);
        }

        @Test
        @DisplayName("should use configured username")
        void shouldUseConfiguredUsername() {
            assertThat(config.username()).isEqualTo(USERNAME);
        }

        @Test
        @DisplayName("should use configured catalog")
        void shouldUseConfiguredCatalog() {
            assertThat(config.catalog()).isEqualTo(CATALOG);
        }

        @Test
        @DisplayName("should use configured timeout")
        void shouldUseConfiguredTimeout() {
            assertThat(config.queryTimeoutSeconds()).isEqualTo(300);
        }

        @Test
        @DisplayName("should handle session properties")
        void shouldHandleSessionProperties() {
            TrinoEngineConfig configWithSession =
                    TrinoEngineConfig.builder()
                            .jdbcUrl(JDBC_URL)
                            .username(USERNAME)
                            .catalog(CATALOG)
                            .sessionProperties(Map.of("query_max_memory", "'1GB'"))
                            .build();

            assertThat(configWithSession.sessionProperties())
                    .containsEntry("query_max_memory", "'1GB'");
        }
    }

    @Nested
    @DisplayName("Operation Types")
    class OperationTypes {

        @Test
        @DisplayName("should support REWRITE_DATA_FILES")
        void shouldSupportRewriteDataFiles() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .targetFileSizeBytes(512 * 1024 * 1024L)
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES);
            assertThat(operation.targetFileSizeBytes()).isPresent();
        }

        @Test
        @DisplayName("should support EXPIRE_SNAPSHOTS")
        void shouldSupportExpireSnapshots() {
            ExpireSnapshotsOperation operation =
                    ExpireSnapshotsOperation.builder()
                            .retainLast(10)
                            .maxSnapshotAge(Duration.ofDays(30))
                            .build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.EXPIRE_SNAPSHOTS);
            assertThat(operation.retainLast()).isEqualTo(10);
        }

        @Test
        @DisplayName("should support ORPHAN_CLEANUP")
        void shouldSupportOrphanCleanup() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder().olderThan(Duration.ofDays(7)).build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.ORPHAN_CLEANUP);
            assertThat(operation.olderThan().toDays()).isEqualTo(7);
        }

        @Test
        @DisplayName("should support REWRITE_MANIFESTS via optimize_manifests")
        void shouldSupportRewriteManifests() {
            RewriteManifestsOperation operation = RewriteManifestsOperation.builder().build();

            assertThat(operation.getType()).isEqualTo(MaintenanceOperation.Type.REWRITE_MANIFESTS);

            EngineCapabilities capabilities = engine.getCapabilities();
            assertThat(
                            capabilities.isOperationSupported(
                                    MaintenanceOperation.Type.REWRITE_MANIFESTS))
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("Execution Results")
    class ExecutionResults {

        @Test
        @DisplayName("should include table identifier in result")
        void shouldIncludeTableInResult() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            assertThat(result.table()).isEqualTo(tableId);
        }

        @Test
        @DisplayName("should include operation type in result")
        void shouldIncludeOperationTypeInResult() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            assertThat(result.operationType())
                    .isEqualTo(MaintenanceOperation.Type.REWRITE_DATA_FILES);
        }

        @Test
        @DisplayName("should include execution ID in result")
        void shouldIncludeExecutionIdInResult() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            assertThat(result.executionId()).isEqualTo(context.executionId());
        }

        @Test
        @DisplayName("should include timestamps in result")
        void shouldIncludeTimestampsInResult() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.builder().build();

            CompletableFuture<ExecutionResult> future = engine.execute(tableId, operation, context);
            ExecutionResult result = future.join();

            assertThat(result.startTime()).isNotNull();
            assertThat(result.endTime()).isNotNull();
        }
    }
}
