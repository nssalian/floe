package com.floe.core.maintenance;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("RewriteDataFilesOperation")
public class RewriteDataFilesOperationTest {

    @Nested
    @DisplayName("defaults")
    class Defaults {

        @Test
        @DisplayName("should create with default values")
        void shouldCreateWithDefaultValues() {
            RewriteDataFilesOperation operation = RewriteDataFilesOperation.defaults();

            assert operation.strategy() == RewriteDataFilesOperation.RewriteStrategy.BINPACK;
            assert operation.sortOrder().isEmpty();
            assert operation.zOrderColumns().isEmpty();
            assert operation.targetFileSizeBytes().isEmpty();
            assert operation.maxFileGroupSizeBytes().isEmpty();
            assert operation.maxConcurrentFileGroupRewrites() == 5;
            assert !operation.partialProgressEnabled();
            assert operation.partialProgressMaxCommits() == 10;
            assert operation.partialProgressMaxFailedCommits().isEmpty();
            assert operation.filter().isEmpty();
            assert operation.rewriteJobOrder() == RewriteDataFilesOperation.RewriteJobOrder.NONE;
            assert operation.useStartingSequenceNumber();
            assert !operation.removeDanglingDeletes();
            assert operation.outputSpecId().isEmpty();
        }
    }

    @Nested
    @DisplayName("describe")
    class Describe {

        @Test
        @DisplayName("should describe operation")
        void shouldDescribeOperation() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .strategy(RewriteDataFilesOperation.RewriteStrategy.SORT)
                            .sortOrder(java.util.List.of("col1", "col2"))
                            .targetFileSizeBytes(128L * 1024 * 1024)
                            .maxConcurrentFileGroupRewrites(4)
                            .partialProgressEnabled(true)
                            .partialProgressMaxCommits(20)
                            .filter("data_col > 50")
                            .rewriteJobOrder(RewriteDataFilesOperation.RewriteJobOrder.BYTES_DESC)
                            .useStartingSequenceNumber(true)
                            .outputSpecId(1)
                            .build();

            String description = operation.describe();
            assert description.contains("SORT strategy");
            assert description.contains("target file size");
            assert description.contains("partial progress enabled");
            assert description.contains("BYTES_DESC");
            assert description.contains("output spec ID 1");
        }
    }

    @Nested
    @DisplayName("Builder")
    class Builder {

        @Test
        @DisplayName("should build with custom values")
        void shouldBuildWithCustomValues() {
            RewriteDataFilesOperation operation =
                    RewriteDataFilesOperation.builder()
                            .strategy(RewriteDataFilesOperation.RewriteStrategy.ZORDER)
                            .sortOrder(java.util.List.of("col1", "col2"))
                            .zOrderColumns(java.util.List.of("col3"))
                            .targetFileSizeBytes(256L * 1024 * 1024)
                            .maxFileGroupSizeBytes(1024L * 1024 * 1024)
                            .maxConcurrentFileGroupRewrites(8)
                            .partialProgressEnabled(true)
                            .partialProgressMaxCommits(15)
                            .partialProgressMaxFailedCommits(5)
                            .filter("data_col > 100")
                            .rewriteJobOrder(RewriteDataFilesOperation.RewriteJobOrder.FILES_ASC)
                            .useStartingSequenceNumber(false)
                            .removeDanglingDeletes(true)
                            .outputSpecId(2)
                            .build();

            assert operation.strategy() == RewriteDataFilesOperation.RewriteStrategy.ZORDER;
            assert operation.sortOrder().isPresent() && operation.sortOrder().get().size() == 2;
            assert operation.zOrderColumns().isPresent()
                    && operation.zOrderColumns().get().size() == 1;
            assert operation.targetFileSizeBytes().isPresent()
                    && operation.targetFileSizeBytes().get() == 256L * 1024 * 1024;
            assert operation.maxFileGroupSizeBytes().isPresent()
                    && operation.maxFileGroupSizeBytes().get() == 1024L * 1024 * 1024;
            assert operation.maxConcurrentFileGroupRewrites() == 8;
            assert operation.partialProgressEnabled();
            assert operation.partialProgressMaxCommits() == 15;
            assert operation.partialProgressMaxFailedCommits().isPresent()
                    && operation.partialProgressMaxFailedCommits().get() == 5;
            assert operation.filter().isPresent()
                    && operation.filter().get().equals("data_col > 100");
            assert operation.rewriteJobOrder()
                    == RewriteDataFilesOperation.RewriteJobOrder.FILES_ASC;
            assert !operation.useStartingSequenceNumber();
            assert operation.removeDanglingDeletes();
            assert operation.outputSpecId().isPresent() && operation.outputSpecId().get() == 2;
        }
    }
}
