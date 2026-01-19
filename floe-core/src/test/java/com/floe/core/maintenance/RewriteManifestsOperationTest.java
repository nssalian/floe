package com.floe.core.maintenance;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("RewriteManifestsOperation")
public class RewriteManifestsOperationTest {

    @Nested
    @DisplayName("defaults")
    class Defaults {

        @Test
        @DisplayName("should create with default values")
        void shouldCreateWithDefaultValues() {
            RewriteManifestsOperation operation = RewriteManifestsOperation.defaults();

            assert operation.specId().isEmpty();
            assert operation.stagingLocation().isEmpty();
            assert operation.sortBy().isEmpty();
        }
    }

    @Nested
    @DisplayName("describe")
    class Describe {

        @Test
        @DisplayName("should describe operation")
        void shouldDescribeOperation() {
            RewriteManifestsOperation operation =
                    new RewriteManifestsOperation(
                            java.util.Optional.of(2),
                            java.util.Optional.of("/data/staging"),
                            java.util.Optional.of(List.of("date", "region")),
                            java.util.Optional.empty());

            String description = operation.describe();
            assert description.contains("spec ID 2");
            assert description.contains("staging location /data/staging");
            assert description.contains("sort by [date, region]");
        }
    }

    @Nested
    @DisplayName("Builder")
    class Builder {

        @Test
        @DisplayName("should build with custom values")
        void shouldBuildWithCustomValues() {
            RewriteManifestsOperation operation =
                    RewriteManifestsOperation.builder()
                            .specId(1)
                            .stagingLocation("/tmp/staging")
                            .sortBy(List.of("date"))
                            .build();

            assert operation.specId().isPresent() && operation.specId().get() == 1;
            assert operation.stagingLocation().isPresent()
                    && operation.stagingLocation().get().equals("/tmp/staging");
            assert operation.sortBy().isPresent()
                    && operation.sortBy().get().equals(List.of("date"));
        }
    }

    @Nested
    @DisplayName("Empty Optional Values")
    class EmptyOptionalValues {

        @Test
        @DisplayName("should allow empty optional values")
        void shouldAllowEmptyOptionalValues() {
            RewriteManifestsOperation operation = RewriteManifestsOperation.builder().build();

            assert operation.specId().isEmpty();
            assert operation.stagingLocation().isEmpty();
            assert operation.sortBy().isEmpty();
        }
    }
}
