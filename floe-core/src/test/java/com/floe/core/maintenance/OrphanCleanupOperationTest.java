package com.floe.core.maintenance;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("OrphanCleanupOperation")
public class OrphanCleanupOperationTest {

    @Nested
    @DisplayName("defaults")
    class Defaults {

        @Test
        @DisplayName("should create with default values")
        void shouldCreateWithDefaultValues() {
            OrphanCleanupOperation operation = OrphanCleanupOperation.defaults();

            assert operation.olderThan().equals(java.time.Duration.ofDays(3));
            assert operation.location().isEmpty();
            assert operation.prefixMismatchMode()
                    == OrphanCleanupOperation.PrefixMismatchMode.ERROR;
            assert operation.equalSchemes().isEmpty();
            assert operation.equalAuthorities().isEmpty();
        }
    }

    @Nested
    @DisplayName("describe")
    class Describe {

        @Test
        @DisplayName("should describe operation")
        void shouldDescribeOperation() {
            OrphanCleanupOperation operation =
                    new OrphanCleanupOperation(
                            java.time.Duration.ofDays(5),
                            java.util.Optional.of("/data/orphans"),
                            OrphanCleanupOperation.PrefixMismatchMode.ERROR,
                            Map.of("s3", "s3a"),
                            Map.of("old-bucket", "new-bucket"));

            String description = operation.describe();
            assert description.equals(
                    "Orphan cleanup older than PT120H at location /data/orphans with prefix mismatch mode ERROR");
        }
    }

    @Nested
    @DisplayName("Builder")
    class Builder {

        @Test
        @DisplayName("should build with custom values")
        void shouldBuildWithCustomValues() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder()
                            .olderThan(java.time.Duration.ofDays(7))
                            .location("/data/orphans")
                            .prefixMismatchMode(OrphanCleanupOperation.PrefixMismatchMode.DELETE)
                            .equalSchemes(Map.of("s3", "s3a"))
                            .equalAuthorities(Map.of("old-bucket", "new-bucket"))
                            .build();

            assert operation.olderThan().equals(java.time.Duration.ofDays(7));
            assert operation.location().isPresent()
                    && operation.location().get().equals("/data/orphans");
            assert operation.prefixMismatchMode()
                    == OrphanCleanupOperation.PrefixMismatchMode.DELETE;
            assert operation.equalSchemes().size() == 1
                    && operation.equalSchemes().get("s3").equals("s3a");
            assert operation.equalAuthorities().size() == 1
                    && operation.equalAuthorities().get("old-bucket").equals("new-bucket");
        }

        @Test
        @DisplayName("should throw for non-positive olderThan")
        void shouldThrowForNonPositiveOlderThan() {
            try {
                OrphanCleanupOperation.builder().olderThan(java.time.Duration.ofDays(0)).build();
                assert false;
            } catch (IllegalArgumentException e) {
                assert e.getMessage().equals("olderThan must be positive");
            }
        }

        @Test
        @DisplayName("should allow empty optional values")
        void shouldAllowEmptyOptionalValues() {
            OrphanCleanupOperation operation = OrphanCleanupOperation.builder().build();

            assert operation.location().isEmpty();
            assert operation.equalSchemes().isEmpty();
            assert operation.equalAuthorities().isEmpty();
        }

        @Test
        @DisplayName("should add equal scheme")
        void shouldAddEqualScheme() {
            OrphanCleanupOperation operation =
                    OrphanCleanupOperation.builder().equalScheme("s3", "s3a").build();

            assert operation.equalSchemes().size() == 1;
            assert operation.equalSchemes().get("s3").equals("s3a");
        }
    }
}
