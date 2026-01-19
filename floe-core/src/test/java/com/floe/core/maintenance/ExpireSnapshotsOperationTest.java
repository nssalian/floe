package com.floe.core.maintenance;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ExpireSnapshotsOperation")
public class ExpireSnapshotsOperationTest {

    @Nested
    @DisplayName("defaults")
    class Defaults {

        @Test
        @DisplayName("should create with default values")
        void shouldCreateWithDefaultValues() {
            ExpireSnapshotsOperation operation = ExpireSnapshotsOperation.defaults();

            assert operation.retainLast() == 5;
            assert operation.maxSnapshotAge().isPresent()
                    && operation.maxSnapshotAge().get().equals(java.time.Duration.ofDays(7));
        }
    }

    @Nested
    @DisplayName("describe")
    class Describe {

        @Test
        @DisplayName("should describe operation")
        void shouldDescribeOperation() {
            ExpireSnapshotsOperation operation =
                    new ExpireSnapshotsOperation(
                            3, java.util.Optional.empty(), false, java.util.Optional.empty());

            String description = operation.describe();
            assert description.equals("Expire snapshots, retain last 3");
        }

        @Test
        @DisplayName("should describe operation with max age")
        void shouldDescribeOperationWithMaxAge() {
            ExpireSnapshotsOperation operation =
                    new ExpireSnapshotsOperation(
                            5,
                            java.util.Optional.of(java.time.Duration.ofDays(14)),
                            false,
                            java.util.Optional.empty());

            String description = operation.describe();
            assert description.contains("retain last 5");
            assert description.contains("max age");
        }
    }

    @Nested
    @DisplayName("Builder")
    class Builder {

        @Test
        @DisplayName("should build with custom values")
        void shouldBuildWithCustomValues() {
            ExpireSnapshotsOperation operation =
                    ExpireSnapshotsOperation.builder()
                            .retainLast(10)
                            .maxSnapshotAge(java.time.Duration.ofDays(14))
                            .build();

            assert operation.retainLast() == 10;
            assert operation.maxSnapshotAge().isPresent()
                    && operation.maxSnapshotAge().get().equals(java.time.Duration.ofDays(14));
        }

        @Test
        @DisplayName("should throw for invalid retainLast")
        void shouldThrowForInvalidRetainLast() {
            try {
                ExpireSnapshotsOperation.builder().retainLast(0).build();
                assert false;
            } catch (IllegalArgumentException e) {
                assert e.getMessage().contains("retainLast must be at least 1");
            }
        }
    }
}
