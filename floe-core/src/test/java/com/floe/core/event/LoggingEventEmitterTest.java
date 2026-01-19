package com.floe.core.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.floe.core.catalog.TableIdentifier;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("LoggingEventEmitter")
class LoggingEventEmitterTest {

    @Nested
    @DisplayName("when enabled")
    class WhenEnabled {

        private final LoggingEventEmitter emitter = new LoggingEventEmitter(true);

        @Test
        @DisplayName("should report as enabled")
        void shouldReportAsEnabled() {
            assertThat(emitter.isEnabled()).isTrue();
        }

        @Test
        @DisplayName("should emit operation started event without error")
        void shouldEmitOperationStarted() {
            var event =
                    MaintenanceEvent.operationStarted(
                            "op-123",
                            "catalog",
                            TableIdentifier.of("catalog", "db", "table"),
                            "COMPACT");

            assertThatCode(() -> emitter.emit(event)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should emit operation completed event without error")
        void shouldEmitOperationCompleted() {
            var event =
                    MaintenanceEvent.operationCompleted(
                            "op-123",
                            "catalog",
                            TableIdentifier.of("catalog", "db", "table"),
                            "COMPACT",
                            Map.of("filesCompacted", 10));

            assertThatCode(() -> emitter.emit(event)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should emit operation failed event without error")
        void shouldEmitOperationFailed() {
            var event =
                    MaintenanceEvent.operationFailed(
                            "op-123",
                            "catalog",
                            TableIdentifier.of("catalog", "db", "table"),
                            "COMPACT",
                            "Connection timeout");

            assertThatCode(() -> emitter.emit(event)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should emit policy created event without error")
        void shouldEmitPolicyCreated() {
            var event = MaintenanceEvent.policyCreated("policy-123", Map.of("name", "test-policy"));

            assertThatCode(() -> emitter.emit(event)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should emit policy updated event without error")
        void shouldEmitPolicyUpdated() {
            var event = MaintenanceEvent.policyUpdated("policy-123", Map.of("name", "test-policy"));

            assertThatCode(() -> emitter.emit(event)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should emit policy deleted event without error")
        void shouldEmitPolicyDeleted() {
            var event = MaintenanceEvent.policyDeleted("policy-123");

            assertThatCode(() -> emitter.emit(event)).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("when disabled")
    class WhenDisabled {

        private final LoggingEventEmitter emitter = new LoggingEventEmitter(false);

        @Test
        @DisplayName("should report as disabled")
        void shouldReportAsDisabled() {
            assertThat(emitter.isEnabled()).isFalse();
        }

        @Test
        @DisplayName("should not throw when emitting events")
        void shouldNotThrowWhenEmitting() {
            var event =
                    MaintenanceEvent.operationStarted(
                            "op-123",
                            "catalog",
                            TableIdentifier.of("catalog", "db", "table"),
                            "COMPACT");

            assertThatCode(() -> emitter.emit(event)).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("default constructor")
    class DefaultConstructor {

        @Test
        @DisplayName("should be enabled by default")
        void shouldBeEnabledByDefault() {
            var emitter = new LoggingEventEmitter();
            assertThat(emitter.isEnabled()).isTrue();
        }
    }
}
