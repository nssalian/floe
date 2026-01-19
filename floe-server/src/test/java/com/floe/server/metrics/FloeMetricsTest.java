package com.floe.server.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import com.floe.core.operation.OperationStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.*;

/** Tests for {@link FloeMetrics}. */
@DisplayName("FloeMetrics")
class FloeMetricsTest {

    private MeterRegistry registry;
    private FloeMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new FloeMetrics(registry);
    }

    @Nested
    @DisplayName("Maintenance operation metrics")
    class MaintenanceOperationMetricsTests {

        @Test
        @DisplayName("recordMaintenanceTriggered increments counter")
        void recordMaintenanceTriggeredIncrementsCounter() {
            metrics.recordMaintenanceTriggered();
            metrics.recordMaintenanceTriggered();
            metrics.recordMaintenanceTriggered();

            Counter counter = registry.find("floe_maintenance_triggers_total").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(3.0);
        }

        @Test
        @DisplayName("recordMaintenanceResult increments success counter for SUCCESS status")
        void recordMaintenanceResultIncrementsSuccessForSuccess() {
            metrics.recordMaintenanceResult(OperationStatus.SUCCESS);
            metrics.recordMaintenanceResult(OperationStatus.SUCCESS);

            Counter counter = registry.find("floe_maintenance_success_total").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(2.0);
        }

        @Test
        @DisplayName("recordMaintenanceResult increments failure counter for FAILED status")
        void recordMaintenanceResultIncrementsFailureForFailed() {
            metrics.recordMaintenanceResult(OperationStatus.FAILED);

            Counter counter = registry.find("floe_maintenance_failures_total").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }

        @Test
        @DisplayName(
                "recordMaintenanceResult increments failure counter for PARTIAL_FAILURE status")
        void recordMaintenanceResultIncrementsFailureForPartialFailure() {
            metrics.recordMaintenanceResult(OperationStatus.PARTIAL_FAILURE);

            Counter counter = registry.find("floe_maintenance_failures_total").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("recordMaintenanceResult ignores non-terminal statuses")
        void recordMaintenanceResultIgnoresNonTerminalStatuses() {
            metrics.recordMaintenanceResult(OperationStatus.RUNNING);
            metrics.recordMaintenanceResult(OperationStatus.PENDING);

            Counter successCounter = registry.find("floe_maintenance_success_total").counter();
            Counter failureCounter = registry.find("floe_maintenance_failures_total").counter();

            // Counters exist but should be zero for these statuses
            Assertions.assertNotNull(successCounter);
            assertThat(successCounter.count()).isEqualTo(0.0);
            Assertions.assertNotNull(failureCounter);
            assertThat(failureCounter.count()).isEqualTo(0.0);
        }

        @Test
        @DisplayName("recordOperationExecution records counter and timer")
        void recordOperationExecutionRecordsCounterAndTimer() {
            metrics.recordOperationExecution(
                    MaintenanceOperation.Type.REWRITE_DATA_FILES,
                    ExecutionStatus.SUCCEEDED,
                    Duration.ofSeconds(30));

            Counter counter =
                    registry.find("floe_operation_executions_total")
                            .tag("operation", "rewrite_data_files")
                            .tag("status", "succeeded")
                            .counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);

            Timer timer =
                    registry.find("floe_operation_duration_seconds")
                            .tag("operation", "rewrite_data_files")
                            .timer();
            assertThat(timer).isNotNull();
            assertThat(timer.count()).isEqualTo(1);
            assertThat(timer.totalTime(java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(30.0);
        }

        @Test
        @DisplayName("recordOperationExecution handles different operation types")
        void recordOperationExecutionHandlesDifferentTypes() {
            metrics.recordOperationExecution(
                    MaintenanceOperation.Type.EXPIRE_SNAPSHOTS,
                    ExecutionStatus.SUCCEEDED,
                    Duration.ofSeconds(10));
            metrics.recordOperationExecution(
                    MaintenanceOperation.Type.ORPHAN_CLEANUP,
                    ExecutionStatus.FAILED,
                    Duration.ofSeconds(5));
            metrics.recordOperationExecution(
                    MaintenanceOperation.Type.REWRITE_MANIFESTS,
                    ExecutionStatus.CANCELLED,
                    Duration.ofSeconds(1));

            assertThat(
                            registry.find("floe_operation_executions_total")
                                    .tag("operation", "expire_snapshots")
                                    .counter())
                    .isNotNull();
            assertThat(
                            registry.find("floe_operation_executions_total")
                                    .tag("operation", "orphan_cleanup")
                                    .counter())
                    .isNotNull();
            assertThat(
                            registry.find("floe_operation_executions_total")
                                    .tag("operation", "rewrite_manifests")
                                    .counter())
                    .isNotNull();
        }

        @Test
        @DisplayName("timeOperation times and returns result")
        void timeOperationTimesAndReturnsResult() {
            String result =
                    metrics.timeOperation(
                            MaintenanceOperation.Type.REWRITE_DATA_FILES,
                            () -> {
                                try {
                                    Thread.sleep(50);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return "succeeded";
                            });

            assertThat(result).isEqualTo("succeeded");

            Timer timer =
                    registry.find("floe_operation_duration_seconds")
                            .tag("operation", "rewrite_data_files")
                            .timer();
            assertThat(timer).isNotNull();
            assertThat(timer.count()).isEqualTo(1);
            assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
                    .isGreaterThanOrEqualTo(50);
        }

        @Test
        @DisplayName("incrementRunningOperations and decrementRunningOperations update gauge")
        void runningOperationsGaugeUpdates() {
            metrics.incrementRunningOperations();
            metrics.incrementRunningOperations();

            Gauge gauge = registry.find("floe_running_operations").gauge();
            assertThat(gauge).isNotNull();
            assertThat(gauge.value()).isEqualTo(2.0);

            metrics.decrementRunningOperations();
            assertThat(gauge.value()).isEqualTo(1.0);
        }
    }

    @Nested
    @DisplayName("Policy metrics")
    class PolicyMetricsTests {

        @Test
        @DisplayName("recordPolicyCreated increments counter")
        void recordPolicyCreatedIncrementsCounter() {
            metrics.recordPolicyCreated();
            metrics.recordPolicyCreated();

            Counter counter = registry.find("floe_policies_created_total").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(2.0);
        }

        @Test
        @DisplayName("recordPolicyUpdated increments counter")
        void recordPolicyUpdatedIncrementsCounter() {
            metrics.recordPolicyUpdated();

            Counter counter = registry.find("floe_policies_updated_total").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("recordPolicyDeleted increments counter")
        void recordPolicyDeletedIncrementsCounter() {
            metrics.recordPolicyDeleted();
            metrics.recordPolicyDeleted();
            metrics.recordPolicyDeleted();

            Counter counter = registry.find("floe_policies_deleted_total").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(3.0);
        }

        @Test
        @DisplayName("recordPolicyMatch increments total and by-name counters")
        void recordPolicyMatchIncrementsCounters() {
            metrics.recordPolicyMatch("default-compaction");
            metrics.recordPolicyMatch("default-compaction");
            metrics.recordPolicyMatch("custom-policy");

            Counter totalCounter = registry.find("floe_policy_matches_total").counter();
            assertThat(totalCounter).isNotNull();
            assertThat(totalCounter.count()).isEqualTo(3.0);

            Counter byNameCounter =
                    registry.find("floe_policy_match_by_name_total")
                            .tag("policy", "default-compaction")
                            .counter();
            assertThat(byNameCounter).isNotNull();
            assertThat(byNameCounter.count()).isEqualTo(2.0);
        }

        @Test
        @DisplayName("recordNoMatchingPolicy increments counter")
        void recordNoMatchingPolicyIncrementsCounter() {
            metrics.recordNoMatchingPolicy();

            Counter counter = registry.find("floe_policy_no_match_total").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("setActivePoliciesCount updates gauge")
        void setActivePoliciesCountUpdatesGauge() {
            metrics.setActivePoliciesCount(10);

            Gauge gauge = registry.find("floe_active_policies").gauge();
            assertThat(gauge).isNotNull();
            assertThat(gauge.value()).isEqualTo(10.0);

            metrics.setActivePoliciesCount(5);
            assertThat(gauge.value()).isEqualTo(5.0);
        }
    }

    @Nested
    @DisplayName("Authentication metrics")
    class AuthenticationMetricsTests {

        @Test
        @DisplayName("recordAuthSuccess increments total and by-provider counters")
        void recordAuthSuccessIncrementsCounters() {
            metrics.recordAuthSuccess("api_key");
            metrics.recordAuthSuccess("api_key");
            metrics.recordAuthSuccess("oidc");

            Counter totalCounter = registry.find("floe_auth_success_total").counter();
            assertThat(totalCounter).isNotNull();
            assertThat(totalCounter.count()).isEqualTo(3.0);

            Counter byProviderCounter =
                    registry.find("floe_auth_by_provider_total")
                            .tag("provider", "api_key")
                            .tag("result", "success")
                            .counter();
            assertThat(byProviderCounter).isNotNull();
            assertThat(byProviderCounter.count()).isEqualTo(2.0);
        }

        @Test
        @DisplayName("recordAuthFailure increments total, by-provider, and by-reason counters")
        void recordAuthFailureIncrementsCounters() {
            metrics.recordAuthFailure("api_key", "invalid_key");
            metrics.recordAuthFailure("oidc", "token_expired");

            Counter totalCounter = registry.find("floe_auth_failure_total").counter();
            assertThat(totalCounter).isNotNull();
            assertThat(totalCounter.count()).isEqualTo(2.0);

            Counter byProviderCounter =
                    registry.find("floe_auth_by_provider_total")
                            .tag("provider", "api_key")
                            .tag("result", "failure")
                            .counter();
            assertThat(byProviderCounter).isNotNull();
            assertThat(byProviderCounter.count()).isEqualTo(1.0);

            Counter byReasonCounter =
                    registry.find("floe_auth_failure_reason_total")
                            .tag("reason", "invalid_key")
                            .counter();
            assertThat(byReasonCounter).isNotNull();
            assertThat(byReasonCounter.count()).isEqualTo(1.0);
        }
    }

    @Nested
    @DisplayName("API request metrics")
    class ApiRequestMetricsTests {

        @Test
        @DisplayName("recordApiRequest records timer and counter")
        void recordApiRequestRecordsTimerAndCounter() {
            metrics.recordApiRequest("/api/v1/policies", "GET", 200, Duration.ofMillis(50));
            metrics.recordApiRequest("/api/v1/policies", "POST", 201, Duration.ofMillis(100));
            metrics.recordApiRequest("/api/v1/policies", "GET", 500, Duration.ofMillis(200));

            // Check counters
            Counter successCounter =
                    registry.find("floe_api_requests_total")
                            .tag("endpoint", "/api/v1/policies")
                            .tag("method", "GET")
                            .tag("status_code", "200")
                            .counter();
            assertThat(successCounter).isNotNull();
            assertThat(successCounter.count()).isEqualTo(1.0);

            // Check timer
            Timer timer =
                    registry.find("floe_api_request_duration_seconds")
                            .tag("endpoint", "/api/v1/policies")
                            .tag("method", "GET")
                            .tag("status", "success")
                            .timer();
            assertThat(timer).isNotNull();
            assertThat(timer.count()).isEqualTo(1);
        }

        @Test
        @DisplayName("recordApiRequest categorizes status codes correctly")
        void recordApiRequestCategorizesStatusCodes() {
            // Success statuses (2xx, 3xx)
            metrics.recordApiRequest("/test", "GET", 200, Duration.ofMillis(10));
            metrics.recordApiRequest("/test", "GET", 201, Duration.ofMillis(10));
            metrics.recordApiRequest("/test", "GET", 302, Duration.ofMillis(10));

            // Error statuses (4xx, 5xx)
            metrics.recordApiRequest("/test", "GET", 400, Duration.ofMillis(10));
            metrics.recordApiRequest("/test", "GET", 401, Duration.ofMillis(10));
            metrics.recordApiRequest("/test", "GET", 500, Duration.ofMillis(10));

            Timer successTimer =
                    registry.find("floe_api_request_duration_seconds")
                            .tag("status", "success")
                            .timer();
            assertThat(successTimer).isNotNull();
            assertThat(successTimer.count()).isEqualTo(3);

            Timer errorTimer =
                    registry.find("floe_api_request_duration_seconds")
                            .tag("status", "error")
                            .timer();
            assertThat(errorTimer).isNotNull();
            assertThat(errorTimer.count()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Utility methods")
    class UtilityMethodsTests {

        @Test
        @DisplayName("getRegistry returns the underlying registry")
        void getRegistryReturnsUnderlyingRegistry() {
            assertThat(metrics.getRegistry()).isSameAs(registry);
        }
    }

    @Nested
    @DisplayName("Metric naming conventions")
    class MetricNamingConventionsTests {

        @Test
        @DisplayName("all metrics have floe_ prefix")
        void allMetricsHaveFloePrefix() {
            // Trigger various metrics
            metrics.recordMaintenanceTriggered();
            metrics.recordPolicyCreated();
            metrics.recordAuthSuccess("api_key");

            registry.getMeters()
                    .forEach(
                            meter -> {
                                assertThat(meter.getId().getName())
                                        .as(
                                                "Metric %s should have floe_ prefix",
                                                meter.getId().getName())
                                        .startsWith("floe_");
                            });
        }

        @Test
        @DisplayName("counters end with _total suffix")
        void countersEndWithTotalSuffix() {
            metrics.recordMaintenanceTriggered();
            metrics.recordPolicyCreated();

            registry.find("floe_maintenance_triggers_total").counter();
            registry.find("floe_policies_created_total").counter();

            // These should exist
            assertThat(registry.find("floe_maintenance_triggers_total").counter()).isNotNull();
            assertThat(registry.find("floe_policies_created_total").counter()).isNotNull();
        }

        @Test
        @DisplayName("timers end with _seconds suffix")
        void timersEndWithSecondsSuffix() {
            metrics.recordOperationExecution(
                    MaintenanceOperation.Type.REWRITE_DATA_FILES,
                    ExecutionStatus.SUCCEEDED,
                    Duration.ofSeconds(1));

            Timer timer = registry.find("floe_operation_duration_seconds").timer();
            assertThat(timer).isNotNull();
        }
    }
}
