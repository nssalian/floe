package com.floe.core.metrics;

import com.floe.core.engine.ExecutionStatus;
import com.floe.core.maintenance.MaintenanceOperation;
import java.time.Duration;

/** Emits operation execution metrics to an external sink. */
public interface OperationMetricsEmitter {
    void recordOperationExecution(
            MaintenanceOperation.Type operationType, ExecutionStatus status, Duration duration);

    /** No-op implementation for environments without metrics wiring. */
    class NoOp implements OperationMetricsEmitter {
        @Override
        public void recordOperationExecution(
                MaintenanceOperation.Type operationType,
                ExecutionStatus status,
                Duration duration) {}
    }
}
