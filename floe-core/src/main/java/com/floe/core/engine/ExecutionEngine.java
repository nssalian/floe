package com.floe.core.engine;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.maintenance.MaintenanceOperation;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Execution engine for running Iceberg maintenance operations.
 *
 * <p>Implementations delegate to compute engines like Spark or Trino to execute compaction,
 * snapshot expiration, orphan cleanup, and manifest optimization.
 */
public interface ExecutionEngine {
    /** Get the type of this execution engine. */
    EngineType getEngineType();

    /** Human-readable name of the execution engine. */
    String getEngineName();

    /** Check if the engine is operational. */
    boolean isOperational();

    /** Shutdown the execution engine and release resources. */
    void shutdown();

    /** Get status information about the engine. */
    Optional<ExecutionStatus> getStatus(String executionId);

    /** Cancel a running execution. */
    boolean cancelExecution(String executionId);

    /** Get the capabilities of this execution engine. */
    EngineCapabilities getCapabilities();

    /** Execute a task on the engine. */
    CompletableFuture<ExecutionResult> execute(
            TableIdentifier table, MaintenanceOperation operation, ExecutionContext context);
}
