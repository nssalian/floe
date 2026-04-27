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
    /**
     * Get the type of this execution engine.
     *
     * @return the engine type (e.g., SPARK, TRINO)
     */
    EngineType getEngineType();

    /**
     * Human-readable name of the execution engine.
     *
     * @return the engine name
     */
    String getEngineName();

    /**
     * Check if the engine is operational.
     *
     * @return true if the engine is ready to execute operations
     */
    boolean isOperational();

    /** Shutdown the execution engine and release resources. */
    void shutdown();

    /**
     * Get status information about the engine.
     *
     * @param executionId the execution ID to check
     * @return the execution status if found, empty otherwise
     */
    Optional<ExecutionStatus> getStatus(String executionId);

    /**
     * Cancel a running execution.
     *
     * @param executionId the execution ID to cancel
     * @return true if the execution was cancelled, false if not found or already complete
     */
    boolean cancelExecution(String executionId);

    /**
     * Get the capabilities of this execution engine.
     *
     * @return the engine capabilities
     */
    EngineCapabilities getCapabilities();

    /**
     * Execute a task on the engine.
     *
     * @param table the table to run maintenance on
     * @param operation the maintenance operation to execute
     * @param context the execution context with timeout and configuration
     * @return a future that completes with the execution result
     */
    CompletableFuture<ExecutionResult> execute(
            TableIdentifier table, MaintenanceOperation operation, ExecutionContext context);
}
