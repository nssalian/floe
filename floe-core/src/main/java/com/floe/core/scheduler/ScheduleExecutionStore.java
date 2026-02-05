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

package com.floe.core.scheduler;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Store for tracking when policies/operations were last executed. Used by the scheduler to
 * determine when operations are due.
 */
public interface ScheduleExecutionStore {

    /**
     * Get the execution record for a specific policy, operation, and table.
     *
     * @param policyId The policy identifier
     * @param operationType The operation type
     * @param tableKey The fully qualified table key
     * @return The execution record if exists
     */
    Optional<ScheduleExecutionRecord> getRecord(
            String policyId, String operationType, String tableKey);

    /**
     * Record that an operation was executed.
     *
     * @param policyId The policy identifier
     * @param operationType The operation type
     * @param tableKey The fully qualified table key
     * @param executedAt When the operation was triggered
     * @param nextRunAt When the operation should next run
     */
    void recordExecution(
            String policyId,
            String operationType,
            String tableKey,
            Instant executedAt,
            Instant nextRunAt);

    /**
     * Find all records that are due for execution.
     *
     * @param beforeTime Find records with nextRunAt before this time
     * @return List of due execution records
     */
    List<ScheduleExecutionRecord> findDueRecords(Instant beforeTime);

    /**
     * Find all records for a specific policy.
     *
     * @param policyId The policy identifier
     * @return List of execution records
     */
    List<ScheduleExecutionRecord> findByPolicy(String policyId);

    /**
     * Delete all records for a specific policy (e.g., when policy is deleted).
     *
     * @param policyId The policy identifier
     * @return Number of records deleted
     */
    int deleteByPolicy(String policyId);

    /**
     * Delete all records for a specific table.
     *
     * @param tableKey The fully qualified table key
     * @return Number of records deleted
     */
    int deleteByTable(String tableKey);

    /** Clear all records. For testing only. */
    void clear();

    /** Count total records. */
    long count();
}
