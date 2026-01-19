package com.floe.core.operation;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Store for persisting and querying maintained operations. */
public interface OperationStore {
    /** Create a new operation record. */
    OperationRecord createOperation(OperationRecord record);

    /** Update the status of an existing operation. */
    void updateStatus(UUID id, OperationStatus status, OperationResults results);

    /** Mark an operation as failed with an error message. */
    void markFailed(UUID id, String errorMessage);

    /** Mark an operation as running with a start time. */
    Optional<OperationRecord> markRunning(UUID id);

    /** Update the policy info for an operation. */
    void updatePolicyInfo(UUID id, String policyName, UUID policyId);

    /** Find an operation by id */
    Optional<OperationRecord> findById(UUID id);

    /** List operations for a given table, optionally filtered by status. */
    List<OperationRecord> findByTable(
            String catalog, String namespace, String tableName, int limit);

    /** Find the most recent operations (no offset). */
    List<OperationRecord> findRecent(int limit);

    /** Find the most recent operations with pagination. */
    List<OperationRecord> findRecent(int limit, int offset);

    /** Find operations by status (no offset). */
    List<OperationRecord> findByStatus(OperationStatus status, int limit);

    /** Find operations by status with pagination. */
    List<OperationRecord> findByStatus(OperationStatus status, int limit, int offset);

    /** Count operations by status. */
    long countByStatus(OperationStatus status);

    /** Find operations within a time range. */
    List<OperationRecord> findInTimeRange(Instant start, Instant end, int limit);

    /** Delete operations older than a given time. */
    int deleteOlderThan(Duration olderThan);

    /** Count total operations in the store. */
    long count();

    /** Clear all operations. For testing only. */
    void clear();

    /** Get aggregated statistics for operations within a time window. */
    OperationStats getStats(Duration window);

    /** Get aggregated statistics for a specific table within a time window. */
    OperationStats getStatsForTable(
            String catalog, String namespace, String tableName, Duration window);
}
