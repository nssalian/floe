package com.floe.core.operation;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Store for persisting and querying maintained operations. */
public interface OperationStore {
    /**
     * Create a new operation record.
     *
     * @param record the operation record to create
     * @return the created operation record with generated ID
     */
    OperationRecord createOperation(OperationRecord record);

    /**
     * Update the status of an existing operation.
     *
     * @param id the operation ID
     * @param status the new status
     * @param results the operation results, may be null
     */
    void updateStatus(UUID id, OperationStatus status, OperationResults results);

    /**
     * Mark an operation as failed with an error message.
     *
     * @param id the operation ID
     * @param errorMessage the error message describing the failure
     */
    void markFailed(UUID id, String errorMessage);

    /**
     * Mark an operation as running with a start time.
     *
     * @param id the operation ID
     * @return the updated operation record if found, empty otherwise
     */
    Optional<OperationRecord> markRunning(UUID id);

    /**
     * Update the policy info for an operation.
     *
     * @param id the operation ID
     * @param policyName the name of the policy
     * @param policyId the ID of the policy
     */
    void updatePolicyInfo(UUID id, String policyName, UUID policyId);

    /**
     * Find an operation by id.
     *
     * @param id the unique identifier of the operation
     * @return the operation record if found, empty otherwise
     */
    Optional<OperationRecord> findById(UUID id);

    /**
     * List operations for a given table, optionally filtered by status.
     *
     * @param catalog the catalog name
     * @param namespace the namespace name
     * @param tableName the table name
     * @param limit maximum number of records to return
     * @return list of operation records for the table
     */
    List<OperationRecord> findByTable(
            String catalog, String namespace, String tableName, int limit);

    /**
     * Find the most recent operations (no offset).
     *
     * @param limit maximum number of records to return
     * @return list of recent operation records
     */
    List<OperationRecord> findRecent(int limit);

    /**
     * Find the most recent operations with pagination.
     *
     * @param limit maximum number of records to return
     * @param offset number of records to skip
     * @return list of recent operation records
     */
    List<OperationRecord> findRecent(int limit, int offset);

    /**
     * Find operations by status (no offset).
     *
     * @param status the status to filter by
     * @param limit maximum number of records to return
     * @return list of operation records with the specified status
     */
    List<OperationRecord> findByStatus(OperationStatus status, int limit);

    /**
     * Find operations by status with pagination.
     *
     * @param status the status to filter by
     * @param limit maximum number of records to return
     * @param offset number of records to skip
     * @return list of operation records with the specified status
     */
    List<OperationRecord> findByStatus(OperationStatus status, int limit, int offset);

    /**
     * Count operations by status.
     *
     * @param status the status to count
     * @return number of operations with the specified status
     */
    long countByStatus(OperationStatus status);

    /**
     * Find operations within a time range.
     *
     * @param start the start of the time range (inclusive)
     * @param end the end of the time range (inclusive)
     * @param limit maximum number of records to return
     * @return list of operation records within the time range
     */
    List<OperationRecord> findInTimeRange(Instant start, Instant end, int limit);

    /**
     * Delete operations older than a given time.
     *
     * @param olderThan the age threshold for deletion
     * @return number of operations deleted
     */
    int deleteOlderThan(Duration olderThan);

    /**
     * Count total operations in the store.
     *
     * @return total number of operations
     */
    long count();

    /** Clear all operations. For testing only. */
    void clear();

    /**
     * Get aggregated statistics for operations within a time window.
     *
     * @param window the time window to aggregate over
     * @return aggregated operation statistics
     */
    OperationStats getStats(Duration window);

    /**
     * Get aggregated statistics for a specific table within a time window.
     *
     * @param catalog the catalog name
     * @param namespace the namespace name
     * @param tableName the table name
     * @param window the time window to aggregate over
     * @return aggregated operation statistics for the table
     */
    OperationStats getStatsForTable(
            String catalog, String namespace, String tableName, Duration window);
}
