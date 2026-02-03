package com.floe.core.operation;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** In-memory implementation of OperationStore for testing and simple deployments. */
public class InMemoryOperationStore implements OperationStore {

    private final ConcurrentHashMap<UUID, OperationRecord> records = new ConcurrentHashMap<>();

    @Override
    public OperationRecord createOperation(OperationRecord record) {
        OperationRecord toStore = record;

        // Generate a new UUID if not provided
        if (toStore.id() == null) {
            toStore = toStore.toBuilder().id(UUID.randomUUID()).build();
        }
        // Ensure createdAt is set
        if (toStore.createdAt() == null) {
            toStore = toStore.toBuilder().createdAt(Instant.now()).build();
        }
        records.put(toStore.id(), toStore);
        return toStore;
    }

    @Override
    public void updateStatus(UUID id, OperationStatus status, OperationResults results) {
        records.computeIfPresent(
                id,
                (key, existing) -> {
                    OperationRecord.Builder builder = existing.toBuilder().status(status);

                    if (results != null) {
                        builder.results(results);
                        builder.normalizedMetrics(results.aggregatedMetrics());
                    }

                    if (status.isTerminal()) {
                        builder.completedAt(Instant.now());
                    }

                    return builder.build();
                });
    }

    @Override
    public void markFailed(UUID id, String errorMessage) {
        records.computeIfPresent(
                id,
                (key, existing) ->
                        existing.toBuilder()
                                .status(OperationStatus.FAILED)
                                .errorMessage(errorMessage)
                                .completedAt(Instant.now())
                                .build());
    }

    @Override
    public Optional<OperationRecord> markRunning(UUID id) {
        return Optional.ofNullable(
                records.computeIfPresent(
                        id,
                        (key, existing) ->
                                existing.toBuilder()
                                        .status(OperationStatus.RUNNING)
                                        .startedAt(Instant.now())
                                        .build()));
    }

    @Override
    public void updatePolicyInfo(UUID id, String policyName, UUID policyId, String policyVersion) {
        records.computeIfPresent(
                id,
                (key, existing) ->
                        existing.toBuilder()
                                .policyName(policyName)
                                .policyId(policyId)
                                .policyVersion(policyVersion)
                                .build());
    }

    @Override
    public Optional<OperationRecord> findById(UUID id) {
        return Optional.ofNullable(records.get(id));
    }

    @Override
    public List<OperationRecord> findByTable(
            String catalog, String namespace, String tableName, int limit) {
        return records.values().stream()
                .filter(
                        r ->
                                r.catalog().equals(catalog)
                                        && r.namespace().equals(namespace)
                                        && r.tableName().equals(tableName))
                .filter(r -> r.startedAt() != null)
                .sorted(Comparator.comparing(OperationRecord::startedAt).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<OperationRecord> findRecent(int limit) {
        return records.values().stream()
                .filter(r -> r.startedAt() != null)
                .sorted(Comparator.comparing(OperationRecord::startedAt).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<OperationRecord> findRecent(int limit, int offset) {
        return records.values().stream()
                .filter(r -> r.startedAt() != null)
                .sorted(Comparator.comparing(OperationRecord::startedAt).reversed())
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<OperationRecord> findByStatus(OperationStatus status, int limit) {
        return records.values().stream()
                .filter(r -> r.status() == status)
                .filter(r -> r.startedAt() != null)
                .sorted(Comparator.comparing(OperationRecord::startedAt).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<OperationRecord> findByStatus(OperationStatus status, int limit, int offset) {
        return records.values().stream()
                .filter(r -> r.status() == status)
                .filter(r -> r.startedAt() != null)
                .sorted(Comparator.comparing(OperationRecord::startedAt).reversed())
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public long countByStatus(OperationStatus status) {
        return records.values().stream().filter(r -> r.status() == status).count();
    }

    @Override
    public List<OperationRecord> findInTimeRange(Instant start, Instant end, int limit) {
        return records.values().stream()
                .filter(r -> r.startedAt() != null)
                .filter(r -> !r.startedAt().isBefore(start) && r.startedAt().isBefore(end))
                .sorted(Comparator.comparing(OperationRecord::startedAt).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public long count() {
        return records.size();
    }

    @Override
    public void clear() {
        records.clear();
    }

    @Override
    public int deleteOlderThan(Duration olderThan) {
        Instant cutoff = Instant.now().minus(olderThan);
        int deleted = 0;
        Iterator<Map.Entry<UUID, OperationRecord>> iterator = records.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<UUID, OperationRecord> entry = iterator.next();
            if (entry.getValue().createdAt().isBefore(cutoff)) {
                iterator.remove();
                deleted++;
            }
        }
        return deleted;
    }

    @Override
    public OperationStats getStats(Duration window) {
        Instant windowStart = Instant.now().minus(window);
        Instant windowEnd = Instant.now();

        List<OperationRecord> inWindow =
                records.values().stream()
                        .filter(r -> r.startedAt() != null)
                        .filter(r -> !r.startedAt().isBefore(windowStart))
                        .toList();

        return buildStats(inWindow, windowStart, windowEnd);
    }

    @Override
    public OperationStats getStatsForTable(
            String catalog, String namespace, String tableName, Duration window) {
        Instant windowStart = Instant.now().minus(window);
        Instant windowEnd = Instant.now();

        List<OperationRecord> inWindow =
                records.values().stream()
                        .filter(
                                r ->
                                        r.catalog().equals(catalog)
                                                && r.namespace().equals(namespace)
                                                && r.tableName().equals(tableName))
                        .filter(r -> r.startedAt() != null)
                        .filter(r -> !r.startedAt().isBefore(windowStart))
                        .toList();

        return buildStats(inWindow, windowStart, windowEnd);
    }

    @Override
    public Optional<Instant> findLastOperationTime(
            String catalog, String namespace, String tableName, String operationType) {
        return records.values().stream()
                .filter(
                        r ->
                                r.catalog().equals(catalog)
                                        && r.namespace().equals(namespace)
                                        && r.tableName().equals(tableName))
                .filter(r -> r.results() != null && r.results().operations() != null)
                .filter(
                        r ->
                                r.results().operations().stream()
                                        .anyMatch(
                                                op ->
                                                        op.operationType() != null
                                                                && op.operationType()
                                                                        .name()
                                                                        .equals(operationType)))
                .filter(r -> r.startedAt() != null)
                .sorted(Comparator.comparing(OperationRecord::startedAt).reversed())
                .findFirst()
                .map(r -> r.completedAt() != null ? r.completedAt() : r.startedAt());
    }

    private OperationStats buildStats(
            List<OperationRecord> records, Instant windowStart, Instant windowEnd) {
        Map<OperationStatus, Long> countsByStatus =
                records.stream()
                        .collect(
                                Collectors.groupingBy(
                                        OperationRecord::status, Collectors.counting()));

        return OperationStats.builder()
                .totalOperations(records.size())
                .successCount(countsByStatus.getOrDefault(OperationStatus.SUCCESS, 0L))
                .failedCount(countsByStatus.getOrDefault(OperationStatus.FAILED, 0L))
                .partialFailureCount(
                        countsByStatus.getOrDefault(OperationStatus.PARTIAL_FAILURE, 0L))
                .runningCount(
                        countsByStatus.getOrDefault(OperationStatus.RUNNING, 0L)
                                + countsByStatus.getOrDefault(OperationStatus.PENDING, 0L))
                .noPolicyCount(countsByStatus.getOrDefault(OperationStatus.NO_POLICY, 0L))
                .noOperationsCount(countsByStatus.getOrDefault(OperationStatus.NO_OPERATIONS, 0L))
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .zeroChangeRuns(0)
                .consecutiveFailures(0)
                .consecutiveZeroChangeRuns(0)
                .averageBytesRewritten(0)
                .averageFilesRewritten(0)
                .lastRunAt(null)
                .build();
    }
}
