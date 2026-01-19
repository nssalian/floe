package com.floe.core.scheduler;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory implementation of ScheduleExecutionStore. */
public class InMemoryScheduleExecutionStore implements ScheduleExecutionStore {

    private final Map<String, ScheduleExecutionRecord> records = new ConcurrentHashMap<>();

    @Override
    public Optional<ScheduleExecutionRecord> getRecord(
            String policyId, String operationType, String tableKey) {
        String key = compositeKey(policyId, operationType, tableKey);
        return Optional.ofNullable(records.get(key));
    }

    @Override
    public void recordExecution(
            String policyId,
            String operationType,
            String tableKey,
            Instant executedAt,
            Instant nextRunAt) {
        String key = compositeKey(policyId, operationType, tableKey);
        ScheduleExecutionRecord record =
                ScheduleExecutionRecord.builder()
                        .policyId(policyId)
                        .operationType(operationType)
                        .tableKey(tableKey)
                        .lastRunAt(executedAt)
                        .nextRunAt(nextRunAt)
                        .build();
        records.put(key, record);
    }

    @Override
    public List<ScheduleExecutionRecord> findDueRecords(Instant beforeTime) {
        return records.values().stream().filter(record -> record.isDue(beforeTime)).toList();
    }

    @Override
    public List<ScheduleExecutionRecord> findByPolicy(String policyId) {
        return records.values().stream()
                .filter(record -> record.policyId().equals(policyId))
                .toList();
    }

    @Override
    public int deleteByPolicy(String policyId) {
        List<String> keysToRemove =
                records.entrySet().stream()
                        .filter(e -> e.getValue().policyId().equals(policyId))
                        .map(Map.Entry::getKey)
                        .toList();
        keysToRemove.forEach(records::remove);
        return keysToRemove.size();
    }

    @Override
    public int deleteByTable(String tableKey) {
        List<String> keysToRemove =
                records.entrySet().stream()
                        .filter(e -> e.getValue().tableKey().equals(tableKey))
                        .map(Map.Entry::getKey)
                        .toList();
        keysToRemove.forEach(records::remove);
        return keysToRemove.size();
    }

    @Override
    public void clear() {
        records.clear();
    }

    @Override
    public long count() {
        return records.size();
    }

    private String compositeKey(String policyId, String operationType, String tableKey) {
        return policyId + ":" + operationType + ":" + tableKey;
    }
}
