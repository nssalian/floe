package com.floe.core.health;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * In-memory implementation of TableHealthStore for testing and simple deployments.
 *
 * <p>This implementation stores all reports in memory and is not suitable for production use with
 * large numbers of reports or across server restarts.
 */
public class InMemoryTableHealthStore implements TableHealthStore {

    private final CopyOnWriteArrayList<HealthReport> reports = new CopyOnWriteArrayList<>();

    @Override
    public void save(HealthReport report) {
        reports.add(report);
    }

    @Override
    public List<HealthReport> findLatest(int limit) {
        return reports.stream()
                .sorted(Comparator.comparing(HealthReport::assessedAt).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<HealthReport> findHistory(
            String catalog, String namespace, String tableName, int limit) {
        return reports.stream()
                .filter(
                        r ->
                                r.tableIdentifier().catalog().equals(catalog)
                                        && r.tableIdentifier().namespace().equals(namespace)
                                        && r.tableIdentifier().table().equals(tableName))
                .sorted(Comparator.comparing(HealthReport::assessedAt).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public void pruneHistory(String catalog, String namespace, String tableName, int maxReports) {
        if (maxReports <= 0) {
            return;
        }

        List<HealthReport> history = findHistory(catalog, namespace, tableName, Integer.MAX_VALUE);
        if (history.size() <= maxReports) {
            return;
        }

        List<HealthReport> keep = history.subList(0, maxReports);
        reports.removeIf(
                report ->
                        report.tableIdentifier().catalog().equals(catalog)
                                && report.tableIdentifier().namespace().equals(namespace)
                                && report.tableIdentifier().table().equals(tableName)
                                && !keep.contains(report));
    }

    @Override
    public void pruneOlderThan(long cutoffEpochMillis) {
        reports.removeIf(report -> report.assessedAt().toEpochMilli() < cutoffEpochMillis);
    }

    @Override
    public long count() {
        return reports.size();
    }

    @Override
    public void clear() {
        reports.clear();
    }
}
