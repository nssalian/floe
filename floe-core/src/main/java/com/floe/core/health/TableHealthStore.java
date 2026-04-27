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

package com.floe.core.health;

import java.util.List;
import java.util.Optional;

/**
 * Store for persisting and retrieving table health reports.
 *
 * <p>Health reports are stored with a unique ID and can be queried by table identifier or retrieved
 * as the most recent reports across all tables.
 */
public interface TableHealthStore {
    /**
     * Save a health report.
     *
     * @param report the health report to save
     */
    void save(HealthReport report);

    /**
     * Find the latest health reports across all tables.
     *
     * @param limit maximum number of reports to return
     * @return list of the most recent health reports, ordered by assessment time descending
     */
    List<HealthReport> findLatest(int limit);

    /**
     * Find health report history for a specific table.
     *
     * @param catalog the catalog name
     * @param namespace the namespace name
     * @param tableName the table name
     * @param limit maximum number of reports to return
     * @return list of health reports for the table, ordered by assessment time descending
     */
    List<HealthReport> findHistory(String catalog, String namespace, String tableName, int limit);

    /**
     * Prune historical reports for a table, keeping only the most recent maxReports.
     *
     * @param catalog the catalog name
     * @param namespace the namespace name
     * @param tableName the table name
     * @param maxReports maximum reports to retain
     */
    void pruneHistory(String catalog, String namespace, String tableName, int maxReports);

    /**
     * Prune historical reports older than a given cutoff time.
     *
     * @param cutoffEpochMillis delete reports with assessed_at older than this epoch millis
     */
    void pruneOlderThan(long cutoffEpochMillis);

    /**
     * Find the most recent health report for a specific table.
     *
     * @param catalog the catalog name
     * @param namespace the namespace name
     * @param tableName the table name
     * @return the most recent health report if available
     */
    default Optional<HealthReport> findLatestForTable(
            String catalog, String namespace, String tableName) {
        List<HealthReport> history = findHistory(catalog, namespace, tableName, 1);
        return history.isEmpty() ? Optional.empty() : Optional.of(history.get(0));
    }

    /**
     * Count total stored health reports.
     *
     * @return the total count of reports
     */
    long count();

    /** Clear all stored reports. For testing only. */
    void clear();
}
