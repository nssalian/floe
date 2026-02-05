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

package com.floe.server.health;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthReport;
import com.floe.core.health.HealthThresholds;
import com.floe.core.health.TableHealthAssessor;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Singleton
public class HealthReportCache {

    private static final Duration DEFAULT_TTL = Duration.ofMinutes(5);
    private static final int DEFAULT_MAX_ENTRIES = 500;

    private final Duration ttl;
    private final int maxEntries;

    private final Map<HealthCacheKey, CacheEntry> entries = new LinkedHashMap<>(16, 0.75f, true);

    public HealthReportCache() {
        this(DEFAULT_TTL, DEFAULT_MAX_ENTRIES);
    }

    HealthReportCache(Duration ttl, int maxEntries) {
        this.ttl = ttl;
        this.maxEntries = maxEntries;
    }

    public Optional<HealthReport> getIfFresh(HealthCacheKey key) {
        synchronized (entries) {
            CacheEntry entry = entries.get(key);
            if (entry == null) {
                return Optional.empty();
            }
            if (isExpired(entry)) {
                entries.remove(key);
                return Optional.empty();
            }
            return Optional.of(entry.report);
        }
    }

    public void put(HealthCacheKey key, HealthReport report) {
        synchronized (entries) {
            entries.put(key, new CacheEntry(report, Instant.now()));
            pruneExpired();
            pruneLRU();
        }
    }

    public void invalidateTable(String catalog, String namespace, String table) {
        synchronized (entries) {
            Iterator<Map.Entry<HealthCacheKey, CacheEntry>> iterator =
                    entries.entrySet().iterator();
            while (iterator.hasNext()) {
                HealthCacheKey key = iterator.next().getKey();
                if (key.matchesTable(catalog, namespace, table)) {
                    iterator.remove();
                }
            }
        }
    }

    private void pruneExpired() {
        Iterator<Map.Entry<HealthCacheKey, CacheEntry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<HealthCacheKey, CacheEntry> entry = iterator.next();
            if (isExpired(entry.getValue())) {
                iterator.remove();
            }
        }
    }

    private void pruneLRU() {
        while (entries.size() > maxEntries) {
            Iterator<HealthCacheKey> iterator = entries.keySet().iterator();
            if (!iterator.hasNext()) {
                break;
            }
            iterator.next();
            iterator.remove();
        }
    }

    private boolean isExpired(CacheEntry entry) {
        return entry.createdAt.plus(ttl).isBefore(Instant.now());
    }

    public record HealthCacheKey(
            String catalog,
            String namespace,
            String table,
            String scanMode,
            int sampleLimit,
            int thresholdsHash) {
        public static HealthCacheKey of(
                TableIdentifier id,
                TableHealthAssessor.ScanMode scanMode,
                int sampleLimit,
                HealthThresholds thresholds) {
            int hash = thresholds != null ? thresholds.hashCode() : 0;
            return new HealthCacheKey(
                    id.catalog(), id.namespace(), id.table(), scanMode.name(), sampleLimit, hash);
        }

        public boolean matchesTable(String catalog, String namespace, String table) {
            return (this.catalog.equals(catalog)
                    && this.namespace.equals(namespace)
                    && this.table.equals(table));
        }
    }

    private record CacheEntry(HealthReport report, Instant createdAt) {}
}
