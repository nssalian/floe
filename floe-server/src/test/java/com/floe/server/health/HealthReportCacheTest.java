package com.floe.server.health;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthReport;
import com.floe.core.health.HealthThresholds;
import com.floe.core.health.TableHealthAssessor;
import com.floe.server.health.HealthReportCache.HealthCacheKey;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class HealthReportCacheTest {

    @Test
    void ttlExpiresEntries() throws InterruptedException {
        HealthReportCache cache = new HealthReportCache(Duration.ofMillis(50), 10);
        HealthCacheKey key =
                HealthCacheKey.of(
                        TableIdentifier.of("catalog", "db", "events"),
                        TableHealthAssessor.ScanMode.METADATA,
                        100,
                        HealthThresholds.defaults());
        cache.put(key, report());
        assertTrue(cache.getIfFresh(key).isPresent());
        Thread.sleep(100);
        assertFalse(cache.getIfFresh(key).isPresent());
    }

    @Test
    void lruEvictsOldest() {
        HealthReportCache cache = new HealthReportCache(Duration.ofMinutes(1), 2);
        HealthCacheKey key1 = key("events");
        HealthCacheKey key2 = key("users");
        HealthCacheKey key3 = key("orders");

        cache.put(key1, report());
        cache.put(key2, report());
        cache.getIfFresh(key1);
        cache.put(key3, report());

        assertTrue(cache.getIfFresh(key1).isPresent());
        assertFalse(cache.getIfFresh(key2).isPresent());
        assertTrue(cache.getIfFresh(key3).isPresent());
    }

    @Test
    void invalidateTableRemovesEntries() {
        HealthReportCache cache = new HealthReportCache(Duration.ofMinutes(1), 10);
        HealthCacheKey key = key("events");
        cache.put(key, report());
        cache.invalidateTable("catalog", "db", "events");
        assertFalse(cache.getIfFresh(key).isPresent());
    }

    private static HealthCacheKey key(String table) {
        return HealthCacheKey.of(
                TableIdentifier.of("catalog", "db", table),
                TableHealthAssessor.ScanMode.METADATA,
                100,
                HealthThresholds.defaults());
    }

    private static HealthReport report() {
        return HealthReport.builder(TableIdentifier.of("catalog", "db", "events"))
                .assessedAt(Instant.now())
                .dataFileCount(1)
                .build();
    }
}
