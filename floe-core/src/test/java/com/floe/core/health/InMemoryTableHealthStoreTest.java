package com.floe.core.health;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.catalog.TableIdentifier;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryTableHealthStoreTest {

    private static final TableIdentifier TABLE_1 = new TableIdentifier("catalog", "db", "table1");
    private static final TableIdentifier TABLE_2 = new TableIdentifier("catalog", "db", "table2");

    private InMemoryTableHealthStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryTableHealthStore();
    }

    @Test
    void saveAndFindLatest() {
        HealthReport report = HealthReport.builder(TABLE_1).dataFileCount(100).build();

        store.save(report);
        List<HealthReport> latest = store.findLatest(10);

        assertEquals(1, latest.size());
        assertEquals(TABLE_1, latest.get(0).tableIdentifier());
    }

    @Test
    void saveMultipleTables() {
        HealthReport report1 = HealthReport.builder(TABLE_1).dataFileCount(100).build();
        HealthReport report2 = HealthReport.builder(TABLE_2).dataFileCount(200).build();

        store.save(report1);
        store.save(report2);

        List<HealthReport> latest = store.findLatest(10);

        assertEquals(2, latest.size());
    }

    @Test
    void findHistorySingleTable() {
        HealthReport report1 =
                HealthReport.builder(TABLE_1)
                        .assessedAt(Instant.now().minus(2, ChronoUnit.HOURS))
                        .dataFileCount(100)
                        .build();
        HealthReport report2 =
                HealthReport.builder(TABLE_1)
                        .assessedAt(Instant.now().minus(1, ChronoUnit.HOURS))
                        .dataFileCount(150)
                        .build();
        HealthReport report3 =
                HealthReport.builder(TABLE_2).assessedAt(Instant.now()).dataFileCount(200).build();

        store.save(report1);
        store.save(report2);
        store.save(report3);

        List<HealthReport> table1History =
                store.findHistory(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table(), 10);

        assertEquals(2, table1History.size());
        // Should only contain table1 reports
        assertTrue(table1History.stream().allMatch(r -> r.tableIdentifier().equals(TABLE_1)));
    }

    @Test
    void findHistoryOrderedByTime() {
        Instant older = Instant.now().minus(2, ChronoUnit.HOURS);
        Instant newer = Instant.now().minus(1, ChronoUnit.HOURS);

        HealthReport olderReport =
                HealthReport.builder(TABLE_1).assessedAt(older).dataFileCount(100).build();
        HealthReport newerReport =
                HealthReport.builder(TABLE_1).assessedAt(newer).dataFileCount(150).build();

        // Save in opposite order to test sorting
        store.save(olderReport);
        store.save(newerReport);

        List<HealthReport> history =
                store.findHistory(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table(), 10);

        assertEquals(2, history.size());
        // Most recent first
        assertTrue(history.get(0).assessedAt().isAfter(history.get(1).assessedAt()));
    }

    @Test
    void findHistoryLimitRespected() {
        for (int i = 0; i < 20; i++) {
            HealthReport report =
                    HealthReport.builder(TABLE_1)
                            .assessedAt(Instant.now().minus(i, ChronoUnit.HOURS))
                            .dataFileCount(100 + i)
                            .build();
            store.save(report);
        }

        List<HealthReport> history =
                store.findHistory(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table(), 5);

        assertEquals(5, history.size());
    }

    @Test
    void findLatestLimitRespected() {
        for (int i = 0; i < 30; i++) {
            TableIdentifier tableId = new TableIdentifier("catalog", "db", "table" + i);
            HealthReport report =
                    HealthReport.builder(tableId)
                            .assessedAt(Instant.now().minus(i, ChronoUnit.HOURS))
                            .dataFileCount(100 + i)
                            .build();
            store.save(report);
        }

        List<HealthReport> latest = store.findLatest(10);

        assertEquals(10, latest.size());
    }

    @Test
    void countReturnsCorrectValue() {
        assertEquals(0, store.count());

        store.save(HealthReport.builder(TABLE_1).build());
        assertEquals(1, store.count());

        store.save(HealthReport.builder(TABLE_2).build());
        assertEquals(2, store.count());
    }

    @Test
    void clearRemovesAllReports() {
        store.save(HealthReport.builder(TABLE_1).build());
        store.save(HealthReport.builder(TABLE_2).build());
        assertEquals(2, store.count());

        store.clear();

        assertEquals(0, store.count());
        assertTrue(store.findLatest(10).isEmpty());
    }

    @Test
    void findLatestForTable() {
        Instant older = Instant.now().minus(2, ChronoUnit.HOURS);
        Instant newer = Instant.now().minus(1, ChronoUnit.HOURS);

        store.save(HealthReport.builder(TABLE_1).assessedAt(older).dataFileCount(100).build());
        store.save(HealthReport.builder(TABLE_1).assessedAt(newer).dataFileCount(150).build());

        var latest =
                store.findLatestForTable(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table());

        assertTrue(latest.isPresent());
        assertEquals(150, latest.get().dataFileCount());
    }

    @Test
    void findLatestForTableEmpty() {
        var latest =
                store.findLatestForTable(TABLE_1.catalog(), TABLE_1.namespace(), TABLE_1.table());

        assertTrue(latest.isEmpty());
    }
}
