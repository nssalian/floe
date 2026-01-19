package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OrphanCleanupConfigTest {

    @Test
    void defaultsShouldHaveExpectedValues() {
        OrphanCleanupConfig config = OrphanCleanupConfig.defaults();

        assertEquals(Duration.ofDays(3), config.retentionPeriodInDays());
        assertNull(config.location());
        assertEquals("ERROR", config.prefixMismatchMode());
        assertEquals(Map.of(), config.equalSchemes());
        assertEquals(Map.of(), config.equalAuthorities());
    }

    @Test
    void conservativeShouldHaveSevenDayRetention() {
        OrphanCleanupConfig config = OrphanCleanupConfig.conservative();

        assertEquals(Duration.ofDays(7), config.retentionPeriodInDays());
        assertNull(config.location());
        assertEquals("ERROR", config.prefixMismatchMode());
        assertEquals(Map.of(), config.equalSchemes());
        assertEquals(Map.of(), config.equalAuthorities());
    }

    @Test
    void builderShouldCreateConfigWithAllFields() {
        OrphanCleanupConfig config =
                OrphanCleanupConfig.builder()
                        .retentionPeriodInDays(Duration.ofDays(14))
                        .location("/data/orphans")
                        .prefixMismatchMode("IGNORE")
                        .equalSchemes(Map.of("s3a", "s3"))
                        .equalAuthorities(Map.of("bucket.s3.amazonaws.com", "bucket"))
                        .build();

        assertEquals(Duration.ofDays(14), config.retentionPeriodInDays());
        assertEquals("/data/orphans", config.location());
        assertEquals("IGNORE", config.prefixMismatchMode());
        assertEquals(Map.of("s3a", "s3"), config.equalSchemes());
        assertEquals(Map.of("bucket.s3.amazonaws.com", "bucket"), config.equalAuthorities());
    }

    @Test
    void builderShouldUseDefaultValues() {
        OrphanCleanupConfig config = OrphanCleanupConfig.builder().build();

        assertNull(config.retentionPeriodInDays());
        assertNull(config.location());
        assertEquals("ERROR", config.prefixMismatchMode());
        assertEquals(Map.of(), config.equalSchemes());
        assertEquals(Map.of(), config.equalAuthorities());
    }

    @Test
    void calculateCutoffTimestampShouldReturnCorrectValue() {
        OrphanCleanupConfig config =
                OrphanCleanupConfig.builder().retentionPeriodInDays(Duration.ofDays(3)).build();

        long cutoff = config.calculateCutoffTimestamp();
        long expected = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

        // Allow 1 second tolerance for test execution time
        assertTrue(Math.abs(cutoff - expected) < 1000);
    }

    @Test
    void recordShouldSupportEquality() {
        OrphanCleanupConfig config1 =
                OrphanCleanupConfig.builder()
                        .retentionPeriodInDays(Duration.ofDays(7))
                        .prefixMismatchMode("ERROR")
                        .build();
        OrphanCleanupConfig config2 =
                OrphanCleanupConfig.builder()
                        .retentionPeriodInDays(Duration.ofDays(7))
                        .prefixMismatchMode("ERROR")
                        .build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    void differentConfigsShouldNotBeEqual() {
        OrphanCleanupConfig config1 = OrphanCleanupConfig.defaults();
        OrphanCleanupConfig config2 = OrphanCleanupConfig.conservative();

        assertNotEquals(config1, config2);
    }

    @Test
    void shouldCreateWithRecordConstructor() {
        OrphanCleanupConfig config =
                new OrphanCleanupConfig(
                        Duration.ofDays(5),
                        "/custom/path",
                        "DELETE",
                        Map.of("s3a", "s3"),
                        Map.of("host1", "host2"));

        assertEquals(Duration.ofDays(5), config.retentionPeriodInDays());
        assertEquals("/custom/path", config.location());
        assertEquals("DELETE", config.prefixMismatchMode());
        assertEquals(Map.of("s3a", "s3"), config.equalSchemes());
        assertEquals(Map.of("host1", "host2"), config.equalAuthorities());
    }
}
