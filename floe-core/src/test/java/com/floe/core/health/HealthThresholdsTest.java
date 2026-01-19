package com.floe.core.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HealthThresholdsTest {

    @Test
    void defaultsHasReasonableValues() {
        HealthThresholds defaults = HealthThresholds.defaults();

        assertEquals(100 * 1024 * 1024, defaults.smallFileSizeBytes());
        assertEquals(20.0, defaults.smallFilePercentWarning());
        assertEquals(50.0, defaults.smallFilePercentCritical());
        assertEquals(1024 * 1024 * 1024L, defaults.largeFileSizeBytes());
        assertEquals(10_000, defaults.fileCountWarning());
        assertEquals(50_000, defaults.fileCountCritical());
        assertEquals(100, defaults.snapshotCountWarning());
        assertEquals(500, defaults.snapshotCountCritical());
        assertEquals(7, defaults.snapshotAgeWarningDays());
        assertEquals(30, defaults.snapshotAgeCriticalDays());
    }

    @Test
    void strictHasTighterThresholds() {
        HealthThresholds strict = HealthThresholds.strict();
        HealthThresholds defaults = HealthThresholds.defaults();

        assertTrue(strict.smallFilePercentWarning() < defaults.smallFilePercentWarning());
        assertTrue(strict.smallFilePercentCritical() < defaults.smallFilePercentCritical());
        assertTrue(strict.snapshotCountWarning() < defaults.snapshotCountWarning());
        assertTrue(strict.snapshotAgeWarningDays() < defaults.snapshotAgeWarningDays());
    }

    @Test
    void relaxedHasLooseThresholds() {
        HealthThresholds relaxed = HealthThresholds.relaxed();
        HealthThresholds defaults = HealthThresholds.defaults();

        assertTrue(relaxed.smallFilePercentWarning() > defaults.smallFilePercentWarning());
        assertTrue(relaxed.smallFilePercentCritical() > defaults.smallFilePercentCritical());
        assertTrue(relaxed.snapshotCountWarning() > defaults.snapshotCountWarning());
        assertTrue(relaxed.snapshotAgeWarningDays() > defaults.snapshotAgeWarningDays());
    }

    @Test
    void builderWithDefaults() {
        HealthThresholds thresholds = HealthThresholds.builder().build();
        HealthThresholds defaults = HealthThresholds.defaults();

        assertEquals(defaults.smallFileSizeBytes(), thresholds.smallFileSizeBytes());
        assertEquals(defaults.smallFilePercentWarning(), thresholds.smallFilePercentWarning());
    }

    @Test
    void builderWithCustomValues() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .smallFileSizeBytes(50 * 1024 * 1024)
                        .smallFilePercentWarning(15.0)
                        .smallFilePercentCritical(30.0)
                        .snapshotCountWarning(50)
                        .snapshotCountCritical(100)
                        .build();

        assertEquals(50 * 1024 * 1024, thresholds.smallFileSizeBytes());
        assertEquals(15.0, thresholds.smallFilePercentWarning());
        assertEquals(30.0, thresholds.smallFilePercentCritical());
        assertEquals(50, thresholds.snapshotCountWarning());
        assertEquals(100, thresholds.snapshotCountCritical());
    }

    @Test
    void builderOverridesSingleValue() {
        HealthThresholds thresholds = HealthThresholds.builder().snapshotAgeWarningDays(14).build();

        // Overridden value
        assertEquals(14, thresholds.snapshotAgeWarningDays());

        // Other values remain default
        assertEquals(100 * 1024 * 1024, thresholds.smallFileSizeBytes());
        assertEquals(30, thresholds.snapshotAgeCriticalDays());
    }

    @Test
    void deleteFileThresholds() {
        HealthThresholds defaults = HealthThresholds.defaults();

        assertEquals(100, defaults.deleteFileCountWarning());
        assertEquals(500, defaults.deleteFileCountCritical());
    }

    @Test
    void manifestThresholds() {
        HealthThresholds defaults = HealthThresholds.defaults();

        assertEquals(100, defaults.manifestCountWarning());
        assertEquals(500, defaults.manifestCountCritical());
    }
}
