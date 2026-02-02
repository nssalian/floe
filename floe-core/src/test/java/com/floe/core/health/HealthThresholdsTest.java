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

    // Phase 1: New threshold field tests

    @Test
    void defaultsContainsAllNewFields() {
        HealthThresholds defaults = HealthThresholds.defaults();

        // Delete file ratio
        assertEquals(0.10, defaults.deleteFileRatioWarning());
        assertEquals(0.25, defaults.deleteFileRatioCritical());

        // Manifest size
        assertEquals(100 * 1024 * 1024L, defaults.manifestSizeWarningBytes());
        assertEquals(500 * 1024 * 1024L, defaults.manifestSizeCriticalBytes());

        // Partition count
        assertEquals(5_000, defaults.partitionCountWarning());
        assertEquals(10_000, defaults.partitionCountCritical());

        // Partition skew
        assertEquals(3.0, defaults.partitionSkewWarning());
        assertEquals(10.0, defaults.partitionSkewCritical());

        // Stale metadata
        assertEquals(7, defaults.staleMetadataWarningDays());
        assertEquals(30, defaults.staleMetadataCriticalDays());
    }

    @Test
    void strictHasLowerNewThresholds() {
        HealthThresholds strict = HealthThresholds.strict();
        HealthThresholds defaults = HealthThresholds.defaults();

        assertTrue(strict.deleteFileRatioWarning() < defaults.deleteFileRatioWarning());
        assertTrue(strict.deleteFileRatioCritical() < defaults.deleteFileRatioCritical());
        assertTrue(strict.manifestSizeWarningBytes() < defaults.manifestSizeWarningBytes());
        assertTrue(strict.partitionCountWarning() < defaults.partitionCountWarning());
        assertTrue(strict.partitionSkewWarning() < defaults.partitionSkewWarning());
        assertTrue(strict.staleMetadataWarningDays() < defaults.staleMetadataWarningDays());
    }

    @Test
    void relaxedHasHigherNewThresholds() {
        HealthThresholds relaxed = HealthThresholds.relaxed();
        HealthThresholds defaults = HealthThresholds.defaults();

        assertTrue(relaxed.deleteFileRatioWarning() > defaults.deleteFileRatioWarning());
        assertTrue(relaxed.deleteFileRatioCritical() > defaults.deleteFileRatioCritical());
        assertTrue(relaxed.manifestSizeWarningBytes() > defaults.manifestSizeWarningBytes());
        assertTrue(relaxed.partitionCountWarning() > defaults.partitionCountWarning());
        assertTrue(relaxed.partitionSkewWarning() > defaults.partitionSkewWarning());
        assertTrue(relaxed.staleMetadataWarningDays() > defaults.staleMetadataWarningDays());
    }

    @Test
    void builderSupportsAllNewFields() {
        HealthThresholds thresholds =
                HealthThresholds.builder()
                        .deleteFileRatioWarning(0.15)
                        .deleteFileRatioCritical(0.30)
                        .manifestSizeWarningBytes(150 * 1024 * 1024L)
                        .manifestSizeCriticalBytes(600 * 1024 * 1024L)
                        .partitionCountWarning(6_000)
                        .partitionCountCritical(12_000)
                        .partitionSkewWarning(4.0)
                        .partitionSkewCritical(12.0)
                        .staleMetadataWarningDays(10)
                        .staleMetadataCriticalDays(45)
                        .build();

        assertEquals(0.15, thresholds.deleteFileRatioWarning());
        assertEquals(0.30, thresholds.deleteFileRatioCritical());
        assertEquals(150 * 1024 * 1024L, thresholds.manifestSizeWarningBytes());
        assertEquals(600 * 1024 * 1024L, thresholds.manifestSizeCriticalBytes());
        assertEquals(6_000, thresholds.partitionCountWarning());
        assertEquals(12_000, thresholds.partitionCountCritical());
        assertEquals(4.0, thresholds.partitionSkewWarning());
        assertEquals(12.0, thresholds.partitionSkewCritical());
        assertEquals(10, thresholds.staleMetadataWarningDays());
        assertEquals(45, thresholds.staleMetadataCriticalDays());
    }

    @Test
    void builderUsesDefaultValuesForNewFields() {
        HealthThresholds thresholds = HealthThresholds.builder().build();
        HealthThresholds defaults = HealthThresholds.defaults();

        assertEquals(defaults.deleteFileRatioWarning(), thresholds.deleteFileRatioWarning());
        assertEquals(defaults.deleteFileRatioCritical(), thresholds.deleteFileRatioCritical());
        assertEquals(defaults.manifestSizeWarningBytes(), thresholds.manifestSizeWarningBytes());
        assertEquals(defaults.manifestSizeCriticalBytes(), thresholds.manifestSizeCriticalBytes());
        assertEquals(defaults.partitionCountWarning(), thresholds.partitionCountWarning());
        assertEquals(defaults.partitionCountCritical(), thresholds.partitionCountCritical());
        assertEquals(defaults.partitionSkewWarning(), thresholds.partitionSkewWarning());
        assertEquals(defaults.partitionSkewCritical(), thresholds.partitionSkewCritical());
        assertEquals(defaults.staleMetadataWarningDays(), thresholds.staleMetadataWarningDays());
        assertEquals(defaults.staleMetadataCriticalDays(), thresholds.staleMetadataCriticalDays());
    }
}
