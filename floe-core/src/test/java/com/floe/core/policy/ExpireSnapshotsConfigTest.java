package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ExpireSnapshotsConfig")
class ExpireSnapshotsConfigTest {

    @Test
    void defaultsShouldHaveExpectedValues() {
        ExpireSnapshotsConfig config = ExpireSnapshotsConfig.defaults();

        assertEquals(5, config.retainLast());
        assertEquals(Duration.ofDays(7), config.maxSnapshotAge());
    }

    @Test
    void builderShouldCreateConfigWithAllFields() {
        ExpireSnapshotsConfig config =
                ExpireSnapshotsConfig.builder()
                        .retainLast(10)
                        .maxSnapshotAge(Duration.ofDays(14))
                        .build();

        assertEquals(10, config.retainLast());
        assertEquals(Duration.ofDays(14), config.maxSnapshotAge());
    }

    @Test
    void builderShouldUseDefaultValues() {
        ExpireSnapshotsConfig config = ExpireSnapshotsConfig.builder().build();

        assertEquals(5, config.retainLast());
        assertEquals(Duration.ofDays(7), config.maxSnapshotAge());
    }

    @Test
    void builderShouldSetExpireSnapshotId() {
        ExpireSnapshotsConfig config =
                ExpireSnapshotsConfig.builder()
                        .retainLast(10)
                        .maxSnapshotAge(Duration.ofDays(14))
                        .expireSnapshotId(123456789L)
                        .build();

        assertEquals(10, config.retainLast());
        assertEquals(Duration.ofDays(14), config.maxSnapshotAge());
        assertEquals(123456789L, config.expireSnapshotId());
    }

    @Test
    void recordShouldSupportEquality() {
        ExpireSnapshotsConfig config1 =
                ExpireSnapshotsConfig.builder()
                        .retainLast(5)
                        .maxSnapshotAge(Duration.ofDays(7))
                        .build();
        ExpireSnapshotsConfig config2 =
                ExpireSnapshotsConfig.builder()
                        .retainLast(5)
                        .maxSnapshotAge(Duration.ofDays(7))
                        .build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    void differentConfigsShouldNotBeEqual() {
        ExpireSnapshotsConfig config1 = ExpireSnapshotsConfig.builder().retainLast(5).build();
        ExpireSnapshotsConfig config2 = ExpireSnapshotsConfig.builder().retainLast(10).build();

        assertNotEquals(config1, config2);
    }

    @Test
    void shouldCreateWithRecordConstructor() {
        ExpireSnapshotsConfig config =
                new ExpireSnapshotsConfig(3, Duration.ofDays(30), true, null);

        assertEquals(3, config.retainLast());
        assertEquals(Duration.ofDays(30), config.maxSnapshotAge());
        assertTrue(config.cleanExpiredMetadata());
    }

    @Test
    @DisplayName("should create config with expireSnapshotId via record constructor")
    void shouldCreateWithExpireSnapshotIdViaConstructor() {
        ExpireSnapshotsConfig config =
                new ExpireSnapshotsConfig(5, Duration.ofDays(7), false, 9876543210L);

        assertEquals(5, config.retainLast());
        assertEquals(Duration.ofDays(7), config.maxSnapshotAge());
        assertFalse(config.cleanExpiredMetadata());
        assertEquals(9876543210L, config.expireSnapshotId());
    }

    @Test
    @DisplayName("defaults should have null expireSnapshotId")
    void defaultsShouldHaveNullExpireSnapshotId() {
        ExpireSnapshotsConfig config = ExpireSnapshotsConfig.defaults();

        assertNull(config.expireSnapshotId());
    }

    @Test
    @DisplayName("builder should allow setting all four fields")
    void builderShouldAllowSettingAllFourFields() {
        ExpireSnapshotsConfig config =
                ExpireSnapshotsConfig.builder()
                        .retainLast(3)
                        .maxSnapshotAge(Duration.ofDays(30))
                        .cleanExpiredMetadata(true)
                        .expireSnapshotId(111222333L)
                        .build();

        assertEquals(3, config.retainLast());
        assertEquals(Duration.ofDays(30), config.maxSnapshotAge());
        assertTrue(config.cleanExpiredMetadata());
        assertEquals(111222333L, config.expireSnapshotId());
    }

    @Test
    @DisplayName("configs with different expireSnapshotId should not be equal")
    void configsWithDifferentExpireSnapshotIdShouldNotBeEqual() {
        ExpireSnapshotsConfig config1 =
                ExpireSnapshotsConfig.builder().expireSnapshotId(123L).build();
        ExpireSnapshotsConfig config2 =
                ExpireSnapshotsConfig.builder().expireSnapshotId(456L).build();

        assertNotEquals(config1, config2);
    }

    @Test
    @DisplayName("configs with same expireSnapshotId should be equal")
    void configsWithSameExpireSnapshotIdShouldBeEqual() {
        ExpireSnapshotsConfig config1 =
                ExpireSnapshotsConfig.builder().retainLast(5).expireSnapshotId(999L).build();
        ExpireSnapshotsConfig config2 =
                ExpireSnapshotsConfig.builder().retainLast(5).expireSnapshotId(999L).build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    @DisplayName("should have meaningful toString with expireSnapshotId")
    void shouldHaveMeaningfulToStringWithExpireSnapshotId() {
        ExpireSnapshotsConfig config =
                ExpireSnapshotsConfig.builder().expireSnapshotId(123456789L).build();

        String toString = config.toString();

        assertTrue(toString.contains("ExpireSnapshotsConfig"));
        assertTrue(toString.contains("123456789"));
    }
}
