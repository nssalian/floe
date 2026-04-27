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

package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ScheduleConfigTest {

    @Test
    void defaultsShouldHaveExpectedValues() {
        ScheduleConfig config = ScheduleConfig.defaults();

        assertEquals(Duration.ofDays(1), config.intervalInDays());
        assertNull(config.cronExpression());
        assertNull(config.windowStart());
        assertNull(config.windowEnd());
        assertNull(config.allowedDays());
        assertNull(config.timeoutInHours());
        assertNull(config.priority());
        assertTrue(config.enabled());
    }

    @Test
    void sixHourIntervalShouldHaveCorrectInterval() {
        ScheduleConfig config = ScheduleConfig.sixHourInterval();

        assertEquals(Duration.ofHours(6), config.intervalInDays());
        assertNull(config.cronExpression());
        assertTrue(config.enabled());
    }

    @Test
    void hourlyIntervalShouldHaveCorrectInterval() {
        ScheduleConfig config = ScheduleConfig.hourlyInterval();

        assertEquals(Duration.ofHours(1), config.intervalInDays());
        assertNull(config.cronExpression());
        assertTrue(config.enabled());
    }

    @Test
    void nightlyAt2amShouldHaveCronExpression() {
        ScheduleConfig config = ScheduleConfig.nightlyAt2am();

        assertNull(config.intervalInDays());
        assertEquals("0 2 * * *", config.cronExpression());
        assertEquals(Duration.ofHours(4), config.timeoutInHours());
        assertTrue(config.enabled());
    }

    @Test
    void weeklyOnSundaysAt3amShouldHaveCronExpression() {
        ScheduleConfig config = ScheduleConfig.weeklyOnSundaysAt3am();

        assertNull(config.intervalInDays());
        assertEquals("0 3 * * SUN", config.cronExpression());
        assertNull(config.timeoutInHours());
        assertTrue(config.enabled());
    }

    @Test
    void builderShouldCreateConfigWithAllFields() {
        ScheduleConfig config =
                ScheduleConfig.builder()
                        .interval(Duration.ofDays(7))
                        .cronExpression("0 2 * * *")
                        .windowStart(LocalTime.of(2, 0))
                        .windowEnd(LocalTime.of(6, 0))
                        .allowedDays(
                                Set.of(DayOfWeek.MONDAY, DayOfWeek.WEDNESDAY, DayOfWeek.FRIDAY))
                        .timeout(Duration.ofHours(2))
                        .priority(5)
                        .enabled(true)
                        .build();

        assertEquals(Duration.ofDays(7), config.intervalInDays());
        assertEquals("0 2 * * *", config.cronExpression());
        assertEquals(LocalTime.of(2, 0), config.windowStart());
        assertEquals(LocalTime.of(6, 0), config.windowEnd());
        assertTrue(config.allowedDays().contains(DayOfWeek.MONDAY));
        assertTrue(config.allowedDays().contains(DayOfWeek.WEDNESDAY));
        assertTrue(config.allowedDays().contains(DayOfWeek.FRIDAY));
        assertEquals(Duration.ofHours(2), config.timeoutInHours());
        assertEquals(5, config.priority());
        assertTrue(config.enabled());
    }

    @Test
    void isWithinWindowShouldReturnTrueWhenNoWindowSet() {
        ScheduleConfig config = ScheduleConfig.builder().build();

        assertTrue(config.isWithinWindow());
    }

    @Test
    void isAllowedDayShouldReturnTrueWhenNoAllowedDaysSet() {
        ScheduleConfig config = ScheduleConfig.builder().build();

        assertTrue(config.isAllowedDay());
    }

    @Test
    void isAllowedDayShouldReturnTrueWhenEmptyAllowedDays() {
        ScheduleConfig config = ScheduleConfig.builder().allowedDays(Set.of()).build();

        assertTrue(config.isAllowedDay());
    }

    @Test
    void isEnabledShouldReturnTrueWhenEnabled() {
        ScheduleConfig config = ScheduleConfig.builder().enabled(true).build();

        assertTrue(config.isEnabled());
    }

    @Test
    void isEnabledShouldReturnFalseWhenDisabled() {
        ScheduleConfig config = ScheduleConfig.builder().enabled(false).build();

        assertFalse(config.isEnabled());
    }

    @Test
    void isEnabledShouldReturnFalseWhenNull() {
        ScheduleConfig config = ScheduleConfig.builder().build();

        assertFalse(config.isEnabled());
    }

    @Test
    void canRunNowShouldCheckAllConditions() {
        ScheduleConfig config = ScheduleConfig.builder().enabled(true).build();

        // With no window and no allowed days restrictions, should be able to run
        assertTrue(config.canRunNow());
    }

    @Test
    void canRunNowShouldReturnFalseWhenDisabled() {
        ScheduleConfig config = ScheduleConfig.builder().enabled(false).build();

        assertFalse(config.canRunNow());
    }

    @Test
    void recordShouldSupportEquality() {
        ScheduleConfig config1 =
                ScheduleConfig.builder().interval(Duration.ofDays(1)).enabled(true).build();
        ScheduleConfig config2 =
                ScheduleConfig.builder().interval(Duration.ofDays(1)).enabled(true).build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    void differentConfigsShouldNotBeEqual() {
        ScheduleConfig config1 = ScheduleConfig.defaults();
        ScheduleConfig config2 = ScheduleConfig.nightlyAt2am();

        assertNotEquals(config1, config2);
    }

    @Test
    void shouldCreateWithRecordConstructor() {
        ScheduleConfig config =
                new ScheduleConfig(
                        Duration.ofDays(3),
                        null,
                        LocalTime.of(1, 0),
                        LocalTime.of(5, 0),
                        Set.of(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY),
                        Duration.ofHours(2),
                        10,
                        true);

        assertEquals(Duration.ofDays(3), config.intervalInDays());
        assertEquals(LocalTime.of(1, 0), config.windowStart());
        assertEquals(LocalTime.of(5, 0), config.windowEnd());
        assertTrue(config.allowedDays().contains(DayOfWeek.SATURDAY));
        assertTrue(config.allowedDays().contains(DayOfWeek.SUNDAY));
        assertEquals(Duration.ofHours(2), config.timeoutInHours());
        assertEquals(10, config.priority());
        assertTrue(config.enabled());
    }
}
