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

package com.floe.server.api.dto;

import com.floe.core.policy.ScheduleConfig;
import com.floe.server.api.validation.ValidCronExpression;
import java.time.DayOfWeek;
import java.time.Duration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public record ScheduleConfigDto(
        Integer interval,
        @ValidCronExpression String cronExpression,
        String windowStart, // HH:mm format
        String windowEnd, // HH:mm format
        String allowedDays, // Comma-separated days, e.g., "MONDAY,WEDNESDAY,FRIDAY"
        Integer timeout, // hours as Integer
        Integer priority,
        Boolean enabled) {
    public ScheduleConfig toConfig() {
        Duration intervalDuration = interval != null ? Duration.ofDays(interval) : null;
        Duration timeoutDuration = timeout != null ? Duration.ofHours(timeout) : null;
        return new ScheduleConfig(
                intervalDuration,
                cronExpression,
                windowStart != null ? java.time.LocalTime.parse(windowStart) : null,
                windowEnd != null ? java.time.LocalTime.parse(windowEnd) : null,
                allowedDays != null ? parseAllowedDays(allowedDays) : null,
                timeoutDuration,
                priority,
                enabled != null ? enabled : true);
    }

    private Set<DayOfWeek> parseAllowedDays(String daysStr) {
        Set<DayOfWeek> daysSet = new HashSet<>();
        String[] daysArray = daysStr.split(",");
        for (String day : daysArray) {
            daysSet.add(DayOfWeek.valueOf(day.trim().toUpperCase(Locale.ROOT)));
        }
        return daysSet;
    }
}
