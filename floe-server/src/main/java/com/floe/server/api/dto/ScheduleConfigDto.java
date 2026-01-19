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
