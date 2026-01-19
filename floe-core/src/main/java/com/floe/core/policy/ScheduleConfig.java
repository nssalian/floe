package com.floe.core.policy;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Set;

/**
 * Configuration for scheduling maintenance operations.
 *
 * @param intervalInDays interval between runs
 * @param cronExpression cron expression (alternative to interval)
 * @param windowStart start of allowed time window
 * @param windowEnd end of allowed time window
 * @param allowedDays days of week when operation can run
 * @param timeoutInHours operation timeout
 * @param priority execution priority
 * @param enabled whether schedule is active
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ScheduleConfig(
        Duration intervalInDays,
        String cronExpression,
        LocalTime windowStart,
        LocalTime windowEnd,
        Set<DayOfWeek> allowedDays,
        Duration timeoutInHours,
        Integer priority,
        Boolean enabled) {
    /** Default schedule: daily interval, no time window, no timeout, enabled. */
    public static ScheduleConfig defaults() {
        return new ScheduleConfig(
                Duration.ofDays(1), // intervalInDays
                null, // cronExpression
                null, // windowStart
                null, // windowEnd
                null, // allowedDays
                null, // timeoutInHours
                null, // priority
                true // enabled
                );
    }

    /** Default every 6 hours, no time restrictions, enabled. */
    public static ScheduleConfig sixHourInterval() {
        return new ScheduleConfig(
                Duration.ofHours(6), // intervalInDays
                null, // cronExpression
                null, // windowStart
                null, // windowEnd
                null, // allowedDays
                null, // timeoutInHours
                null, // priority
                true // enabled
                );
    }

    /** Default hourly schedule: every hour, no time restrictions, enabled. */
    public static ScheduleConfig hourlyInterval() {
        return new ScheduleConfig(
                Duration.ofHours(1), // intervalInDays
                null, // cronExpression
                null, // windowStart
                null, // windowEnd
                null, // allowedDays
                null, // timeoutInHours
                null, // priority
                true // enabled
                );
    }

    /** Default nightly schedule: daily at 2am, 4-hour timeout, enabled. */
    public static ScheduleConfig nightlyAt2am() {
        return new ScheduleConfig(
                null, // intervalInDays
                "0 2 * * *", // cronExpression
                null, // windowStart
                null, // windowEnd
                null, // allowedDays
                Duration.ofHours(4), // timeoutInHours
                null, // priority
                true // enabled
                );
    }

    /** Default weekly schedule for low-priority tasks: every Sunday at 3am, no timeout, enabled. */
    public static ScheduleConfig weeklyOnSundaysAt3am() {
        return new ScheduleConfig(
                null, // intervalInDays
                "0 3 * * SUN", // cronExpression
                null, // windowStart
                null, // windowEnd
                null, // allowedDays
                null, // timeoutInHours
                null, // priority
                true // enabled
                );
    }

    /** Check if current time is within the allowed time window. */
    public boolean isWithinWindow() {
        if (windowStart == null && windowEnd == null) {
            return true;
        }

        LocalTime now = LocalTime.now(java.time.ZoneId.systemDefault());

        if (windowStart != null && windowEnd != null) {
            if (windowStart.isBefore(windowEnd)) {
                return !now.isBefore(windowStart) && !now.isAfter(windowEnd);
            } else {
                return !now.isBefore(windowStart) || !now.isAfter(windowEnd);
            }
        } else if (windowStart != null) {
            return !now.isBefore(windowStart);
        } else {
            return !now.isAfter(windowEnd);
        }
    }

    /** Check if today is an allowed day for the operation. */
    public boolean isAllowedDay() {
        if (allowedDays == null || allowedDays.isEmpty()) {
            return true;
        }

        DayOfWeek today = DayOfWeek.from(java.time.LocalDate.now(java.time.ZoneId.systemDefault()));
        return allowedDays.contains(today);
    }

    /** Check if the schedule is enabled. */
    public boolean isEnabled() {
        return enabled != null && enabled;
    }

    /** Determine if the operation can run now based on schedule settings. */
    public boolean canRunNow() {
        return isEnabled() && isWithinWindow() && isAllowedDay();
    }

    /** Builder for ScheduleConfig. */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Duration intervalInDays;
        private String cronExpression;
        private LocalTime windowStart;
        private LocalTime windowEnd;
        private Set<DayOfWeek> allowedDays;
        private Duration timeoutInHours;
        private Integer priority;
        private Boolean enabled;

        public Builder interval(Duration interval) {
            this.intervalInDays = interval;
            return this;
        }

        public Builder cronExpression(String cronExpression) {
            this.cronExpression = cronExpression;
            return this;
        }

        public Builder windowStart(LocalTime windowStart) {
            this.windowStart = windowStart;
            return this;
        }

        public Builder windowEnd(LocalTime windowEnd) {
            this.windowEnd = windowEnd;
            return this;
        }

        public Builder allowedDays(Set<DayOfWeek> allowedDays) {
            this.allowedDays = allowedDays;
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeoutInHours = timeout;
            return this;
        }

        public Builder priority(Integer priority) {
            this.priority = priority;
            return this;
        }

        public Builder enabled(Boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public ScheduleConfig build() {
            return new ScheduleConfig(
                    intervalInDays,
                    cronExpression,
                    windowStart,
                    windowEnd,
                    allowedDays,
                    timeoutInHours,
                    priority,
                    enabled);
        }
    }
}
