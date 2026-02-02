package com.floe.server.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;

class SchedulerConfigTest {

    @Test
    void maxTablesPerPollConfigurable() {
        SmallRyeConfig config =
                new SmallRyeConfigBuilder()
                        .withMapping(SchedulerConfig.class)
                        .withSources(
                                new SimpleConfigSource(
                                        Map.of("floe.scheduler.max-tables-per-poll", "5")))
                        .build();

        SchedulerConfig schedulerConfig = config.getConfigMapping(SchedulerConfig.class);

        assertEquals(5, schedulerConfig.maxTablesPerPoll());
    }

    @Test
    void maxOperationsPerPollConfigurable() {
        SmallRyeConfig config =
                new SmallRyeConfigBuilder()
                        .withMapping(SchedulerConfig.class)
                        .withSources(
                                new SimpleConfigSource(
                                        Map.of("floe.scheduler.max-operations-per-poll", "7")))
                        .build();

        SchedulerConfig schedulerConfig = config.getConfigMapping(SchedulerConfig.class);

        assertEquals(7, schedulerConfig.maxOperationsPerPoll());
    }

    @Test
    void maxBytesPerHourConfigurable() {
        SmallRyeConfig config =
                new SmallRyeConfigBuilder()
                        .withMapping(SchedulerConfig.class)
                        .withSources(
                                new SimpleConfigSource(
                                        Map.of("floe.scheduler.max-bytes-per-hour", "1024")))
                        .build();

        SchedulerConfig schedulerConfig = config.getConfigMapping(SchedulerConfig.class);

        assertEquals(1024L, schedulerConfig.maxBytesPerHour());
    }

    @Test
    void failureBackoffHoursConfigurable() {
        SmallRyeConfig config =
                new SmallRyeConfigBuilder()
                        .withMapping(SchedulerConfig.class)
                        .withSources(
                                new SimpleConfigSource(
                                        Map.of("floe.scheduler.failure-backoff-hours", "12")))
                        .build();

        SchedulerConfig schedulerConfig = config.getConfigMapping(SchedulerConfig.class);

        assertEquals(12, schedulerConfig.failureBackoffHours());
    }

    private static class SimpleConfigSource implements ConfigSource {

        private final Map<String, String> properties;

        private SimpleConfigSource(Map<String, String> properties) {
            this.properties = properties;
        }

        @Override
        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public java.util.Set<String> getPropertyNames() {
            return properties.keySet();
        }

        @Override
        public String getValue(String propertyName) {
            return properties.get(propertyName);
        }

        @Override
        public String getName() {
            return "test";
        }

        @Override
        public int getOrdinal() {
            return 100;
        }
    }
}
