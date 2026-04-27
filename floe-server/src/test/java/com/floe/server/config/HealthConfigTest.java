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

package com.floe.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;

class HealthConfigTest {

    @Test
    void scanModeDefaultsToMetadata() {
        SmallRyeConfig config = new SmallRyeConfigBuilder().withMapping(HealthConfig.class).build();
        HealthConfig healthConfig = config.getConfigMapping(HealthConfig.class);

        assertEquals("metadata", healthConfig.scanMode());
    }

    @Test
    void scanModeConfigurableViaProperties() {
        SmallRyeConfig config =
                new SmallRyeConfigBuilder()
                        .withMapping(HealthConfig.class)
                        .withSources(
                                new ConfigSource() {
                                    @Override
                                    public Map<String, String> getProperties() {
                                        return Map.of("floe.health.scan-mode", "sample");
                                    }

                                    @Override
                                    public java.util.Set<String> getPropertyNames() {
                                        return getProperties().keySet();
                                    }

                                    @Override
                                    public String getValue(String propertyName) {
                                        return getProperties().get(propertyName);
                                    }

                                    @Override
                                    public String getName() {
                                        return "test";
                                    }

                                    @Override
                                    public int getOrdinal() {
                                        return 100;
                                    }
                                })
                        .build();
        HealthConfig healthConfig = config.getConfigMapping(HealthConfig.class);

        assertEquals("sample", healthConfig.scanMode());
    }

    @Test
    void sampleLimitDefaultsTo10000() {
        SmallRyeConfig config = new SmallRyeConfigBuilder().withMapping(HealthConfig.class).build();
        HealthConfig healthConfig = config.getConfigMapping(HealthConfig.class);

        assertEquals(10000, healthConfig.sampleLimit());
    }

    @Test
    void sampleLimitConfigurable() {
        SmallRyeConfig config =
                new SmallRyeConfigBuilder()
                        .withMapping(HealthConfig.class)
                        .withSources(
                                new ConfigSource() {
                                    @Override
                                    public Map<String, String> getProperties() {
                                        return Map.of("floe.health.sample-limit", "500");
                                    }

                                    @Override
                                    public java.util.Set<String> getPropertyNames() {
                                        return getProperties().keySet();
                                    }

                                    @Override
                                    public String getValue(String propertyName) {
                                        return getProperties().get(propertyName);
                                    }

                                    @Override
                                    public String getName() {
                                        return "test";
                                    }

                                    @Override
                                    public int getOrdinal() {
                                        return 100;
                                    }
                                })
                        .build();
        HealthConfig healthConfig = config.getConfigMapping(HealthConfig.class);

        assertEquals(500, healthConfig.sampleLimit());
    }

    @Test
    void maxReportsPerTableDefaultsTo100() {
        SmallRyeConfig config = new SmallRyeConfigBuilder().withMapping(HealthConfig.class).build();
        HealthConfig healthConfig = config.getConfigMapping(HealthConfig.class);

        assertEquals(100, healthConfig.maxReportsPerTable());
    }

    @Test
    void maxReportAgeDaysDefaultsTo30() {
        SmallRyeConfig config = new SmallRyeConfigBuilder().withMapping(HealthConfig.class).build();
        HealthConfig healthConfig = config.getConfigMapping(HealthConfig.class);

        assertEquals(30, healthConfig.maxReportAgeDays());
    }
}
