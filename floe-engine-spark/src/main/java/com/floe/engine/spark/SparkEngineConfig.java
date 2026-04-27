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

package com.floe.engine.spark;

import java.util.Map;

/** Configuration for the Spark execution engine. Submission and managing Spark is through Livy. */
public record SparkEngineConfig(
        String livyUrl,
        String maintenanceJobJar,
        String maintenanceJobClass,
        Map<String, String> catalogProperties,
        Map<String, String> sparkConf,
        String driverMemory,
        String executorMemory,
        int pollIntervalMs,
        int defaultTimeoutSeconds) {
    public SparkEngineConfig {
        if (livyUrl == null || livyUrl.isBlank()) {
            throw new IllegalArgumentException("livyUrl is required");
        }
        if (maintenanceJobJar == null || maintenanceJobJar.isBlank()) {
            throw new IllegalArgumentException("maintenanceJobJar is required");
        }
        if (maintenanceJobClass == null || maintenanceJobClass.isBlank()) {
            throw new IllegalArgumentException("maintenanceJobClass is required");
        }
        catalogProperties = catalogProperties != null ? Map.copyOf(catalogProperties) : Map.of();
        sparkConf = sparkConf != null ? Map.copyOf(sparkConf) : Map.of();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String livyUrl = "http://localhost:8998";
        private String maintenanceJobJar;
        private String maintenanceJobClass = "com.floe.spark.job.MaintenanceJob";
        private Map<String, String> catalogProperties = Map.of();
        private Map<String, String> sparkConf = Map.of();
        private String driverMemory = "2g";
        private String executorMemory = "2g";
        private int pollIntervalMs = 2000;
        private int defaultTimeoutSeconds = 3600;

        public Builder livyUrl(String url) {
            this.livyUrl = url;
            return this;
        }

        public Builder maintenanceJobJar(String jar) {
            this.maintenanceJobJar = jar;
            return this;
        }

        public Builder maintenanceJobClass(String className) {
            this.maintenanceJobClass = className;
            return this;
        }

        public Builder catalogProperties(Map<String, String> props) {
            this.catalogProperties = props;
            return this;
        }

        public Builder sparkConf(Map<String, String> conf) {
            this.sparkConf = conf;
            return this;
        }

        public Builder driverMemory(String memory) {
            this.driverMemory = memory;
            return this;
        }

        public Builder executorMemory(String memory) {
            this.executorMemory = memory;
            return this;
        }

        public Builder pollIntervalMs(int ms) {
            this.pollIntervalMs = ms;
            return this;
        }

        public Builder defaultTimeoutSeconds(int seconds) {
            this.defaultTimeoutSeconds = seconds;
            return this;
        }

        public SparkEngineConfig build() {
            return new SparkEngineConfig(
                    livyUrl,
                    maintenanceJobJar,
                    maintenanceJobClass,
                    catalogProperties,
                    sparkConf,
                    driverMemory,
                    executorMemory,
                    pollIntervalMs,
                    defaultTimeoutSeconds);
        }
    }
}
