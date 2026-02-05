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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("SparkEngineConfig")
class SparkEngineConfigTest {

    private static final String LIVY_URL = "http://livy:8998";
    private static final String JOB_JAR = "/path/to/maintenance-job.jar";
    private static final String JOB_CLASS = "com.floe.spark.job.MaintenanceJob";

    @Nested
    @DisplayName("Builder")
    class Builder {

        @Test
        @DisplayName("should create config with all parameters")
        void shouldCreateWithAllParams() {
            Map<String, String> catalogProps = Map.of("uri", "http://catalog:8181");
            Map<String, String> sparkConf = Map.of("spark.executor.instances", "2");

            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .catalogProperties(catalogProps)
                            .sparkConf(sparkConf)
                            .driverMemory("4g")
                            .executorMemory("4g")
                            .pollIntervalMs(3000)
                            .defaultTimeoutSeconds(7200)
                            .build();

            assertThat(config.livyUrl()).isEqualTo(LIVY_URL);
            assertThat(config.maintenanceJobJar()).isEqualTo(JOB_JAR);
            assertThat(config.maintenanceJobClass()).isEqualTo(JOB_CLASS);
            assertThat(config.catalogProperties()).containsEntry("uri", "http://catalog:8181");
            assertThat(config.sparkConf()).containsEntry("spark.executor.instances", "2");
            assertThat(config.driverMemory()).isEqualTo("4g");
            assertThat(config.executorMemory()).isEqualTo("4g");
            assertThat(config.pollIntervalMs()).isEqualTo(3000);
            assertThat(config.defaultTimeoutSeconds()).isEqualTo(7200);
        }

        @Test
        @DisplayName("should use default values when not specified")
        void shouldUseDefaults() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .build();

            assertThat(config.driverMemory()).isEqualTo("2g");
            assertThat(config.executorMemory()).isEqualTo("2g");
            assertThat(config.pollIntervalMs()).isEqualTo(2000);
            assertThat(config.defaultTimeoutSeconds()).isEqualTo(3600);
            assertThat(config.catalogProperties()).isEmpty();
            assertThat(config.sparkConf()).isEmpty();
        }

        @Test
        @DisplayName("should use default livy URL")
        void shouldUseDefaultLivyUrl() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .build();

            assertThat(config.livyUrl()).isEqualTo("http://localhost:8998");
        }

        @Test
        @DisplayName("should use default maintenance job class")
        void shouldUseDefaultJobClass() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .build();

            assertThat(config.maintenanceJobClass()).isEqualTo("com.floe.spark.job.MaintenanceJob");
        }
    }

    @Nested
    @DisplayName("Validation")
    class Validation {

        @Test
        @DisplayName("should throw when livyUrl is null")
        void shouldThrowWhenLivyUrlNull() {
            assertThatThrownBy(
                            () ->
                                    SparkEngineConfig.builder()
                                            .livyUrl(null)
                                            .maintenanceJobJar(JOB_JAR)
                                            .maintenanceJobClass(JOB_CLASS)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("livyUrl is required");
        }

        @Test
        @DisplayName("should throw when livyUrl is blank")
        void shouldThrowWhenLivyUrlBlank() {
            assertThatThrownBy(
                            () ->
                                    SparkEngineConfig.builder()
                                            .livyUrl("   ")
                                            .maintenanceJobJar(JOB_JAR)
                                            .maintenanceJobClass(JOB_CLASS)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("livyUrl is required");
        }

        @Test
        @DisplayName("should throw when maintenanceJobJar is null")
        void shouldThrowWhenJobJarNull() {
            assertThatThrownBy(
                            () ->
                                    SparkEngineConfig.builder()
                                            .livyUrl(LIVY_URL)
                                            .maintenanceJobJar(null)
                                            .maintenanceJobClass(JOB_CLASS)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maintenanceJobJar is required");
        }

        @Test
        @DisplayName("should throw when maintenanceJobJar is blank")
        void shouldThrowWhenJobJarBlank() {
            assertThatThrownBy(
                            () ->
                                    SparkEngineConfig.builder()
                                            .livyUrl(LIVY_URL)
                                            .maintenanceJobJar("")
                                            .maintenanceJobClass(JOB_CLASS)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maintenanceJobJar is required");
        }

        @Test
        @DisplayName("should throw when maintenanceJobClass is null")
        void shouldThrowWhenJobClassNull() {
            assertThatThrownBy(
                            () ->
                                    SparkEngineConfig.builder()
                                            .livyUrl(LIVY_URL)
                                            .maintenanceJobJar(JOB_JAR)
                                            .maintenanceJobClass(null)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maintenanceJobClass is required");
        }

        @Test
        @DisplayName("should throw when maintenanceJobClass is blank")
        void shouldThrowWhenJobClassBlank() {
            assertThatThrownBy(
                            () ->
                                    SparkEngineConfig.builder()
                                            .livyUrl(LIVY_URL)
                                            .maintenanceJobJar(JOB_JAR)
                                            .maintenanceJobClass("  ")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maintenanceJobClass is required");
        }
    }

    @Nested
    @DisplayName("Immutability")
    class Immutability {

        @Test
        @DisplayName("should return immutable catalog properties")
        void shouldReturnImmutableCatalogProperties() {
            Map<String, String> mutableProps = new java.util.HashMap<>();
            mutableProps.put("key", "value");

            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .catalogProperties(mutableProps)
                            .build();

            // Original map modification should not affect config
            mutableProps.put("another", "value");

            assertThat(config.catalogProperties()).hasSize(1);
            assertThat(config.catalogProperties()).containsOnlyKeys("key");

            // Config's map should be immutable
            assertThatThrownBy(() -> config.catalogProperties().put("new", "value"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("should return immutable spark conf")
        void shouldReturnImmutableSparkConf() {
            Map<String, String> mutableConf = new java.util.HashMap<>();
            mutableConf.put("spark.key", "value");

            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .sparkConf(mutableConf)
                            .build();

            // Original map modification should not affect config
            mutableConf.put("spark.another", "value");

            assertThat(config.sparkConf()).hasSize(1);
            assertThat(config.sparkConf()).containsOnlyKeys("spark.key");

            // Config's map should be immutable
            assertThatThrownBy(() -> config.sparkConf().put("spark.new", "value"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("should handle null catalog properties")
        void shouldHandleNullCatalogProperties() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .catalogProperties(null)
                            .build();

            assertThat(config.catalogProperties()).isNotNull();
            assertThat(config.catalogProperties()).isEmpty();
        }

        @Test
        @DisplayName("should handle null spark conf")
        void shouldHandleNullSparkConf() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .sparkConf(null)
                            .build();

            assertThat(config.sparkConf()).isNotNull();
            assertThat(config.sparkConf()).isEmpty();
        }
    }

    @Nested
    @DisplayName("URL Formats")
    class UrlFormats {

        @Test
        @DisplayName("should accept HTTP URL")
        void shouldAcceptHttpUrl() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl("http://livy-server:8998")
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .build();

            assertThat(config.livyUrl()).startsWith("http://");
        }

        @Test
        @DisplayName("should accept HTTPS URL")
        void shouldAcceptHttpsUrl() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl("https://livy-server:8998")
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .build();

            assertThat(config.livyUrl()).startsWith("https://");
        }

        @Test
        @DisplayName("should accept URL with path")
        void shouldAcceptUrlWithPath() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl("http://gateway:8080/livy")
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .build();

            assertThat(config.livyUrl()).contains("/livy");
        }
    }

    @Nested
    @DisplayName("Jar Path Formats")
    class JarPathFormats {

        @Test
        @DisplayName("should accept local file path")
        void shouldAcceptLocalPath() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar("/opt/floe/maintenance-job.jar")
                            .maintenanceJobClass(JOB_CLASS)
                            .build();

            assertThat(config.maintenanceJobJar()).startsWith("/");
        }

        @Test
        @DisplayName("should accept HDFS path")
        void shouldAcceptHdfsPath() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar("hdfs:///apps/floe/maintenance-job.jar")
                            .maintenanceJobClass(JOB_CLASS)
                            .build();

            assertThat(config.maintenanceJobJar()).startsWith("hdfs://");
        }

        @Test
        @DisplayName("should accept S3 path")
        void shouldAcceptS3Path() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar("s3://bucket/jars/maintenance-job.jar")
                            .maintenanceJobClass(JOB_CLASS)
                            .build();

            assertThat(config.maintenanceJobJar()).startsWith("s3://");
        }
    }

    @Nested
    @DisplayName("Memory Configuration")
    class MemoryConfiguration {

        @Test
        @DisplayName("should accept memory with g suffix")
        void shouldAcceptGigabytes() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .driverMemory("8g")
                            .executorMemory("16g")
                            .build();

            assertThat(config.driverMemory()).isEqualTo("8g");
            assertThat(config.executorMemory()).isEqualTo("16g");
        }

        @Test
        @DisplayName("should accept memory with m suffix")
        void shouldAcceptMegabytes() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .driverMemory("512m")
                            .executorMemory("1024m")
                            .build();

            assertThat(config.driverMemory()).isEqualTo("512m");
            assertThat(config.executorMemory()).isEqualTo("1024m");
        }

        @Test
        @DisplayName("should accept null memory values")
        void shouldAcceptNullMemory() {
            SparkEngineConfig config =
                    SparkEngineConfig.builder()
                            .livyUrl(LIVY_URL)
                            .maintenanceJobJar(JOB_JAR)
                            .maintenanceJobClass(JOB_CLASS)
                            .driverMemory(null)
                            .executorMemory(null)
                            .build();

            assertThat(config.driverMemory()).isNull();
            assertThat(config.executorMemory()).isNull();
        }
    }
}
