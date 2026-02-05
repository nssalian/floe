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

package com.floe.core.engine;

import java.util.Map;
import java.util.Optional;

/**
 * Context for executing a maintenance operation.
 *
 * <p>Contains all the information needed to execute an operation, including catalog and engine
 * properties, timeout settings, and optional callback URL for async notifications.
 *
 * @param executionId unique identifier for this execution
 * @param catalogProperties properties for the Iceberg catalog
 * @param engineProperties properties for the execution engine
 * @param callbackUrl optional URL for async completion notifications
 * @param timeoutSeconds maximum time allowed for execution
 * @param priority execution priority (higher values = higher priority)
 */
public record ExecutionContext(
        String executionId,
        Map<String, String> catalogProperties,
        Map<String, String> engineProperties,
        Optional<String> callbackUrl,
        int timeoutSeconds,
        int priority) {
    public ExecutionContext {
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId is required");
        }
        catalogProperties = Map.copyOf(catalogProperties);
        engineProperties = Map.copyOf(engineProperties);
    }

    /** Get a catalog property with default value. */
    public String getCatalogProperty(String key, String defaultValue) {
        return catalogProperties.getOrDefault(key, defaultValue);
    }

    /** Get an engine property with default value. */
    public String getEngineProperty(String key, String defaultValue) {
        return engineProperties.getOrDefault(key, defaultValue);
    }

    /** Create a builder starting with an execution ID. */
    public static Builder builder(String executionId) {
        return new Builder(executionId);
    }

    public static class Builder {

        private final String executionId;
        private Map<String, String> catalogProperties = Map.of();
        private Map<String, String> engineProperties = Map.of();
        private Optional<String> callbackUrl = Optional.empty();
        private int timeoutSeconds = 3600; // 1 hour default
        private int priority = 5; // Medium priority

        public Builder(String executionId) {
            this.executionId = executionId;
        }

        public Builder catalogProperties(Map<String, String> props) {
            this.catalogProperties = props;
            return this;
        }

        public Builder engineProperties(Map<String, String> props) {
            this.engineProperties = props;
            return this;
        }

        public Builder callbackUrl(String url) {
            this.callbackUrl = Optional.ofNullable(url);
            return this;
        }

        public Builder timeoutSeconds(int seconds) {
            this.timeoutSeconds = seconds;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public ExecutionContext build() {
            return new ExecutionContext(
                    executionId,
                    catalogProperties,
                    engineProperties,
                    callbackUrl,
                    timeoutSeconds,
                    priority);
        }
    }
}
