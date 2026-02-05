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

package com.floe.engine.trino;

import java.util.Map;

/** Configuration for the Trino execution engine. */
public record TrinoEngineConfig(
        String jdbcUrl,
        String username,
        String password,
        String catalog,
        String schema,
        String catalogType,
        int queryTimeoutSeconds,
        Map<String, String> sessionProperties) {
    public TrinoEngineConfig {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("jdbcUrl is required");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("username is required");
        }
        if (catalog == null || catalog.isBlank()) {
            throw new IllegalArgumentException("catalog is required");
        }
        catalogType = catalogType != null ? catalogType.toUpperCase(java.util.Locale.ROOT) : "REST";
        sessionProperties = sessionProperties != null ? Map.copyOf(sessionProperties) : Map.of();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String jdbcUrl;
        private String username;
        private String password;
        private String catalog;
        private String schema;
        private String catalogType = "REST";
        private int queryTimeoutSeconds = 300;
        private Map<String, String> sessionProperties = Map.of();

        public Builder jdbcUrl(String url) {
            this.jdbcUrl = url;
            return this;
        }

        public Builder username(String user) {
            this.username = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder catalogType(String type) {
            this.catalogType = type;
            return this;
        }

        public Builder queryTimeoutSeconds(int timeout) {
            this.queryTimeoutSeconds = timeout;
            return this;
        }

        public Builder sessionProperties(Map<String, String> props) {
            this.sessionProperties = props;
            return this;
        }

        public TrinoEngineConfig build() {
            return new TrinoEngineConfig(
                    jdbcUrl,
                    username,
                    password,
                    catalog,
                    schema,
                    catalogType,
                    queryTimeoutSeconds,
                    sessionProperties);
        }
    }
}
